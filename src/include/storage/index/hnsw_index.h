#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/rid.h"
#include "container/hash/hash_function.h"
#include "storage/index/index.h"
#include "type/value_factory.h"

namespace bustub {

struct HNSWIndexOptions {
  std::size_t m_{8};
  std::size_t ef_construction_{32};
  std::size_t ef_search_{16};
  VectorIndexDistanceMetric metric_{VectorIndexDistanceMetric::L2};
};

template <typename KT, typename VT, typename Cmp>
class HNSWIndex : public Index {
 public:
  /** 作用：初始化 HNSW 的图参数，并约束每层最大边数与搜索预算。 */
  HNSWIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
            const HashFunction<KT> &hash_fn, HNSWIndexOptions options = {})
      : Index(std::move(metadata)),
        m_(std::max<std::size_t>(2, options.m_)),
        ef_construction_(std::max<std::size_t>(m_, options.ef_construction_)),
        ef_search_(std::max<std::size_t>(1, options.ef_search_)),
        metric_(options.metric_) {}

  /** 作用：按稳定顺序从现有条目构建整张 HNSW 图，供建索引阶段复用。 */
  void BuildFromEntries(const std::vector<std::pair<Tuple, RID>> &entries) {
    std::scoped_lock<std::mutex> lck(lock_);
    ResetBuildStateUnlocked();
    for (const auto &[key, rid] : entries) {
      InsertEntryUnlocked(key, rid);
    }
  }

  auto InsertEntry(const Tuple &key, VT rid, Transaction *transaction) -> bool override;

  void DeleteEntry(const Tuple &key, VT rid, Transaction *transaction) override;

  void ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) override;

  auto SearchKnn(const Tuple &query, size_t k, std::vector<RID> *result, Transaction *transaction) -> void override;

  auto SearchVector(const Tuple &query, const AnnSearchOptions &options, std::vector<VectorIndexCandidate> *result,
                    Transaction *transaction) -> void override;

  auto GetVectorDistanceMetric() const -> std::optional<VectorIndexDistanceMetric> override { return metric_; }

  auto GetDefaultAnnSearchOptions(std::size_t top_k) const -> std::optional<AnnSearchOptions> override {
    return AnnSearchOptions{top_k, top_k, ef_search_};
  }

  auto GetMaxAnnSearchBudget() const -> std::optional<std::size_t> override {
    std::scoped_lock<std::mutex> lck(lock_);
    return nodes_.size();
  }

  auto GetM() const -> std::size_t { return m_; }

  auto GetEfConstruction() const -> std::size_t { return ef_construction_; }

  auto GetEfSearch() const -> std::size_t { return ef_search_; }

  /** 作用：向测试暴露当前图里的活跃节点数量。 */
  auto GetLiveEntryCount() const -> std::size_t {
    std::scoped_lock<std::mutex> lck(lock_);
    return active_entries_.size();
  }

  /** 作用：向测试暴露删除/更新遗留的 stale 节点数量。 */
  auto GetStaleEntryCount() const -> std::size_t {
    std::scoped_lock<std::mutex> lck(lock_);
    return stale_entry_count_;
  }

  /** 作用：统计一次 ANN 搜索中返回给执行器的 stale 候选数量。 */
  auto GetReturnedStaleCandidateCount() const -> std::size_t {
    std::scoped_lock<std::mutex> lck(lock_);
    return returned_stale_candidate_count_;
  }

  /** 作用：向测试暴露当前图的最高层，验证层级生成不会退化。 */
  auto GetMaxLevel() const -> int {
    std::scoped_lock<std::mutex> lck(lock_);
    return max_level_;
  }

  auto Distance(const Tuple &a, const Tuple &b) const -> double;

 private:
  /** 作用：保存 HNSW 图中的一个节点以及各层邻接边。 */
  struct GraphNode {
    Tuple key_;
    RID rid_;
    std::uint64_t entry_id_;
    int level_{0};
    bool is_active_{true};
    std::vector<std::vector<std::uint32_t>> neighbors_;
  };

  /** 作用：统一保存图搜索过程中的候选节点与距离。 */
  struct SearchCandidate {
    double distance_;
    std::uint32_t node_idx_;
  };

  /** 作用：清空当前图状态，为首次构建或重建做准备。 */
  void ResetBuildStateUnlocked();

  /** 作用：在持锁状态下完成单条记录插入，并维护多层图连边。 */
  auto InsertEntryUnlocked(const Tuple &key, RID rid) -> bool;

  /** 作用：生成稳定可复现的节点最高层，避免测试结果依赖随机数。 */
  auto GenerateLevelUnlocked(RID rid, std::uint64_t entry_id) const -> int;

  /** 作用：根据层号返回该层允许的最大边数，使底层更稠密、高层更稀疏。 */
  auto GetMaxNeighborsForLevelUnlocked(int level) const -> std::size_t;

  /** 作用：在高层执行 greedy descent，逐层逼近查询向量的局部入口点。 */
  auto GreedySearchLayerUnlocked(const Tuple &query, std::uint32_t entry_node, double entry_distance, int layer) const
      -> SearchCandidate;

  /** 作用：在指定层执行 bounded best-first search，`ef` 对应 HNSW 的搜索预算。 */
  auto SearchLayerUnlocked(const Tuple &query, const std::vector<std::uint32_t> &entry_points, std::size_t ef, int layer)
      -> std::vector<SearchCandidate>;

  /** 作用：用可解释的启发式裁剪候选邻居，避免简单全连接导致图退化。 */
  auto SelectNeighborsUnlocked(const std::vector<SearchCandidate> &candidates, std::size_t limit) const
      -> std::vector<std::uint32_t>;

  /** 作用：为已有节点重新裁剪某一层邻居，确保反向连边后度数仍受限。 */
  void PruneNodeNeighborsUnlocked(std::uint32_t node_idx, int layer, std::size_t limit);

  /** 作用：为新节点建立双向连边，并在必要时回收超出上限的旧边。 */
  void ConnectNodeUnlocked(std::uint32_t node_idx, const std::vector<std::uint32_t> &neighbors, int layer);

  /** 作用：用时间戳数组实现 visited set，避免每次查询都分配哈希集合。 */
  auto NextVisitedTokenUnlocked() -> std::uint32_t;

  /** 作用：把单列 VECTOR key 提取成数值向量。 */
  auto ExtractVector(const Tuple &key) const -> std::vector<double>;

  mutable std::mutex lock_;
  std::vector<GraphNode> nodes_;
  std::unordered_map<RID, std::uint32_t> active_entries_;
  std::optional<std::uint32_t> entry_point_{std::nullopt};
  int max_level_{-1};
  std::uint64_t next_entry_id_{1};
  std::size_t stale_entry_count_{0};
  std::size_t returned_stale_candidate_count_{0};
  mutable std::vector<std::uint32_t> visit_tokens_;
  mutable std::uint32_t visit_token_clock_{0};
  std::size_t m_{8};
  std::size_t ef_construction_{32};
  std::size_t ef_search_{16};
  VectorIndexDistanceMetric metric_{VectorIndexDistanceMetric::L2};
  static constexpr int kMaxLevelCap = 16;
};

template <typename KT, typename VT, typename Cmp>
void HNSWIndex<KT, VT, Cmp>::ResetBuildStateUnlocked() {
  nodes_.clear();
  active_entries_.clear();
  entry_point_.reset();
  max_level_ = -1;
  next_entry_id_ = 1;
  stale_entry_count_ = 0;
  returned_stale_candidate_count_ = 0;
  visit_tokens_.clear();
  visit_token_clock_ = 0;
}

template <typename KT, typename VT, typename Cmp>
auto HNSWIndex<KT, VT, Cmp>::GenerateLevelUnlocked(RID rid, std::uint64_t entry_id) const -> int {
  auto mix = [](std::uint64_t value) -> std::uint64_t {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31U);
  };

  std::uint64_t state = mix(static_cast<std::uint64_t>(rid.Get()) ^ (entry_id << 1U));
  int level = 0;
  while ((state & 0x3ULL) == 0ULL && level < kMaxLevelCap) {
    level += 1;
    state >>= 2U;
  }
  return level;
}

template <typename KT, typename VT, typename Cmp>
auto HNSWIndex<KT, VT, Cmp>::GetMaxNeighborsForLevelUnlocked(int level) const -> std::size_t {
  return level == 0 ? 2 * m_ : m_;
}

template <typename KT, typename VT, typename Cmp>
auto HNSWIndex<KT, VT, Cmp>::NextVisitedTokenUnlocked() -> std::uint32_t {
  if (visit_tokens_.size() < nodes_.size()) {
    visit_tokens_.resize(nodes_.size(), 0);
  }
  if (visit_token_clock_ == std::numeric_limits<std::uint32_t>::max()) {
    std::fill(visit_tokens_.begin(), visit_tokens_.end(), 0);
    visit_token_clock_ = 0;
  }
  visit_token_clock_ += 1;
  return visit_token_clock_;
}

template <typename KT, typename VT, typename Cmp>
auto HNSWIndex<KT, VT, Cmp>::ExtractVector(const Tuple &key) const -> std::vector<double> {
  auto *schema = this->GetKeySchema();
  BUSTUB_ASSERT(schema->GetColumnCount() == 1, "HNSW only supports single-column vector keys");
  const auto value = key.GetValue(schema, 0);
  BUSTUB_ASSERT(value.GetTypeId() == TypeId::VECTOR, "HNSW expects VECTOR keys");
  return value.GetVector();
}

template <typename KT, typename VT, typename Cmp>
auto HNSWIndex<KT, VT, Cmp>::Distance(const Tuple &a, const Tuple &b) const -> double {
  const auto lhs = ExtractVector(a);
  const auto rhs = ExtractVector(b);
  BUSTUB_ASSERT(lhs.size() == rhs.size(), "Tuples must have the same dimensionality for distance calculation");

  double dot = 0.0;
  double lhs_norm_sq = 0.0;
  double rhs_norm_sq = 0.0;
  double l2_sq = 0.0;
  for (std::size_t i = 0; i < lhs.size(); i++) {
    const double left = lhs[i];
    const double right = rhs[i];
    const double diff = left - right;
    dot += left * right;
    lhs_norm_sq += left * left;
    rhs_norm_sq += right * right;
    l2_sq += diff * diff;
  }

  switch (metric_) {
    case VectorIndexDistanceMetric::L2:
      return std::sqrt(l2_sq);
    case VectorIndexDistanceMetric::Cosine: {
      const double denom = std::sqrt(lhs_norm_sq) * std::sqrt(rhs_norm_sq);
      if (denom == 0.0) {
        return 1.0;
      }
      return 1.0 - dot / denom;
    }
    case VectorIndexDistanceMetric::InnerProduct:
      return -dot;
  }
  UNREACHABLE("unsupported HNSW metric");
}

template <typename KT, typename VT, typename Cmp>
auto HNSWIndex<KT, VT, Cmp>::GreedySearchLayerUnlocked(const Tuple &query, std::uint32_t entry_node,
                                                       double entry_distance, int layer) const -> SearchCandidate {
  auto best = SearchCandidate{entry_distance, entry_node};
  bool improved = true;
  while (improved) {
    improved = false;
    for (const auto neighbor_idx : nodes_[best.node_idx_].neighbors_[layer]) {
      const auto neighbor_distance = Distance(query, nodes_[neighbor_idx].key_);
      if (neighbor_distance < best.distance_) {
        best = {neighbor_distance, neighbor_idx};
        improved = true;
      }
    }
  }
  return best;
}

template <typename KT, typename VT, typename Cmp>
auto HNSWIndex<KT, VT, Cmp>::SearchLayerUnlocked(const Tuple &query, const std::vector<std::uint32_t> &entry_points,
                                                 std::size_t ef, int layer) -> std::vector<SearchCandidate> {
  if (entry_points.empty()) {
    return {};
  }

  struct MinDistanceCmp {
    auto operator()(const SearchCandidate &lhs, const SearchCandidate &rhs) const -> bool {
      if (lhs.distance_ != rhs.distance_) {
        return lhs.distance_ > rhs.distance_;
      }
      return lhs.node_idx_ > rhs.node_idx_;
    }
  };

  struct MaxDistanceCmp {
    auto operator()(const SearchCandidate &lhs, const SearchCandidate &rhs) const -> bool {
      if (lhs.distance_ != rhs.distance_) {
        return lhs.distance_ < rhs.distance_;
      }
      return lhs.node_idx_ < rhs.node_idx_;
    }
  };

  ef = std::max<std::size_t>(1, ef);
  const auto token = NextVisitedTokenUnlocked();
  std::priority_queue<SearchCandidate, std::vector<SearchCandidate>, MinDistanceCmp> candidate_queue;
  std::priority_queue<SearchCandidate, std::vector<SearchCandidate>, MaxDistanceCmp> top_candidates;

  for (const auto node_idx : entry_points) {
    if (visit_tokens_[node_idx] == token) {
      continue;
    }
    visit_tokens_[node_idx] = token;
    const auto distance = Distance(query, nodes_[node_idx].key_);
    candidate_queue.push({distance, node_idx});
    top_candidates.push({distance, node_idx});
  }

  while (!candidate_queue.empty()) {
    const auto current = candidate_queue.top();
    if (top_candidates.size() >= ef && current.distance_ > top_candidates.top().distance_) {
      break;
    }
    candidate_queue.pop();

    for (const auto neighbor_idx : nodes_[current.node_idx_].neighbors_[layer]) {
      if (visit_tokens_[neighbor_idx] == token) {
        continue;
      }
      visit_tokens_[neighbor_idx] = token;
      const auto distance = Distance(query, nodes_[neighbor_idx].key_);
      if (top_candidates.size() < ef || distance < top_candidates.top().distance_) {
        candidate_queue.push({distance, neighbor_idx});
        top_candidates.push({distance, neighbor_idx});
        if (top_candidates.size() > ef) {
          top_candidates.pop();
        }
      }
    }
  }

  std::vector<SearchCandidate> result;
  result.reserve(top_candidates.size());
  while (!top_candidates.empty()) {
    result.push_back(top_candidates.top());
    top_candidates.pop();
  }
  std::sort(result.begin(), result.end(), [](const auto &lhs, const auto &rhs) {
    if (lhs.distance_ != rhs.distance_) {
      return lhs.distance_ < rhs.distance_;
    }
    return lhs.node_idx_ < rhs.node_idx_;
  });
  return result;
}

template <typename KT, typename VT, typename Cmp>
auto HNSWIndex<KT, VT, Cmp>::SelectNeighborsUnlocked(const std::vector<SearchCandidate> &candidates,
                                                     std::size_t limit) const -> std::vector<std::uint32_t> {
  std::vector<SearchCandidate> sorted = candidates;
  std::sort(sorted.begin(), sorted.end(), [](const auto &lhs, const auto &rhs) {
    if (lhs.distance_ != rhs.distance_) {
      return lhs.distance_ < rhs.distance_;
    }
    return lhs.node_idx_ < rhs.node_idx_;
  });

  std::vector<std::uint32_t> selected;
  selected.reserve(std::min(limit, sorted.size()));
  for (const auto &candidate : sorted) {
    bool should_skip = false;
    for (const auto selected_idx : selected) {
      if (Distance(nodes_[candidate.node_idx_].key_, nodes_[selected_idx].key_) < candidate.distance_) {
        should_skip = true;
        break;
      }
    }
    if (should_skip) {
      continue;
    }
    selected.push_back(candidate.node_idx_);
    if (selected.size() >= limit) {
      return selected;
    }
  }

  for (const auto &candidate : sorted) {
    if (std::find(selected.begin(), selected.end(), candidate.node_idx_) != selected.end()) {
      continue;
    }
    selected.push_back(candidate.node_idx_);
    if (selected.size() >= limit) {
      break;
    }
  }
  return selected;
}

template <typename KT, typename VT, typename Cmp>
void HNSWIndex<KT, VT, Cmp>::PruneNodeNeighborsUnlocked(std::uint32_t node_idx, int layer, std::size_t limit) {
  auto &neighbors = nodes_[node_idx].neighbors_[layer];
  if (neighbors.size() <= limit) {
    return;
  }

  std::vector<SearchCandidate> candidates;
  candidates.reserve(neighbors.size());
  for (const auto neighbor_idx : neighbors) {
    candidates.push_back({Distance(nodes_[node_idx].key_, nodes_[neighbor_idx].key_), neighbor_idx});
  }

  const auto selected = SelectNeighborsUnlocked(candidates, limit);
  neighbors.assign(selected.begin(), selected.end());
}

template <typename KT, typename VT, typename Cmp>
void HNSWIndex<KT, VT, Cmp>::ConnectNodeUnlocked(std::uint32_t node_idx, const std::vector<std::uint32_t> &neighbors,
                                                 int layer) {
  auto &node_neighbors = nodes_[node_idx].neighbors_[layer];
  for (const auto neighbor_idx : neighbors) {
    if (neighbor_idx == node_idx) {
      continue;
    }

    if (std::find(node_neighbors.begin(), node_neighbors.end(), neighbor_idx) == node_neighbors.end()) {
      node_neighbors.push_back(neighbor_idx);
    }

    auto &reverse_neighbors = nodes_[neighbor_idx].neighbors_[layer];
    if (std::find(reverse_neighbors.begin(), reverse_neighbors.end(), node_idx) == reverse_neighbors.end()) {
      reverse_neighbors.push_back(node_idx);
    }
    PruneNodeNeighborsUnlocked(neighbor_idx, layer, GetMaxNeighborsForLevelUnlocked(layer));
  }

  PruneNodeNeighborsUnlocked(node_idx, layer, GetMaxNeighborsForLevelUnlocked(layer));
}

template <typename KT, typename VT, typename Cmp>
auto HNSWIndex<KT, VT, Cmp>::InsertEntryUnlocked(const Tuple &key, RID rid) -> bool {
  if (active_entries_.find(rid) != active_entries_.end()) {
    return false;
  }

  const auto entry_id = next_entry_id_++;
  const auto level = GenerateLevelUnlocked(rid, entry_id);
  const auto node_idx = static_cast<std::uint32_t>(nodes_.size());
  nodes_.push_back({key, rid, entry_id, level, true, std::vector<std::vector<std::uint32_t>>(level + 1)});
  visit_tokens_.push_back(0);
  active_entries_[rid] = node_idx;

  if (!entry_point_.has_value()) {
    entry_point_ = node_idx;
    max_level_ = level;
    return true;
  }

  auto current = *entry_point_;
  auto current_distance = Distance(key, nodes_[current].key_);
  for (int layer = max_level_; layer > level; layer--) {
    const auto best = GreedySearchLayerUnlocked(key, current, current_distance, layer);
    current = best.node_idx_;
    current_distance = best.distance_;
  }

  const auto connect_from_level = std::min(level, max_level_);
  for (int layer = connect_from_level; layer >= 0; layer--) {
    const auto candidates = SearchLayerUnlocked(key, {current}, ef_construction_, layer);
    const auto selected = SelectNeighborsUnlocked(candidates, GetMaxNeighborsForLevelUnlocked(layer));
    ConnectNodeUnlocked(node_idx, selected, layer);
    if (!candidates.empty()) {
      current = candidates.front().node_idx_;
      current_distance = candidates.front().distance_;
    }
  }

  if (level > max_level_) {
    entry_point_ = node_idx;
    max_level_ = level;
  }
  return true;
}

template <typename KT, typename VT, typename Cmp>
auto HNSWIndex<KT, VT, Cmp>::InsertEntry(const Tuple &key, VT rid, Transaction *transaction) -> bool {
  std::scoped_lock<std::mutex> lck(lock_);
  return InsertEntryUnlocked(key, rid);
}

template <typename KT, typename VT, typename Cmp>
void HNSWIndex<KT, VT, Cmp>::DeleteEntry(const Tuple &key, VT rid, Transaction *transaction) {
  std::scoped_lock<std::mutex> lck(lock_);
  const auto it = active_entries_.find(rid);
  if (it == active_entries_.end()) {
    return;
  }

  auto &node = nodes_[it->second];
  if (!IsTupleContentEqual(node.key_, key)) {
    return;
  }

  node.is_active_ = false;
  active_entries_.erase(it);
  stale_entry_count_ += 1;
}

template <typename KT, typename VT, typename Cmp>
void HNSWIndex<KT, VT, Cmp>::ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) {
  std::scoped_lock<std::mutex> lck(lock_);
  result->clear();
  for (const auto &node : nodes_) {
    if (!node.is_active_) {
      continue;
    }
    if (IsTupleContentEqual(node.key_, key)) {
      result->push_back(node.rid_);
    }
  }
}

template <typename KT, typename VT, typename Cmp>
auto HNSWIndex<KT, VT, Cmp>::SearchKnn(const Tuple &query, size_t k, std::vector<RID> *result,
                                       Transaction *transaction) -> void {
  std::vector<VectorIndexCandidate> candidates;
  SearchVector(query, AnnSearchOptions{k, k, ef_search_}, &candidates, transaction);

  result->clear();
  std::unordered_map<RID, bool> seen_rids;
  for (const auto &candidate : candidates) {
    if (seen_rids.emplace(candidate.rid_, true).second) {
      result->push_back(candidate.rid_);
    }
    if (result->size() >= k) {
      break;
    }
  }
}

template <typename KT, typename VT, typename Cmp>
auto HNSWIndex<KT, VT, Cmp>::SearchVector(const Tuple &query, const AnnSearchOptions &options,
                                          std::vector<VectorIndexCandidate> *result, Transaction *transaction) -> void {
  std::scoped_lock<std::mutex> lck(lock_);
  result->clear();
  if (!entry_point_.has_value()) {
    return;
  }

  const auto candidate_budget = std::max<std::size_t>(1, std::max(options.top_k_, options.candidate_budget_));
  const auto search_budget =
      std::max<std::size_t>(candidate_budget, options.search_budget_ == 0 ? ef_search_ : options.search_budget_);

  auto current = *entry_point_;
  auto current_distance = Distance(query, nodes_[current].key_);
  for (int layer = max_level_; layer > 0; layer--) {
    const auto best = GreedySearchLayerUnlocked(query, current, current_distance, layer);
    current = best.node_idx_;
    current_distance = best.distance_;
  }

  const auto ranked = SearchLayerUnlocked(query, {current}, search_budget, 0);
  const auto keep = std::min(candidate_budget, ranked.size());
  result->reserve(keep);
  for (std::size_t i = 0; i < keep; i++) {
    const auto &node = nodes_[ranked[i].node_idx_];
    if (!node.is_active_) {
      returned_stale_candidate_count_ += 1;
    }
    result->push_back({node.rid_, node.key_, node.entry_id_});
  }
}

}  // namespace bustub
