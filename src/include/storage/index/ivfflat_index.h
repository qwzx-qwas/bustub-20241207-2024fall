#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
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

struct IVFFlatIndexOptions {
  std::size_t nlist_{10};
  std::size_t nprobe_{1};
  VectorIndexDistanceMetric metric_{VectorIndexDistanceMetric::L2};
  std::size_t sample_multiplier_{4};
  std::size_t kmeans_iterations_{5};
};

template <typename KT, typename VT, typename Cmp>
class IVFFlatIndex : public Index {
 public:
  /** 作用：初始化 IVFFlat 索引参数与阶段一所需的 stale/rebuild 状态。 */
  IVFFlatIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
               const HashFunction<KT> &hash_fn, IVFFlatIndexOptions options = {})
      : Index(std::move(metadata)),
        nlist_(std::max<std::size_t>(1, options.nlist_)),
        nprobe_(std::max<std::size_t>(1, std::min(options.nprobe_, nlist_))),
        metric_(options.metric_),
        sample_multiplier_(std::max<std::size_t>(1, options.sample_multiplier_)),
        kmeans_iterations_(std::max<std::size_t>(1, options.kmeans_iterations_)) {}

  /** 作用：从当前有效条目重建整棵 IVF 状态，用于建索引与 stale 后同步重建。 */
  void BuildFromEntries(const std::vector<std::pair<Tuple, RID>> &entries) {
    std::scoped_lock<std::mutex> lck(lock_);
    auto artifacts = BuildArtifactsFromEntriesUnlocked(entries);
    InstallBuildArtifactsUnlocked(std::move(artifacts));
  }

  auto InsertEntry(const Tuple &key, VT rid, Transaction *transaction) -> bool override;

  void DeleteEntry(const Tuple &key, VT rid, Transaction *transaction) override;

  void ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) override;

  auto SearchKnn(const Tuple &query, size_t k, std::vector<RID> *result, Transaction *transaction) -> void override;

  auto SearchVector(const Tuple &query, const AnnSearchOptions &options, std::vector<VectorIndexCandidate> *result,
                    Transaction *transaction) -> void override;

  auto GetVectorDistanceMetric() const -> std::optional<VectorIndexDistanceMetric> override { return metric_; }

  auto GetDefaultAnnSearchOptions(std::size_t top_k) const -> std::optional<AnnSearchOptions> override {
    return AnnSearchOptions{top_k, top_k, nprobe_};
  }

  auto GetMaxAnnSearchBudget() const -> std::optional<std::size_t> override {
    return centroids_.empty() ? nlist_ : centroids_.size();
  }

  auto GetNList() const -> std::size_t { return nlist_; }

  auto GetNProbe() const -> std::size_t { return nprobe_; }

  /** 作用：向测试暴露 stale 规模，便于验证阶段一的积累与清理逻辑。 */
  auto GetStaleEntryCount() const -> std::size_t {
    std::scoped_lock<std::mutex> lck(lock_);
    return stale_entry_count_;
  }

  /** 作用：向测试暴露当前有效条目数，便于验证 rebuild 前后状态。 */
  auto GetLiveEntryCount() const -> std::size_t {
    std::scoped_lock<std::mutex> lck(lock_);
    return active_entries_.size();
  }

  /** 作用：向测试暴露 rebuild 次数，确认阈值触发逻辑真正执行。 */
  auto GetRebuildCount() const -> std::size_t {
    std::scoped_lock<std::mutex> lck(lock_);
    return rebuild_count_;
  }

  /** 作用：统计一次 ANN 搜索中返回给执行器的 stale 候选数量。 */
  auto GetReturnedStaleCandidateCount() const -> std::size_t {
    std::scoped_lock<std::mutex> lck(lock_);
    return returned_stale_candidate_count_;
  }

  auto FindClosestCentroid(const Tuple &key) const -> std::size_t;

  auto Distance(const Tuple &a, const Tuple &b) const -> double;

 private:
  /** 作用：保存单个 IVF list 中的索引条目，并为执行器提供稳定 candidate id。 */
  struct IndexedEntry {
    Tuple key_;
    RID rid_;
    std::uint64_t entry_id_;
  };

  /** 作用：记录某个 RID 当前“活跃版本”所在的位置，用于判断 list 里的条目是否 stale。 */
  struct EntryLocator {
    std::size_t list_idx_;
    std::size_t slot_idx_;
    std::uint64_t entry_id_;
  };

  /** 作用：把重建过程需要的中间状态放到局部对象里，成功后再原子替换旧状态。 */
  struct BuildArtifacts {
    std::vector<Tuple> centroids_;
    std::vector<std::vector<IndexedEntry>> lists_;
    std::unordered_map<RID, EntryLocator> active_entries_;
    std::uint64_t next_entry_id_{1};
  };

  /** 作用：清空当前 IVF 内存状态，为首次建索引或重建后的替换做准备。 */
  void ResetBuildStateUnlocked();

  /** 作用：抽样训练集，控制 centroid 训练成本。 */
  auto SampleEntriesUnlocked(const std::vector<std::pair<Tuple, RID>> &entries) const
      -> std::vector<std::pair<Tuple, RID>>;

  /** 作用：基于样本训练 centroid，并返回新的 centroid 集合。 */
  auto TrainCentroidsUnlocked(const std::vector<std::pair<Tuple, RID>> &samples) const -> std::vector<Tuple>;

  /** 作用：把当前有效条目分配到 IVF list，并建立 active RID 定位表。 */
  void AssignEntriesToListsUnlocked(const std::vector<std::pair<Tuple, RID>> &entries,
                                    const std::vector<Tuple> &centroids, BuildArtifacts *artifacts) const;

  /** 作用：构造一次完整 rebuild 的局部结果，失败时不污染旧索引。 */
  auto BuildArtifactsFromEntriesUnlocked(const std::vector<std::pair<Tuple, RID>> &entries) const -> BuildArtifacts;

  /** 作用：将局部构建结果原子替换为当前可读索引状态。 */
  void InstallBuildArtifactsUnlocked(BuildArtifacts &&artifacts);

  /** 作用：收集当前活跃条目，供阈值触发 rebuild 时重新训练和分桶。 */
  auto CollectActiveEntriesUnlocked() const -> std::vector<std::pair<Tuple, RID>>;

  /** 作用：判断某个 list 槽位是否仍然是该 RID 的活跃版本。 */
  auto IsActiveEntryUnlocked(RID rid, std::size_t list_idx, std::size_t slot_idx, std::uint64_t entry_id) const -> bool;

  /** 作用：根据 stale 占比判断是否需要在查询结束后触发一次同步 rebuild。 */
  auto ShouldTriggerRebuildUnlocked() const -> bool;

  /** 作用：在不破坏旧状态的前提下同步重建 IVF 索引。 */
  void MaybeRebuildUnlocked();

  /** 作用：从单列 VECTOR key 中提取实际向量。 */
  auto ExtractVector(const Tuple &key) const -> std::vector<double>;

  /** 作用：把质心向量重新封装成索引键 tuple。 */
  auto MakeVectorTuple(const std::vector<double> &vector) const -> Tuple;

  /** 作用：在指定 centroid 集合中找到最近的一个 list。 */
  auto FindClosestCentroidIn(const std::vector<Tuple> &centroids, const Tuple &key) const -> std::size_t;

  /** 作用：在指定 centroid 集合中找到最近的若干个 list。 */
  auto FindClosestCentroidsIn(const std::vector<Tuple> &centroids, const Tuple &key, std::size_t probe_count) const
      -> std::vector<std::size_t>;

  mutable std::mutex lock_;
  std::vector<Tuple> centroids_;
  std::vector<std::vector<IndexedEntry>> lists_;
  std::unordered_map<RID, EntryLocator> active_entries_;
  std::size_t stale_entry_count_{0};
  std::size_t rebuild_count_{0};
  std::size_t returned_stale_candidate_count_{0};
  std::uint64_t next_entry_id_{1};
  std::size_t nlist_{10};
  std::size_t nprobe_{1};
  VectorIndexDistanceMetric metric_{VectorIndexDistanceMetric::L2};
  std::size_t sample_multiplier_{4};
  std::size_t kmeans_iterations_{5};
};

template <typename KT, typename VT, typename Cmp>
void IVFFlatIndex<KT, VT, Cmp>::ResetBuildStateUnlocked() {
  centroids_.clear();
  lists_.clear();
  active_entries_.clear();
  stale_entry_count_ = 0;
  next_entry_id_ = 1;
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::SampleEntriesUnlocked(const std::vector<std::pair<Tuple, RID>> &entries) const
    -> std::vector<std::pair<Tuple, RID>> {
  if (entries.empty()) {
    return {};
  }

  const auto sample_size = std::min(entries.size(), std::max<std::size_t>(nlist_, nlist_ * sample_multiplier_));
  if (sample_size >= entries.size()) {
    return entries;
  }

  std::vector<std::pair<Tuple, RID>> samples;
  samples.reserve(sample_size);
  const auto stride = std::max<std::size_t>(1, entries.size() / sample_size);
  for (std::size_t i = 0; i < entries.size() && samples.size() < sample_size; i += stride) {
    samples.push_back(entries[i]);
  }
  if (samples.empty()) {
    samples.push_back(entries.front());
  }
  return samples;
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::TrainCentroidsUnlocked(const std::vector<std::pair<Tuple, RID>> &samples) const
    -> std::vector<Tuple> {
  if (samples.empty()) {
    return {};
  }

  std::vector<Tuple> centroids;
  const auto centroid_count = std::min(nlist_, samples.size());
  centroids.reserve(centroid_count);
  for (std::size_t i = 0; i < centroid_count; i++) {
    centroids.push_back(samples[i].first);
  }

  if (centroids.empty()) {
    return centroids;
  }

  const auto dim = ExtractVector(samples.front().first).size();
  for (std::size_t iter = 0; iter < kmeans_iterations_; iter++) {
    std::vector<std::vector<double>> sums(centroids.size(), std::vector<double>(dim, 0.0));
    std::vector<std::size_t> counts(centroids.size(), 0);

    for (const auto &[key, rid] : samples) {
      const auto centroid_idx = FindClosestCentroidIn(centroids, key);
      const auto vector = ExtractVector(key);
      for (std::size_t d = 0; d < dim; d++) {
        sums[centroid_idx][d] += vector[d];
      }
      counts[centroid_idx] += 1;
    }

    for (std::size_t centroid_idx = 0; centroid_idx < centroids.size(); centroid_idx++) {
      if (counts[centroid_idx] == 0) {
        continue;
      }
      for (std::size_t d = 0; d < dim; d++) {
        sums[centroid_idx][d] /= static_cast<double>(counts[centroid_idx]);
      }
      centroids[centroid_idx] = MakeVectorTuple(sums[centroid_idx]);
    }
  }

  return centroids;
}

template <typename KT, typename VT, typename Cmp>
void IVFFlatIndex<KT, VT, Cmp>::AssignEntriesToListsUnlocked(const std::vector<std::pair<Tuple, RID>> &entries,
                                                             const std::vector<Tuple> &centroids,
                                                             BuildArtifacts *artifacts) const {
  if (centroids.empty()) {
    return;
  }

  artifacts->lists_.assign(centroids.size(), {});
  for (const auto &[key, rid] : entries) {
    const auto centroid_idx = FindClosestCentroidIn(centroids, key);
    const auto slot_idx = artifacts->lists_[centroid_idx].size();
    artifacts->lists_[centroid_idx].push_back({key, rid, artifacts->next_entry_id_++});
    artifacts->active_entries_[rid] =
        EntryLocator{centroid_idx, slot_idx, artifacts->lists_[centroid_idx].back().entry_id_};
  }
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::BuildArtifactsFromEntriesUnlocked(
    const std::vector<std::pair<Tuple, RID>> &entries) const -> BuildArtifacts {
  BuildArtifacts artifacts;
  if (entries.empty()) {
    return artifacts;
  }

  artifacts.centroids_ = TrainCentroidsUnlocked(SampleEntriesUnlocked(entries));
  AssignEntriesToListsUnlocked(entries, artifacts.centroids_, &artifacts);
  return artifacts;
}

template <typename KT, typename VT, typename Cmp>
void IVFFlatIndex<KT, VT, Cmp>::InstallBuildArtifactsUnlocked(BuildArtifacts &&artifacts) {
  ResetBuildStateUnlocked();
  centroids_ = std::move(artifacts.centroids_);
  lists_ = std::move(artifacts.lists_);
  active_entries_ = std::move(artifacts.active_entries_);
  next_entry_id_ = artifacts.next_entry_id_;
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::CollectActiveEntriesUnlocked() const -> std::vector<std::pair<Tuple, RID>> {
  std::vector<std::pair<Tuple, RID>> entries;
  entries.reserve(active_entries_.size());
  for (const auto &[rid, locator] : active_entries_) {
    const auto &entry = lists_[locator.list_idx_][locator.slot_idx_];
    entries.emplace_back(entry.key_, rid);
  }
  return entries;
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::IsActiveEntryUnlocked(RID rid, std::size_t list_idx, std::size_t slot_idx,
                                                      std::uint64_t entry_id) const -> bool {
  const auto it = active_entries_.find(rid);
  if (it == active_entries_.end()) {
    return false;
  }
  const auto &locator = it->second;
  return locator.list_idx_ == list_idx && locator.slot_idx_ == slot_idx && locator.entry_id_ == entry_id;
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::ShouldTriggerRebuildUnlocked() const -> bool {
  const auto live_entry_count = active_entries_.size();
  const auto total_entry_count = live_entry_count + stale_entry_count_;
  if (stale_entry_count_ == 0 || total_entry_count == 0) {
    return false;
  }

  // 阶段一先采用保守阈值，避免少量 stale 就频繁重建。
  return stale_entry_count_ * 2 >= total_entry_count;
}

template <typename KT, typename VT, typename Cmp>
void IVFFlatIndex<KT, VT, Cmp>::MaybeRebuildUnlocked() {
  if (!ShouldTriggerRebuildUnlocked()) {
    return;
  }

  try {
    auto artifacts = BuildArtifactsFromEntriesUnlocked(CollectActiveEntriesUnlocked());
    InstallBuildArtifactsUnlocked(std::move(artifacts));
    rebuild_count_ += 1;
  } catch (...) {
    // rebuild 失败时保持旧索引仍然可读。
  }
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::ExtractVector(const Tuple &key) const -> std::vector<double> {
  auto *schema = this->GetKeySchema();
  BUSTUB_ASSERT(schema->GetColumnCount() == 1, "IVFFlat only supports single-column vector keys");
  const auto value = key.GetValue(schema, 0);
  BUSTUB_ASSERT(value.GetTypeId() == TypeId::VECTOR, "IVFFlat expects VECTOR keys");
  return value.GetVector();
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::MakeVectorTuple(const std::vector<double> &vector) const -> Tuple {
  auto *schema = this->GetKeySchema();
  return Tuple({ValueFactory::GetVectorValue(vector)}, schema);
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::InsertEntry(const Tuple &key, VT rid, Transaction *transaction) -> bool {
  std::scoped_lock<std::mutex> lck(lock_);
  if (active_entries_.find(rid) != active_entries_.end()) {
    return false;
  }

  if (centroids_.empty()) {
    centroids_.push_back(key);
    lists_.resize(1);
  }

  const auto closest = FindClosestCentroidIn(centroids_, key);
  const auto slot_idx = lists_[closest].size();
  lists_[closest].push_back({key, rid, next_entry_id_++});
  active_entries_[rid] = EntryLocator{closest, slot_idx, lists_[closest].back().entry_id_};
  return true;
}

template <typename KT, typename VT, typename Cmp>
void IVFFlatIndex<KT, VT, Cmp>::DeleteEntry(const Tuple &key, VT rid, Transaction *transaction) {
  std::scoped_lock<std::mutex> lck(lock_);
  const auto it = active_entries_.find(rid);
  if (it == active_entries_.end()) {
    return;
  }

  const auto &locator = it->second;
  const auto &entry = lists_[locator.list_idx_][locator.slot_idx_];
  if (!IsTupleContentEqual(entry.key_, key)) {
    return;
  }

  active_entries_.erase(it);
  stale_entry_count_ += 1;
}

template <typename KT, typename VT, typename Cmp>
void IVFFlatIndex<KT, VT, Cmp>::ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) {
  std::scoped_lock<std::mutex> lck(lock_);

  result->clear();
  if (centroids_.empty()) {
    return;
  }

  const auto closest = FindClosestCentroidIn(centroids_, key);
  for (std::size_t slot_idx = 0; slot_idx < lists_[closest].size(); slot_idx++) {
    const auto &entry = lists_[closest][slot_idx];
    if (!IsActiveEntryUnlocked(entry.rid_, closest, slot_idx, entry.entry_id_)) {
      continue;
    }
    if (IsTupleContentEqual(entry.key_, key)) {
      result->push_back(entry.rid_);
    }
  }
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::SearchKnn(const Tuple &query, size_t k, std::vector<RID> *result,
                                          Transaction *transaction) -> void {
  std::vector<VectorIndexCandidate> candidates;
  SearchVector(query, AnnSearchOptions{k, k, nprobe_}, &candidates, transaction);

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
auto IVFFlatIndex<KT, VT, Cmp>::SearchVector(const Tuple &query, const AnnSearchOptions &options,
                                             std::vector<VectorIndexCandidate> *result, Transaction *transaction)
    -> void {
  std::scoped_lock<std::mutex> lck(lock_);
  result->clear();
  if (centroids_.empty()) {
    return;
  }

  struct RankedCandidate {
    double distance_;
    VectorIndexCandidate candidate_;
    bool is_stale_;
  };

  const auto candidate_budget = std::max<std::size_t>(1, std::max(options.top_k_, options.candidate_budget_));
  const auto search_budget = std::max<std::size_t>(1, options.search_budget_ == 0 ? nprobe_ : options.search_budget_);
  const auto closest_lists = FindClosestCentroidsIn(centroids_, query, std::min(search_budget, centroids_.size()));

  std::vector<RankedCandidate> ranked_candidates;
  for (const auto list_idx : closest_lists) {
    ranked_candidates.reserve(ranked_candidates.size() + lists_[list_idx].size());
    for (std::size_t slot_idx = 0; slot_idx < lists_[list_idx].size(); slot_idx++) {
      const auto &entry = lists_[list_idx][slot_idx];
      ranked_candidates.push_back({Distance(entry.key_, query),
                                   VectorIndexCandidate{entry.rid_, entry.key_, entry.entry_id_},
                                   !IsActiveEntryUnlocked(entry.rid_, list_idx, slot_idx, entry.entry_id_)});
    }
  }

  std::sort(ranked_candidates.begin(), ranked_candidates.end(), [](const auto &a, const auto &b) {
    if (a.distance_ != b.distance_) {
      return a.distance_ < b.distance_;
    }
    return a.candidate_.rid_.Get() < b.candidate_.rid_.Get();
  });

  const auto keep = std::min(candidate_budget, ranked_candidates.size());
  result->reserve(keep);
  for (std::size_t i = 0; i < keep; i++) {
    if (ranked_candidates[i].is_stale_) {
      returned_stale_candidate_count_ += 1;
    }
    result->push_back(ranked_candidates[i].candidate_);
  }

  MaybeRebuildUnlocked();
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::FindClosestCentroid(const Tuple &key) const -> std::size_t {
  std::scoped_lock<std::mutex> lck(lock_);
  return FindClosestCentroidIn(centroids_, key);
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::FindClosestCentroidIn(const std::vector<Tuple> &centroids, const Tuple &key) const
    -> std::size_t {
  BUSTUB_ASSERT(!centroids.empty(), "FindClosestCentroid called with no centroids");

  std::size_t closest = 0;
  double closest_dist = Distance(key, centroids[0]);
  for (std::size_t i = 1; i < centroids.size(); i++) {
    const double dist = Distance(key, centroids[i]);
    if (dist < closest_dist) {
      closest = i;
      closest_dist = dist;
    }
  }
  return closest;
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::FindClosestCentroidsIn(const std::vector<Tuple> &centroids, const Tuple &key,
                                                       std::size_t probe_count) const -> std::vector<std::size_t> {
  BUSTUB_ASSERT(!centroids.empty(), "FindClosestCentroids called with no centroids");

  std::vector<std::pair<double, std::size_t>> centroid_distances;
  centroid_distances.reserve(centroids.size());
  for (std::size_t i = 0; i < centroids.size(); i++) {
    centroid_distances.emplace_back(Distance(key, centroids[i]), i);
  }

  const auto keep = std::min(probe_count, centroid_distances.size());
  std::partial_sort(centroid_distances.begin(), centroid_distances.begin() + keep, centroid_distances.end(),
                    [](const auto &a, const auto &b) { return a.first < b.first; });

  std::vector<std::size_t> result;
  result.reserve(keep);
  for (std::size_t i = 0; i < keep; i++) {
    result.push_back(centroid_distances[i].second);
  }
  return result;
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::Distance(const Tuple &a, const Tuple &b) const -> double {
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
  UNREACHABLE("unsupported IVFFlat metric");
}

}  // namespace bustub
