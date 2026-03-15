#pragma once

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "container/hash/hash_function.h"
#include "buffer/buffer_pool_manager.h"
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
  IVFFlatIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
               const HashFunction<KT> &hash_fn, IVFFlatIndexOptions options = {})
      : Index(std::move(metadata)),
        nlist_(std::max<std::size_t>(1, options.nlist_)),
        nprobe_(std::max<std::size_t>(1, std::min(options.nprobe_, nlist_))),
        metric_(options.metric_),
        sample_multiplier_(std::max<std::size_t>(1, options.sample_multiplier_)),
        kmeans_iterations_(std::max<std::size_t>(1, options.kmeans_iterations_)) {}

  // Build the IVF state from existing table entries when the index is created.
  void BuildFromEntries(const std::vector<std::pair<Tuple, RID>> &entries) {
    std::scoped_lock<std::mutex> lck(lock_);
    // 刷新IVF状态以从头开始重建。
    ResetBuildStateUnlocked();
    if (entries.empty()) {
      return;
    }
    // 选样本
    auto samples = SampleEntriesUnlocked(entries);
    // 训练centroid
    TrainCentroidsUnlocked(samples);
    // 将数据分配到列表中
    AssignEntriesToListsUnlocked(entries);
  }

  auto InsertEntry(const Tuple &key, VT rid, Transaction *transaction) -> bool override;

  void DeleteEntry(const Tuple &key, VT rid, Transaction *transaction) override;

  void ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) override;

  auto SearchKnn(const Tuple &query, size_t k, std::vector<RID> *result, Transaction *transaction) -> void override;

  auto SearchKnnWithProbe(const Tuple &query, size_t k, std::size_t probe_count, std::vector<RID> *result,
                          Transaction *transaction) -> void override;

  auto GetVectorDistanceMetric() const -> std::optional<VectorIndexDistanceMetric> override { return metric_; }

  auto GetDefaultKnnProbeCount() const -> std::optional<std::size_t> override { return nprobe_; }

  auto GetMaxKnnProbeCount() const -> std::optional<std::size_t> override { return lists_.empty() ? nlist_ : lists_.size(); }

  auto GetNList() const -> std::size_t { return nlist_; }

  auto GetNProbe() const -> std::size_t { return nprobe_; }

  auto FindClosestCentroid(const Tuple &key) const -> std::size_t;

  auto Distance(const Tuple &a, const Tuple &b) const -> double;

 private:
  // Clear all in-memory IVF state before rebuilding from scratch.
  void ResetBuildStateUnlocked();

  // Pick a representative subset for centroid training without revisiting the
  // full dataset in every training iteration.
  auto SampleEntriesUnlocked(const std::vector<std::pair<Tuple, RID>> &entries) const
      -> std::vector<std::pair<Tuple, RID>>;

  // Train centroids from sampled entries using a small fixed number of k-means
  // iterations. This keeps the MVP implementation simple but useful.
  void TrainCentroidsUnlocked(const std::vector<std::pair<Tuple, RID>> &samples);

  // Assign the full dataset to the trained centroids and materialize IVF lists.
  void AssignEntriesToListsUnlocked(const std::vector<std::pair<Tuple, RID>> &entries);

  // IVFFlat currently indexes a single VECTOR column, so each key tuple unwraps
  // to one std::vector<double>.
  auto ExtractVector(const Tuple &key) const -> std::vector<double>;

  auto MakeVectorTuple(const std::vector<double> &vector) const -> Tuple;

  auto FindClosestCentroidUnlocked(const Tuple &key) const -> std::size_t;

  auto FindClosestCentroidsUnlocked(const Tuple &key, std::size_t probe_count) const -> std::vector<std::size_t>;

  mutable std::mutex lock_;
  std::vector<Tuple> centroids_;
  std::vector<std::vector<std::pair<Tuple, RID>>> lists_;
  std::unordered_map<RID, std::size_t> rid_to_list_;
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
  rid_to_list_.clear();
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

  // A fixed-stride sample is deterministic and cheap, which is sufficient for
  // the first IVFFlat implementation.
  // 第一版：固定步长采样
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
void IVFFlatIndex<KT, VT, Cmp>::TrainCentroidsUnlocked(const std::vector<std::pair<Tuple, RID>> &samples) {
  if (samples.empty()) {
    return;
  }

  // Seed centroids from sampled entries, then refine them with a few Lloyd
  // iterations over the sample set.
  const auto centroid_count = std::min(nlist_, samples.size());
  centroids_.reserve(centroid_count);
  for (std::size_t i = 0; i < centroid_count; i++) {
    centroids_.push_back(samples[i].first);
  }

  if (centroids_.empty()) {
    return;
  }

  const auto dim = ExtractVector(samples.front().first).size();
  for (std::size_t iter = 0; iter < kmeans_iterations_; iter++) {
    std::vector<std::vector<double>> sums(centroids_.size(), std::vector<double>(dim, 0.0));
    std::vector<std::size_t> counts(centroids_.size(), 0);
    
    // 把每个sample分配到最近的centroid的桶中，并累加这个桶的和与数量
    for (const auto &[key, rid] : samples) {
      const auto centroid_idx = FindClosestCentroidUnlocked(key);
      const auto vector = ExtractVector(key);
      for (std::size_t d = 0; d < dim; d++) {
        sums[centroid_idx][d] += vector[d];
      }
      counts[centroid_idx] += 1;
    }

    // Empty clusters keep their previous centroid instead of forcing a re-seed.
    for (std::size_t centroid_idx = 0; centroid_idx < centroids_.size(); centroid_idx++) {
      if (counts[centroid_idx] == 0) {
        continue;
      }
      for (std::size_t d = 0; d < dim; d++) {
        // 得到这个桶在d维度的均值
        sums[centroid_idx][d] /= static_cast<double>(counts[centroid_idx]);
      }
      // 得到平均向量，更新centroid
      centroids_[centroid_idx] = MakeVectorTuple(sums[centroid_idx]);
    }
  }
}

template <typename KT, typename VT, typename Cmp>
void IVFFlatIndex<KT, VT, Cmp>::AssignEntriesToListsUnlocked(const std::vector<std::pair<Tuple, RID>> &entries) {
  if (centroids_.empty()) {
    return;
  }

  // The final lists are built from the full dataset, not just the training sample.
  lists_.clear();
  rid_to_list_.clear();
  lists_.resize(centroids_.size());
  // 把每个entry分配到最近的centroid的桶中，并记录rid到桶的映射
  for (const auto &[key, rid] : entries) {
    const auto centroid_idx = FindClosestCentroidUnlocked(key);
    lists_[centroid_idx].push_back({key, rid});
    rid_to_list_[rid] = centroid_idx;
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
  if (rid_to_list_.find(rid) != rid_to_list_.end()) {
    return false;
  }

  if (centroids_.empty()) {
    centroids_.push_back(key);
    lists_.resize(1);
  }

  const auto closest = FindClosestCentroidUnlocked(key);
  lists_[closest].push_back({key, rid});
  rid_to_list_[rid] = closest;
  return true;
}

template <typename KT, typename VT, typename Cmp>
void IVFFlatIndex<KT, VT, Cmp>::DeleteEntry(const Tuple &key, VT rid, Transaction *transaction) {
  std::scoped_lock<std::mutex> lck(lock_);
  const auto it = rid_to_list_.find(rid);
  if (it == rid_to_list_.end()) {
    return;
  }

  auto &bucket = lists_[it->second];
  bucket.erase(std::remove_if(bucket.begin(), bucket.end(),
                              [&](const auto &entry) {
                                return entry.second == rid && IsTupleContentEqual(entry.first, key);
                              }),
               bucket.end());
  rid_to_list_.erase(it);
}

template <typename KT, typename VT, typename Cmp>
void IVFFlatIndex<KT, VT, Cmp>::ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) {
  std::scoped_lock<std::mutex> lck(lock_);

  result->clear();
  if (centroids_.empty()) {
    return;
  }

  const auto closest = FindClosestCentroidUnlocked(key);
  for (const auto &[existing_key, existing_rid] : lists_[closest]) {
    if (IsTupleContentEqual(existing_key, key)) {
      result->push_back(existing_rid);
    }
  }
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::SearchKnn(const Tuple &query, size_t k, std::vector<RID> *result,
                                          Transaction *transaction) -> void {
  // Preserve the stage-1 behavior for callers that just want the index default.
  SearchKnnWithProbe(query, k, nprobe_, result, transaction);
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::SearchKnnWithProbe(const Tuple &query, size_t k, std::size_t probe_count,
                                                   std::vector<RID> *result, Transaction *transaction) -> void {
  std::scoped_lock<std::mutex> lck(lock_);
  result->clear();
  if (centroids_.empty()) {
    return;
  }

  // Stage 2 may temporarily widen the search radius beyond the index default
  // to recover enough post-filter candidates.
  const auto effective_probe_count = std::max<std::size_t>(1, std::min(probe_count, centroids_.size()));
  const auto closest_lists = FindClosestCentroidsUnlocked(query, effective_probe_count);
  std::vector<std::pair<double, RID>> dist_rids;
  for (const auto list_idx : closest_lists) {
    dist_rids.reserve(dist_rids.size() + lists_[list_idx].size());
    for (const auto &[existing_key, existing_rid] : lists_[list_idx]) {
      dist_rids.emplace_back(Distance(existing_key, query), existing_rid);
    }
  }

  std::sort(dist_rids.begin(), dist_rids.end(), [](const auto &a, const auto &b) { return a.first < b.first; });
  for (std::size_t i = 0; i < std::min(k, dist_rids.size()); i++) {
    result->push_back(dist_rids[i].second);
  }
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::FindClosestCentroid(const Tuple &key) const -> std::size_t {
  std::scoped_lock<std::mutex> lck(lock_);
  return FindClosestCentroidUnlocked(key);
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::FindClosestCentroidUnlocked(const Tuple &key) const -> std::size_t {
  BUSTUB_ASSERT(!centroids_.empty(), "FindClosestCentroid called with no centroids");

  std::size_t closest = 0;
  double closest_dist = Distance(key, centroids_[0]);
  for (std::size_t i = 1; i < centroids_.size(); i++) {
    const double dist = Distance(key, centroids_[i]);
    if (dist < closest_dist) {
      closest = i;
      closest_dist = dist;
    }
  }
  return closest;
}

template <typename KT, typename VT, typename Cmp>
auto IVFFlatIndex<KT, VT, Cmp>::FindClosestCentroidsUnlocked(const Tuple &key, std::size_t probe_count) const
    -> std::vector<std::size_t> {
  BUSTUB_ASSERT(!centroids_.empty(), "FindClosestCentroids called with no centroids");

  std::vector<std::pair<double, std::size_t>> centroid_distances;
  centroid_distances.reserve(centroids_.size());
  for (std::size_t i = 0; i < centroids_.size(); i++) {
    centroid_distances.emplace_back(Distance(key, centroids_[i]), i);
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
