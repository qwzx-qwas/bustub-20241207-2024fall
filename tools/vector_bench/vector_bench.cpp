#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/column.h"
#include "catalog/schema.h"
#include "fmt/core.h"
#include "storage/index/hnsw_index.h"
#include "storage/index/int_comparator.h"
#include "storage/index/ivfflat_index.h"
#include "type/value_factory.h"

namespace bustub {

namespace {

struct BenchConfig {
  std::size_t dataset_size_{2000};
  std::size_t query_count_{200};
  std::size_t dim_{16};
  VectorIndexDistanceMetric metric_{VectorIndexDistanceMetric::L2};
  std::vector<std::size_t> top_ks_{1, 10, 50};
  std::vector<std::size_t> ivf_budgets_{1, 2, 4, 8};
  std::vector<std::size_t> hnsw_budgets_{4, 8, 16, 32};
};

struct BenchEntry {
  std::vector<double> vector_;
  Tuple key_;
  RID rid_;
};

struct QueryWorkload {
  std::vector<double> vector_;
  Tuple key_;
  std::vector<std::pair<double, RID>> exact_rank_;
};

/** 作用：生成固定 schema，确保 IVFFlat、HNSW 和 exact baseline 使用同一向量列定义。 */
auto MakeVectorSchema(std::size_t dim) -> Schema { return Schema({Column("v", TypeId::VECTOR, dim)}); }

/** 作用：把裸向量封装成索引统一使用的单列 tuple。 */
auto MakeVectorTuple(const std::vector<double> &vector, const Schema &schema) -> Tuple {
  return Tuple({ValueFactory::GetVectorValue(vector)}, &schema);
}

/** 作用：用与索引相同的距离语义计算 exact baseline。 */
auto ComputeDistance(const std::vector<double> &lhs, const std::vector<double> &rhs, VectorIndexDistanceMetric metric) -> double {
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

  switch (metric) {
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
  UNREACHABLE("unsupported metric");
}

/** 作用：生成可复现的簇状数据，让 ANN budget 改变时 recall-latency 差异更容易观察。 */
auto GenerateDataset(const BenchConfig &config, const Schema &schema) -> std::vector<BenchEntry> {
  std::mt19937 rng(15445);
  std::normal_distribution<double> center_dist(0.0, 12.0);
  std::normal_distribution<double> point_noise(0.0, 1.2);
  std::vector<std::vector<double>> centers(8, std::vector<double>(config.dim_, 0.0));
  for (auto &center : centers) {
    for (auto &value : center) {
      value = center_dist(rng);
    }
  }

  std::vector<BenchEntry> entries;
  entries.reserve(config.dataset_size_);
  for (std::size_t i = 0; i < config.dataset_size_; i++) {
    const auto &center = centers[i % centers.size()];
    std::vector<double> vector(config.dim_, 0.0);
    for (std::size_t d = 0; d < config.dim_; d++) {
      vector[d] = center[d] + point_noise(rng);
    }
    entries.push_back({vector, MakeVectorTuple(vector, schema), RID(static_cast<int64_t>(i + 1))});
  }
  return entries;
}

/** 作用：从数据集中抽取固定 query，并预计算 exact 排序结果作为 recall baseline。 */
auto GenerateQueries(const BenchConfig &config, const Schema &schema, const std::vector<BenchEntry> &entries)
    -> std::vector<QueryWorkload> {
  std::mt19937 rng(2024);
  std::normal_distribution<double> query_noise(0.0, 0.8);
  std::vector<QueryWorkload> queries;
  queries.reserve(config.query_count_);

  for (std::size_t i = 0; i < config.query_count_; i++) {
    const auto &base = entries[(i * 17) % entries.size()].vector_;
    std::vector<double> query = base;
    for (auto &value : query) {
      value += query_noise(rng);
    }

    std::vector<std::pair<double, RID>> exact_rank;
    exact_rank.reserve(entries.size());
    for (const auto &entry : entries) {
      exact_rank.emplace_back(ComputeDistance(query, entry.vector_, config.metric_), entry.rid_);
    }
    std::sort(exact_rank.begin(), exact_rank.end(), [](const auto &lhs, const auto &rhs) {
      if (lhs.first != rhs.first) {
        return lhs.first < rhs.first;
      }
      return lhs.second.Get() < rhs.second.Get();
    });

    queries.push_back({query, MakeVectorTuple(query, schema), std::move(exact_rank)});
  }
  return queries;
}

/** 作用：提取 exact top-k 的 RID 集合，供 recall@k 统计使用。 */
auto GetExactTopKSet(const QueryWorkload &query, std::size_t top_k) -> std::unordered_set<int64_t> {
  std::unordered_set<int64_t> result;
  const auto keep = std::min(top_k, query.exact_rank_.size());
  for (std::size_t i = 0; i < keep; i++) {
    result.emplace(query.exact_rank_[i].second.Get());
  }
  return result;
}

/** 作用：统计 ANN 返回结果对 exact top-k 的平均 recall。 */
auto ComputeRecall(const std::vector<VectorIndexCandidate> &candidates, const std::unordered_set<int64_t> &exact_top_k)
    -> double {
  if (exact_top_k.empty()) {
    return 1.0;
  }

  std::unordered_set<int64_t> seen;
  std::size_t matched = 0;
  for (const auto &candidate : candidates) {
    if (!seen.emplace(candidate.rid_.Get()).second) {
      continue;
    }
    if (exact_top_k.count(candidate.rid_.Get()) != 0) {
      matched += 1;
    }
  }
  return static_cast<double>(matched) / static_cast<double>(exact_top_k.size());
}

/** 作用：执行一组 ANN 查询并输出 recall-latency 统计。 */
template <typename IndexT>
void RunAnnBench(const std::string &label, IndexT *index, const std::vector<QueryWorkload> &queries, std::size_t top_k,
                 std::size_t budget) {
  double total_recall = 0.0;
  double total_latency_us = 0.0;

  for (const auto &query : queries) {
    const auto exact_top_k = GetExactTopKSet(query, top_k);
    std::vector<VectorIndexCandidate> candidates;
    const auto start = std::chrono::steady_clock::now();
    index->SearchVector(query.key_, AnnSearchOptions{top_k, top_k, budget}, &candidates, nullptr);
    const auto end = std::chrono::steady_clock::now();
    total_latency_us +=
        static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
    total_recall += ComputeRecall(candidates, exact_top_k);
  }

  fmt::print("{},{},{},{:.4f},{:.2f}\n", label, top_k, budget, total_recall / static_cast<double>(queries.size()),
             total_latency_us / static_cast<double>(queries.size()));
}

/** 作用：真实执行一次全量 exact KNN，给实验提供公平的 latency baseline。 */
void RunExactBench(const std::vector<QueryWorkload> &queries, const std::vector<BenchEntry> &entries, std::size_t top_k,
                   VectorIndexDistanceMetric metric) {
  double total_latency_us = 0.0;
  for (const auto &query : queries) {
    const auto start = std::chrono::steady_clock::now();
    std::vector<std::pair<double, RID>> exact_rank;
    exact_rank.reserve(entries.size());
    for (const auto &entry : entries) {
      exact_rank.emplace_back(ComputeDistance(query.vector_, entry.vector_, metric), entry.rid_);
    }
    const auto keep = std::min(top_k, exact_rank.size());
    std::partial_sort(exact_rank.begin(), exact_rank.begin() + keep, exact_rank.end(), [](const auto &lhs, const auto &rhs) {
      if (lhs.first != rhs.first) {
        return lhs.first < rhs.first;
      }
      return lhs.second.Get() < rhs.second.Get();
    });
    volatile auto sink = exact_rank[keep - 1].second.Get();
    static_cast<void>(sink);
    const auto end = std::chrono::steady_clock::now();
    total_latency_us +=
        static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
  }
  fmt::print("{},{},{},{:.4f},{:.2f}\n", "exact", top_k, 0, 1.0, total_latency_us / static_cast<double>(queries.size()));
}

/** 作用：解析少量命令行参数，方便重复实验时快速切换规模和 metric。 */
auto ParseConfig(int argc, char **argv) -> BenchConfig {
  BenchConfig config;
  for (int i = 1; i < argc; i++) {
    const std::string arg = argv[i];
    if (arg == "--dataset-size" && i + 1 < argc) {
      config.dataset_size_ = static_cast<std::size_t>(std::stoull(argv[++i]));
      continue;
    }
    if (arg == "--queries" && i + 1 < argc) {
      config.query_count_ = static_cast<std::size_t>(std::stoull(argv[++i]));
      continue;
    }
    if (arg == "--dim" && i + 1 < argc) {
      config.dim_ = static_cast<std::size_t>(std::stoull(argv[++i]));
      continue;
    }
    if (arg == "--metric" && i + 1 < argc) {
      const std::string metric = argv[++i];
      if (metric == "l2") {
        config.metric_ = VectorIndexDistanceMetric::L2;
      } else if (metric == "cosine") {
        config.metric_ = VectorIndexDistanceMetric::Cosine;
      } else if (metric == "ip") {
        config.metric_ = VectorIndexDistanceMetric::InnerProduct;
      } else {
        throw bustub::Exception("metric must be one of: l2, cosine, ip");
      }
      continue;
    }
    throw bustub::Exception("unknown argument: " + arg);
  }
  return config;
}

}  // namespace

}  // namespace bustub

auto main(int argc, char **argv) -> int {
  using namespace bustub;

  const auto config = ParseConfig(argc, argv);
  const auto schema = MakeVectorSchema(config.dim_);
  const auto entries = GenerateDataset(config, schema);
  const auto queries = GenerateQueries(config, schema, entries);

  std::vector<std::pair<Tuple, RID>> build_entries;
  build_entries.reserve(entries.size());
  for (const auto &entry : entries) {
    build_entries.emplace_back(entry.key_, entry.rid_);
  }

  auto ivf_meta = std::make_unique<IndexMetadata>("bench_ivf", "bench", &schema, std::vector<uint32_t>{0}, false);
  auto hnsw_meta = std::make_unique<IndexMetadata>("bench_hnsw", "bench", &schema, std::vector<uint32_t>{0}, false);

  const auto nlist = std::max<std::size_t>(1, static_cast<std::size_t>(std::sqrt(static_cast<double>(entries.size()))));
  auto ivf = IVFFlatIndex<Tuple, RID, IntComparator>(
      std::move(ivf_meta), nullptr, HashFunction<Tuple>{},
      IVFFlatIndexOptions{nlist, std::min<std::size_t>(4, nlist), config.metric_, 4, 5});
  auto hnsw = HNSWIndex<Tuple, RID, IntComparator>(
      std::move(hnsw_meta), nullptr, HashFunction<Tuple>{}, HNSWIndexOptions{8, 32, 16, config.metric_});
  ivf.BuildFromEntries(build_entries);
  hnsw.BuildFromEntries(build_entries);

  fmt::print("# dataset_size={}, queries={}, dim={}, metric={}\n", config.dataset_size_, config.query_count_, config.dim_,
             config.metric_ == VectorIndexDistanceMetric::L2
                 ? "l2"
                 : (config.metric_ == VectorIndexDistanceMetric::Cosine ? "cosine" : "ip"));
  fmt::print("algo,top_k,search_budget,avg_recall,avg_latency_us\n");

  for (const auto top_k : config.top_ks_) {
    RunExactBench(queries, entries, top_k, config.metric_);
    for (const auto budget : config.ivf_budgets_) {
      RunAnnBench("ivfflat", &ivf, queries, top_k, std::min(budget, nlist));
    }
    for (const auto budget : config.hnsw_budgets_) {
      RunAnnBench("hnsw", &hnsw, queries, top_k, budget);
    }
  }

  return 0;
}
