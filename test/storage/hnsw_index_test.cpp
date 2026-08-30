#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "../txn/txn_common.h"
#include "common/bustub_instance.h"
#include "common/util/string_util.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "storage/index/hnsw_index.h"
#include "storage/index/int_comparator.h"

namespace bustub {

namespace {

struct IndexedVectorEntry {
  int32_t id_;
  Tuple key_;
  RID rid_;
};

void ExecuteStatement(BusTubInstance &instance, const std::string &sql) {
  NoopWriter writer;
  ASSERT_TRUE(instance.ExecuteSql(sql, writer));
}

auto QueryRows(BusTubInstance &instance, const std::string &sql) -> std::vector<std::vector<std::string>> {
  StringVectorWriter writer;
  EXPECT_TRUE(instance.ExecuteSql(sql, writer));
  return writer.values_;
}

auto QueryRowsTxn(BusTubInstance &instance, Transaction *txn, const std::string &sql)
    -> std::vector<std::vector<std::string>> {
  StringVectorWriter writer;
  EXPECT_TRUE(instance.ExecuteSqlTxn(sql, writer, txn));
  return writer.values_;
}

auto ExplainPlan(BusTubInstance &instance, const std::string &sql) -> std::string {
  std::stringstream ss;
  SimpleStreamWriter writer(ss);
  EXPECT_TRUE(instance.ExecuteSql("EXPLAIN (o) " + sql, writer));
  return ss.str();
}

/** 作用：从 catalog 中取出具体 HNSW 索引对象，便于测试参数和搜索行为。 */
auto GetHNSWIndex(BusTubInstance &instance, const std::string &table_name, const std::string &index_name)
    -> HNSWIndex<Tuple, RID, IntComparator> * {
  auto index_info = instance.catalog_->GetIndex(index_name, table_name);
  EXPECT_NE(index_info, nullptr);
  if (index_info == nullptr) {
    return nullptr;
  }
  auto *hnsw = dynamic_cast<HNSWIndex<Tuple, RID, IntComparator> *>(index_info->index_.get());
  EXPECT_NE(hnsw, nullptr);
  return hnsw;
}

auto CollectIndexedEntries(const TableInfo *table_info, const Index *index) -> std::vector<IndexedVectorEntry> {
  std::vector<IndexedVectorEntry> entries;
  for (auto iter = table_info->table_->MakeIterator(); !iter.IsEnd(); ++iter) {
    auto [meta, tuple] = iter.GetTuple();
    if (meta.is_deleted_) {
      continue;
    }
    entries.push_back({
        tuple.GetValue(&table_info->schema_, 1).GetAs<int32_t>(),
        tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), index->GetKeyAttrs()),
        tuple.GetRid(),
    });
  }
  return entries;
}

auto FindRidById(const std::vector<IndexedVectorEntry> &entries, int32_t id) -> RID {
  for (const auto &entry : entries) {
    if (entry.id_ == id) {
      return entry.rid_;
    }
  }
  ADD_FAILURE() << "RID not found for id=" << id;
  return {};
}

auto MakeQueryTuple(const std::vector<double> &values, const Index *index) -> Tuple {
  return Tuple({ValueFactory::GetVectorValue(values)}, index->GetKeySchema());
}

/** 作用：计算 ANN 结果命中 exact top-k 的数量，用来验证 recall 不会下降。 */
auto RecallAtK(const std::vector<VectorIndexCandidate> &actual, const std::unordered_set<int64_t> &expected_rids)
    -> std::size_t {
  std::size_t matched = 0;
  std::unordered_set<int64_t> seen;
  for (const auto &candidate : actual) {
    if (!seen.emplace(candidate.rid_.Get()).second) {
      continue;
    }
    if (expected_rids.count(candidate.rid_.Get()) != 0) {
      matched += 1;
    }
  }
  return matched;
}

}  // namespace

TEST(HNSWIndexTest, BuildIndexFromExistingTuples) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE hnsw_build(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub,
                   "INSERT INTO hnsw_build VALUES "
                   "(ARRAY [1.0, 0.0, 0.0], 1), "
                   "(ARRAY [2.0, 0.0, 0.0], 2), "
                   "(ARRAY [3.0, 0.0, 0.0], 3), "
                   "(ARRAY [10.0, 0.0, 0.0], 4)");
  ExecuteStatement(*bustub,
                   "CREATE INDEX hnsw_build_idx ON hnsw_build USING hnsw (v) WITH (m = 4, ef_construction = 16, "
                   "ef_search = 8)");

  auto *index = GetHNSWIndex(*bustub, "hnsw_build", "hnsw_build_idx");
  ASSERT_NE(index, nullptr);
  EXPECT_EQ(index->GetM(), 4U);
  EXPECT_EQ(index->GetEfConstruction(), 16U);
  EXPECT_EQ(index->GetEfSearch(), 8U);
  EXPECT_GE(index->GetMaxLevel(), 0);

  auto table_info = bustub->catalog_->GetTable("hnsw_build");
  ASSERT_NE(table_info, nullptr);
  const auto entries = CollectIndexedEntries(table_info.get(), index);
  ASSERT_EQ(entries.size(), 4U);

  for (const auto &entry : entries) {
    std::vector<RID> scan_result;
    index->ScanKey(entry.key_, &scan_result, nullptr);
    EXPECT_NE(std::find(scan_result.begin(), scan_result.end(), entry.rid_), scan_result.end());
  }
}

TEST(HNSWIndexTest, MatchesExactQueryOnSmallDataset) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE hnsw_exact(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub,
                   "INSERT INTO hnsw_exact VALUES "
                   "(ARRAY [0.0, 0.0, 0.0], 1), "
                   "(ARRAY [1.0, 0.0, 0.0], 2), "
                   "(ARRAY [2.0, 0.0, 0.0], 3), "
                   "(ARRAY [3.0, 0.0, 0.0], 4), "
                   "(ARRAY [20.0, 0.0, 0.0], 5)");
  ExecuteStatement(*bustub, "CREATE INDEX hnsw_exact_idx ON hnsw_exact USING hnsw (v) WITH (m = 4, ef_search = 8)");

  const auto sql = "SELECT id FROM hnsw_exact ORDER BY l2_distance(v, ARRAY [1.2, 0.0, 0.0]) LIMIT 3";
  const auto optimized_plan = ExplainPlan(*bustub, sql);
  EXPECT_TRUE(StringUtil::Contains(optimized_plan, "VectorIndexScan")) << optimized_plan;

  const auto optimized_rows = QueryRows(*bustub, sql);
  ExecuteStatement(*bustub, "SET force_optimizer_starter_rule=yes");
  const auto exact_rows = QueryRows(*bustub, sql);
  EXPECT_EQ(optimized_rows, exact_rows);
}

TEST(HNSWIndexTest, HonorsMetricAndSearchBudget) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE hnsw_metric(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub,
                   "INSERT INTO hnsw_metric VALUES "
                   "(ARRAY [2.0, 1.0, 0.0], 1), "
                   "(ARRAY [10.0, 0.0, 0.0], 2), "
                   "(ARRAY [0.0, 10.0, 0.0], 3), "
                   "(ARRAY [9.0, 1.0, 0.0], 4), "
                   "(ARRAY [8.0, 2.0, 0.0], 5)");
  ExecuteStatement(*bustub, "CREATE INDEX hnsw_metric_l2 ON hnsw_metric USING hnsw (v) WITH (metric = 'l2')");
  ExecuteStatement(*bustub, "CREATE INDEX hnsw_metric_ip ON hnsw_metric USING hnsw (v) WITH (metric = 'ip')");

  auto *l2_index = GetHNSWIndex(*bustub, "hnsw_metric", "hnsw_metric_l2");
  auto *ip_index = GetHNSWIndex(*bustub, "hnsw_metric", "hnsw_metric_ip");
  ASSERT_NE(l2_index, nullptr);
  ASSERT_NE(ip_index, nullptr);
  EXPECT_EQ(l2_index->GetVectorDistanceMetric(), VectorIndexDistanceMetric::L2);
  EXPECT_EQ(ip_index->GetVectorDistanceMetric(), VectorIndexDistanceMetric::InnerProduct);

  auto table_info = bustub->catalog_->GetTable("hnsw_metric");
  ASSERT_NE(table_info, nullptr);
  const auto entries = CollectIndexedEntries(table_info.get(), l2_index);

  const auto l2_query = MakeQueryTuple({2.1, 1.0, 0.0}, l2_index);
  std::vector<RID> l2_result;
  std::vector<RID> ip_result;
  l2_index->SearchKnn(l2_query, 1, &l2_result, nullptr);
  ip_index->SearchKnn(l2_query, 1, &ip_result, nullptr);
  ASSERT_EQ(l2_result.size(), 1U);
  ASSERT_EQ(ip_result.size(), 1U);
  EXPECT_EQ(l2_result[0], FindRidById(entries, 1));
  EXPECT_EQ(ip_result[0], FindRidById(entries, 2));

  const auto budget_query = MakeQueryTuple({8.5, 1.0, 0.0}, l2_index);
  std::vector<VectorIndexCandidate> budget1_result;
  std::vector<VectorIndexCandidate> budget8_result;
  l2_index->SearchVector(budget_query, AnnSearchOptions{2, 2, 1}, &budget1_result, nullptr);
  l2_index->SearchVector(budget_query, AnnSearchOptions{2, 2, 8}, &budget8_result, nullptr);

  const std::unordered_set<int64_t> exact_top2 = {FindRidById(entries, 4).Get(), FindRidById(entries, 5).Get()};
  EXPECT_GE(RecallAtK(budget8_result, exact_top2), RecallAtK(budget1_result, exact_top2));
}

TEST(HNSWIndexTest, OptimizerPrefersHNSWForSmallTopK) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE hnsw_choose(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub,
                   "INSERT INTO hnsw_choose VALUES "
                   "(ARRAY [0.0, 0.0, 0.0], 1), "
                   "(ARRAY [1.0, 0.0, 0.0], 2), "
                   "(ARRAY [2.0, 0.0, 0.0], 3), "
                   "(ARRAY [3.0, 0.0, 0.0], 4), "
                   "(ARRAY [4.0, 0.0, 0.0], 5)");
  ExecuteStatement(*bustub,
                   "CREATE INDEX choose_ivf_idx ON hnsw_choose USING ivfflat (v) WITH (nlist = 2, nprobe = 1)");
  ExecuteStatement(*bustub, "CREATE INDEX choose_hnsw_idx ON hnsw_choose USING hnsw (v) WITH (m = 4, ef_search = 8)");

  const auto plan =
      ExplainPlan(*bustub, "SELECT id FROM hnsw_choose ORDER BY l2_distance(v, ARRAY [1.1, 0.0, 0.0]) LIMIT 3");
  const auto hnsw_oid = bustub->catalog_->GetIndex("choose_hnsw_idx", "hnsw_choose")->index_oid_;
  EXPECT_TRUE(StringUtil::Contains(plan, fmt::format("index_oid={}", hnsw_oid))) << plan;
}

TEST(HNSWIndexTest, MetricMismatchDoesNotUseHNSW) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE hnsw_mismatch(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub,
                   "INSERT INTO hnsw_mismatch VALUES "
                   "(ARRAY [1.0, 0.0, 0.0], 1), "
                   "(ARRAY [1.0, 1.0, 0.0], 2), "
                   "(ARRAY [0.0, 1.0, 0.0], 3)");
  ExecuteStatement(*bustub, "CREATE INDEX mismatch_hnsw_idx ON hnsw_mismatch USING hnsw (v) WITH (metric = 'l2')");
  ExecuteStatement(*bustub,
                   "CREATE INDEX mismatch_ivf_idx ON hnsw_mismatch USING ivfflat (v) WITH (metric = 'cosine')");

  const auto plan =
      ExplainPlan(*bustub, "SELECT id FROM hnsw_mismatch ORDER BY cosine_distance(v, ARRAY [1.0, 0.0, 0.0]) LIMIT 2");
  const auto ivf_oid = bustub->catalog_->GetIndex("mismatch_ivf_idx", "hnsw_mismatch")->index_oid_;
  EXPECT_TRUE(StringUtil::Contains(plan, fmt::format("index_oid={}", ivf_oid))) << plan;
}

TEST(HNSWIndexTest, VectorIndexScanRespectsMvccAndFilter) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE hnsw_mvcc(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub, "CREATE INDEX hnsw_mvcc_idx ON hnsw_mvcc USING hnsw (v) WITH (m = 4, ef_search = 8)");
  ExecuteStatement(*bustub,
                   "INSERT INTO hnsw_mvcc VALUES "
                   "(ARRAY [5.0, 0.0, 0.0], 5), "
                   "(ARRAY [10.0, 0.0, 0.0], 10), "
                   "(ARRAY [15.0, 0.0, 0.0], 15)");

  const auto sql =
      "SELECT id FROM hnsw_mvcc WHERE id <> 5 "
      "ORDER BY l2_distance(v, ARRAY [0.0, 0.0, 0.0]) LIMIT 2";
  const auto plan = ExplainPlan(*bustub, sql);
  EXPECT_TRUE(StringUtil::Contains(plan, "VectorIndexScan")) << plan;
  EXPECT_TRUE(StringUtil::Contains(plan, "filter=(#0.1!=5)")) << plan;

  auto *reader_txn = BeginTxn(*bustub, "reader_txn");
  auto *writer_txn = BeginTxn(*bustub, "writer_txn");
  WithTxn(writer_txn, ExecuteTxn(*bustub, _var, _txn, "INSERT INTO hnsw_mvcc VALUES (ARRAY [1.0, 0.0, 0.0], 1)"));
  WithTxn(writer_txn, CommitTxn(*bustub, _var, _txn));

  EXPECT_EQ((QueryRowsTxn(*bustub, reader_txn, sql)), (std::vector<std::vector<std::string>>{{"10"}, {"15"}}));

  auto *fresh_txn = BeginTxn(*bustub, "fresh_txn");
  EXPECT_EQ((QueryRowsTxn(*bustub, fresh_txn, sql)), (std::vector<std::vector<std::string>>{{"1"}, {"10"}}));

  WithTxn(reader_txn, CommitTxn(*bustub, _var, _txn));
  WithTxn(fresh_txn, CommitTxn(*bustub, _var, _txn));
}

TEST(HNSWIndexTest, FallsBackWhenRewriteIsImpossible) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE hnsw_fallback(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub,
                   "INSERT INTO hnsw_fallback VALUES "
                   "(ARRAY [1.0, 0.0, 0.0], 1), "
                   "(ARRAY [2.0, 0.0, 0.0], 2)");
  ExecuteStatement(*bustub, "CREATE INDEX hnsw_fallback_idx ON hnsw_fallback USING hnsw (v)");

  const auto plan = ExplainPlan(*bustub, "SELECT id FROM hnsw_fallback ORDER BY l2_distance(v, v) LIMIT 2");
  EXPECT_FALSE(StringUtil::Contains(plan, "VectorIndexScan")) << plan;
}

}  // namespace bustub
