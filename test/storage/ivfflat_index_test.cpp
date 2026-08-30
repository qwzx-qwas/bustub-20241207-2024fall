#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../txn/txn_common.h"
#include "common/bustub_instance.h"
#include "common/util/string_util.h"
#include "gtest/gtest.h"
#include "storage/index/int_comparator.h"
#include "storage/index/ivfflat_index.h"

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

void ExecuteStatementExpectFailure(BusTubInstance &instance, const std::string &sql) {
  NoopWriter writer;
  EXPECT_THROW(instance.ExecuteSql(sql, writer), Exception);
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

auto GetIVFFlatIndex(BusTubInstance &instance, const std::string &table_name, const std::string &index_name)
    -> IVFFlatIndex<Tuple, RID, IntComparator> * {
  auto index_info = instance.catalog_->GetIndex(index_name, table_name);
  EXPECT_NE(index_info, nullptr);
  if (index_info == nullptr) {
    return nullptr;
  }
  auto *ivf = dynamic_cast<IVFFlatIndex<Tuple, RID, IntComparator> *>(index_info->index_.get());
  EXPECT_NE(ivf, nullptr);
  return ivf;
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

void AssertIndexContains(IVFFlatIndex<Tuple, RID, IntComparator> *index, const IndexedVectorEntry &entry) {
  std::vector<RID> scan_result;
  index->ScanKey(entry.key_, &scan_result, nullptr);
  ASSERT_EQ(scan_result.size(), 1U) << "expected exact key lookup to find one RID for id=" << entry.id_;
  EXPECT_EQ(scan_result[0], entry.rid_);

  std::vector<RID> knn_result;
  index->SearchKnn(entry.key_, 1, &knn_result, nullptr);
  ASSERT_EQ(knn_result.size(), 1U) << "expected exact KNN lookup to find one RID for id=" << entry.id_;
  EXPECT_EQ(knn_result[0], entry.rid_);
}

}  // namespace

TEST(IVFFlatIndexTest, BuildIndexFromExistingTuples) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE ivf_build(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub,
                   "INSERT INTO ivf_build VALUES "
                   "(ARRAY [1.0, 0.0, 0.0], 1), "
                   "(ARRAY [10.0, 0.0, 0.0], 2), "
                   "(ARRAY [20.0, 0.0, 0.0], 3)");
  ExecuteStatement(*bustub, "CREATE INDEX ivf_build_idx ON ivf_build USING ivfflat (v)");

  auto *index = GetIVFFlatIndex(*bustub, "ivf_build", "ivf_build_idx");
  ASSERT_NE(index, nullptr);

  auto table_info = bustub->catalog_->GetTable("ivf_build");
  ASSERT_NE(table_info, nullptr);
  const auto entries = CollectIndexedEntries(table_info.get(), index);
  ASSERT_EQ(entries.size(), 3U);
  for (const auto &entry : entries) {
    AssertIndexContains(index, entry);
  }
}

TEST(IVFFlatIndexTest, AppliesOptionsAndMultiProbeSearch) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE ivf_probe(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub,
                   "INSERT INTO ivf_probe VALUES "
                   "(ARRAY [0.0, 0.0, 0.0], 1), "
                   "(ARRAY [40.0, 0.0, 0.0], 2), "
                   "(ARRAY [100.0, 0.0, 0.0], 3)");
  ExecuteStatement(*bustub, "CREATE INDEX ivf_probe_1 ON ivf_probe USING ivfflat (v) WITH (nlist = 2, nprobe = 1)");
  ExecuteStatement(*bustub, "CREATE INDEX ivf_probe_2 ON ivf_probe USING ivfflat (v) WITH (nlist = 2, nprobe = 2)");

  auto *probe1 = GetIVFFlatIndex(*bustub, "ivf_probe", "ivf_probe_1");
  auto *probe2 = GetIVFFlatIndex(*bustub, "ivf_probe", "ivf_probe_2");
  ASSERT_NE(probe1, nullptr);
  ASSERT_NE(probe2, nullptr);
  EXPECT_EQ(probe1->GetNList(), 2U);
  EXPECT_EQ(probe1->GetNProbe(), 1U);
  EXPECT_EQ(probe2->GetNProbe(), 2U);

  auto table_info = bustub->catalog_->GetTable("ivf_probe");
  ASSERT_NE(table_info, nullptr);
  const auto entries = CollectIndexedEntries(table_info.get(), probe1);

  const auto query = MakeQueryTuple({30.0, 0.0, 0.0}, probe1);
  std::vector<RID> probe1_result;
  std::vector<RID> probe2_result;
  probe1->SearchKnn(query, 1, &probe1_result, nullptr);
  probe2->SearchKnn(query, 1, &probe2_result, nullptr);

  ASSERT_EQ(probe1_result.size(), 1U);
  ASSERT_EQ(probe2_result.size(), 1U);
  EXPECT_EQ(probe1_result[0], FindRidById(entries, 1));
  EXPECT_EQ(probe2_result[0], FindRidById(entries, 2));
}

TEST(IVFFlatIndexTest, HonorsMetricOption) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE ivf_metric(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub,
                   "INSERT INTO ivf_metric VALUES "
                   "(ARRAY [2.0, 2.0, 0.0], 1), "
                   "(ARRAY [10.0, 0.0, 0.0], 2), "
                   "(ARRAY [0.0, 10.0, 0.0], 3)");
  ExecuteStatement(*bustub, "CREATE INDEX ivf_metric_l2 ON ivf_metric USING ivfflat (v) WITH (metric = 'l2')");
  ExecuteStatement(*bustub, "CREATE INDEX ivf_metric_ip ON ivf_metric USING ivfflat (v) WITH (metric = 'ip')");

  auto *l2_index = GetIVFFlatIndex(*bustub, "ivf_metric", "ivf_metric_l2");
  auto *ip_index = GetIVFFlatIndex(*bustub, "ivf_metric", "ivf_metric_ip");
  ASSERT_NE(l2_index, nullptr);
  ASSERT_NE(ip_index, nullptr);
  EXPECT_EQ(l2_index->GetVectorDistanceMetric(), VectorIndexDistanceMetric::L2);
  EXPECT_EQ(ip_index->GetVectorDistanceMetric(), VectorIndexDistanceMetric::InnerProduct);

  auto table_info = bustub->catalog_->GetTable("ivf_metric");
  ASSERT_NE(table_info, nullptr);
  const auto entries = CollectIndexedEntries(table_info.get(), l2_index);

  const auto query = MakeQueryTuple({2.0, 1.0, 0.0}, l2_index);
  std::vector<RID> l2_result;
  std::vector<RID> ip_result;
  l2_index->SearchKnn(query, 1, &l2_result, nullptr);
  ip_index->SearchKnn(query, 1, &ip_result, nullptr);

  ASSERT_EQ(l2_result.size(), 1U);
  ASSERT_EQ(ip_result.size(), 1U);
  EXPECT_EQ(l2_result[0], FindRidById(entries, 1));
  EXPECT_EQ(ip_result[0], FindRidById(entries, 2));
}

TEST(IVFFlatIndexTest, InsertAfterIndexCreation) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE ivf_insert(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub, "CREATE INDEX ivf_insert_idx ON ivf_insert USING ivfflat (v)");
  ExecuteStatement(*bustub,
                   "INSERT INTO ivf_insert VALUES "
                   "(ARRAY [2.0, 0.0, 0.0], 1), "
                   "(ARRAY [12.0, 0.0, 0.0], 2), "
                   "(ARRAY [22.0, 0.0, 0.0], 3)");

  auto *index = GetIVFFlatIndex(*bustub, "ivf_insert", "ivf_insert_idx");
  ASSERT_NE(index, nullptr);

  auto table_info = bustub->catalog_->GetTable("ivf_insert");
  ASSERT_NE(table_info, nullptr);
  const auto entries = CollectIndexedEntries(table_info.get(), index);
  ASSERT_EQ(entries.size(), 3U);
  for (const auto &entry : entries) {
    AssertIndexContains(index, entry);
  }
}

TEST(IVFFlatIndexTest, ExactKnnRewriteMatchesStarterPlanAndEmptyTable) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE exact_knn(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub,
                   "INSERT INTO exact_knn VALUES "
                   "(ARRAY [1.0, 0.0, 0.0], 1), "
                   "(ARRAY [2.0, 0.0, 0.0], 2), "
                   "(ARRAY [3.0, 0.0, 0.0], 3)");

  const auto sql = "SELECT id FROM exact_knn ORDER BY l2_distance(v, ARRAY [1.0, 0.0, 0.0]) LIMIT 2";
  const auto optimized_plan = ExplainPlan(*bustub, sql);
  EXPECT_TRUE(StringUtil::Contains(optimized_plan, "VectorKnnScan")) << optimized_plan;

  const auto exact_rows = QueryRows(*bustub, sql);

  ExecuteStatement(*bustub, "SET force_optimizer_starter_rule=yes");
  const auto starter_plan = ExplainPlan(*bustub, sql);
  EXPECT_FALSE(StringUtil::Contains(starter_plan, "VectorKnnScan")) << starter_plan;
  EXPECT_TRUE(StringUtil::Contains(starter_plan, "ExternalMergeSort")) << starter_plan;
  const auto starter_rows = QueryRows(*bustub, sql);
  EXPECT_EQ(exact_rows, starter_rows);

  ExecuteStatement(*bustub, "SET force_optimizer_starter_rule=no");
  ExecuteStatement(*bustub, "CREATE TABLE exact_knn_empty(v VECTOR(3), id INTEGER)");
  const auto empty_sql = "SELECT id FROM exact_knn_empty ORDER BY l2_distance(v, ARRAY [1.0, 0.0, 0.0]) LIMIT 3";
  const auto empty_plan = ExplainPlan(*bustub, empty_sql);
  EXPECT_TRUE(StringUtil::Contains(empty_plan, "VectorKnnScan")) << empty_plan;
  EXPECT_TRUE(QueryRows(*bustub, empty_sql).empty());
}

TEST(IVFFlatIndexTest, ExactKnnKeepsIndexPriorityAndDimensionMismatchError) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE knn_priority(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub,
                   "INSERT INTO knn_priority VALUES "
                   "(ARRAY [1.0, 0.0, 0.0], 1), "
                   "(ARRAY [1.0, 1.0, 0.0], 2), "
                   "(ARRAY [0.0, 1.0, 0.0], 3)");
  ExecuteStatement(*bustub, "CREATE INDEX knn_priority_l2 ON knn_priority USING ivfflat (v)");

  const auto l2_sql = "SELECT id FROM knn_priority ORDER BY l2_distance(v, ARRAY [1.0, 0.0, 0.0]) LIMIT 2";
  const auto l2_plan = ExplainPlan(*bustub, l2_sql);
  EXPECT_TRUE(StringUtil::Contains(l2_plan, "VectorIndexScan")) << l2_plan;
  EXPECT_FALSE(StringUtil::Contains(l2_plan, "VectorKnnScan")) << l2_plan;

  const auto cosine_sql = "SELECT id FROM knn_priority ORDER BY cosine_distance(v, ARRAY [1.0, 0.0, 0.0]) LIMIT 2";
  const auto cosine_plan = ExplainPlan(*bustub, cosine_sql);
  EXPECT_TRUE(StringUtil::Contains(cosine_plan, "VectorKnnScan")) << cosine_plan;
  EXPECT_FALSE(StringUtil::Contains(cosine_plan, "VectorIndexScan")) << cosine_plan;

  NoopWriter writer;
  EXPECT_THROW(
      bustub->ExecuteSql("SELECT id FROM knn_priority ORDER BY cosine_distance(v, ARRAY [1.0, 0.0]) LIMIT 1", writer),
      Exception);
}

TEST(IVFFlatIndexTest, ExactKnnRespectsMvccVisibility) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE mvcc_knn(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub,
                   "INSERT INTO mvcc_knn VALUES "
                   "(ARRAY [5.0, 0.0, 0.0], 5), "
                   "(ARRAY [10.0, 0.0, 0.0], 10)");

  const auto sql = "SELECT id FROM mvcc_knn ORDER BY l2_distance(v, ARRAY [0.0, 0.0, 0.0]) LIMIT 2";
  const auto plan = ExplainPlan(*bustub, sql);
  EXPECT_TRUE(StringUtil::Contains(plan, "VectorKnnScan")) << plan;

  auto *reader_txn = BeginTxn(*bustub, "reader_txn");
  auto *writer_txn = BeginTxn(*bustub, "writer_txn");
  WithTxn(writer_txn, ExecuteTxn(*bustub, _var, _txn, "INSERT INTO mvcc_knn VALUES (ARRAY [1.0, 0.0, 0.0], 1)"));
  WithTxn(writer_txn, CommitTxn(*bustub, _var, _txn));

  EXPECT_EQ((QueryRowsTxn(*bustub, reader_txn, sql)), (std::vector<std::vector<std::string>>{{"5"}, {"10"}}));

  auto *fresh_txn = BeginTxn(*bustub, "fresh_txn");
  EXPECT_EQ((QueryRowsTxn(*bustub, fresh_txn, sql)), (std::vector<std::vector<std::string>>{{"1"}, {"5"}}));

  WithTxn(reader_txn, CommitTxn(*bustub, _var, _txn));
  WithTxn(fresh_txn, CommitTxn(*bustub, _var, _txn));
}

TEST(IVFFlatIndexTest, VectorIndexScanFilterBackfillsCandidates) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE filter_knn(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub, "CREATE INDEX filter_knn_idx ON filter_knn USING ivfflat (v) WITH (nlist = 2, nprobe = 1)");
  ExecuteStatement(*bustub,
                   "INSERT INTO filter_knn VALUES "
                   "(ARRAY [0.0, 0.0, 0.0], 1), "
                   "(ARRAY [40.0, 0.0, 0.0], 2), "
                   "(ARRAY [100.0, 0.0, 0.0], 3)");

  const auto sql =
      "SELECT id FROM filter_knn WHERE id <> 1 "
      "ORDER BY l2_distance(v, ARRAY [30.0, 0.0, 0.0]) LIMIT 2";
  const auto plan = ExplainPlan(*bustub, sql);
  EXPECT_TRUE(StringUtil::Contains(plan, "VectorIndexScan")) << plan;
  EXPECT_TRUE(StringUtil::Contains(plan, "filter=(#0.1!=1)")) << plan;

  const auto optimized_rows = QueryRows(*bustub, sql);
  EXPECT_EQ(optimized_rows, (std::vector<std::vector<std::string>>{{"2"}, {"3"}}));

  ExecuteStatement(*bustub, "SET force_optimizer_starter_rule=yes");
  const auto starter_rows = QueryRows(*bustub, sql);
  EXPECT_EQ(optimized_rows, starter_rows);
}

TEST(IVFFlatIndexTest, VectorIndexScanFilterCanReturnEmpty) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE filter_knn_empty(v VECTOR(3), id INTEGER)");
  ExecuteStatement(
      *bustub, "CREATE INDEX filter_knn_empty_idx ON filter_knn_empty USING ivfflat (v) WITH (nlist = 2, nprobe = 1)");
  ExecuteStatement(*bustub,
                   "INSERT INTO filter_knn_empty VALUES "
                   "(ARRAY [0.0, 0.0, 0.0], 1), "
                   "(ARRAY [40.0, 0.0, 0.0], 2)");

  const auto sql =
      "SELECT id FROM filter_knn_empty WHERE id < 0 "
      "ORDER BY l2_distance(v, ARRAY [30.0, 0.0, 0.0]) LIMIT 2";
  const auto plan = ExplainPlan(*bustub, sql);
  EXPECT_TRUE(StringUtil::Contains(plan, "VectorIndexScan")) << plan;
  EXPECT_TRUE(QueryRows(*bustub, sql).empty());
}

TEST(IVFFlatIndexTest, ValidatesVectorDimensionsOnWritePaths) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE dim_guard(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub, "INSERT INTO dim_guard VALUES (ARRAY [1.0, 0.0, 0.0], 1)");
  ExecuteStatement(*bustub, "CREATE TABLE dim_src(v VECTOR(2), id INTEGER)");
  ExecuteStatement(*bustub, "INSERT INTO dim_src VALUES (ARRAY [9.0, 9.0], 9)");

  ExecuteStatementExpectFailure(*bustub, "INSERT INTO dim_guard VALUES (ARRAY [1.0, 2.0], 2)");
  ExecuteStatementExpectFailure(*bustub, "UPDATE dim_guard SET v = ARRAY [1.0, 2.0] WHERE id = 1");
  ExecuteStatementExpectFailure(*bustub, "INSERT INTO dim_guard SELECT v, id FROM dim_src");
  ExecuteStatementExpectFailure(*bustub, "INSERT INTO dim_guard VALUES (NULL, 3)");
}

TEST(IVFFlatIndexTest, DeleteReturnsCorrectRowsAndSurfacesStaleCandidates) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE stale_delete(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub,
                   "CREATE INDEX stale_delete_idx ON stale_delete USING ivfflat (v) WITH (nlist = 2, nprobe = 1)");
  ExecuteStatement(*bustub,
                   "INSERT INTO stale_delete VALUES "
                   "(ARRAY [0.0, 0.0, 0.0], 1), "
                   "(ARRAY [20.0, 0.0, 0.0], 2), "
                   "(ARRAY [40.0, 0.0, 0.0], 3)");
  ExecuteStatement(*bustub, "DELETE FROM stale_delete WHERE id = 1");

  auto *index = GetIVFFlatIndex(*bustub, "stale_delete", "stale_delete_idx");
  ASSERT_NE(index, nullptr);
  EXPECT_EQ(index->GetStaleEntryCount(), 1U);
  const auto stale_before = index->GetReturnedStaleCandidateCount();

  const auto sql = "SELECT id FROM stale_delete ORDER BY l2_distance(v, ARRAY [1.0, 0.0, 0.0]) LIMIT 2";
  EXPECT_EQ(QueryRows(*bustub, sql), (std::vector<std::vector<std::string>>{{"2"}, {"3"}}));
  EXPECT_GT(index->GetReturnedStaleCandidateCount(), stale_before);
}

TEST(IVFFlatIndexTest, UpdateVectorMovesSearchHitToNewPosition) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE stale_update(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub,
                   "CREATE INDEX stale_update_idx ON stale_update USING ivfflat (v) WITH (nlist = 2, nprobe = 1)");
  ExecuteStatement(*bustub,
                   "INSERT INTO stale_update VALUES "
                   "(ARRAY [0.0, 0.0, 0.0], 1), "
                   "(ARRAY [5.0, 0.0, 0.0], 2), "
                   "(ARRAY [100.0, 0.0, 0.0], 3)");
  ExecuteStatement(*bustub, "UPDATE stale_update SET v = ARRAY [90.0, 0.0, 0.0] WHERE id = 1");

  auto *index = GetIVFFlatIndex(*bustub, "stale_update", "stale_update_idx");
  ASSERT_NE(index, nullptr);
  const auto stale_before = index->GetReturnedStaleCandidateCount();

  EXPECT_EQ(QueryRows(*bustub, "SELECT id FROM stale_update ORDER BY l2_distance(v, ARRAY [0.0, 0.0, 0.0]) LIMIT 1"),
            (std::vector<std::vector<std::string>>{{"2"}}));
  EXPECT_EQ(QueryRows(*bustub, "SELECT id FROM stale_update ORDER BY l2_distance(v, ARRAY [95.0, 0.0, 0.0]) LIMIT 1"),
            (std::vector<std::vector<std::string>>{{"1"}}));
  EXPECT_GT(index->GetReturnedStaleCandidateCount(), stale_before);
}

TEST(IVFFlatIndexTest, RebuildCleansStaleEntriesWithoutChangingResults) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE rebuild_ivf(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub,
                   "CREATE INDEX rebuild_ivf_idx ON rebuild_ivf USING ivfflat (v) WITH (nlist = 2, nprobe = 1)");
  ExecuteStatement(*bustub,
                   "INSERT INTO rebuild_ivf VALUES "
                   "(ARRAY [0.0, 0.0, 0.0], 1), "
                   "(ARRAY [10.0, 0.0, 0.0], 2), "
                   "(ARRAY [20.0, 0.0, 0.0], 3), "
                   "(ARRAY [30.0, 0.0, 0.0], 4), "
                   "(ARRAY [40.0, 0.0, 0.0], 5), "
                   "(ARRAY [50.0, 0.0, 0.0], 6)");
  ExecuteStatement(*bustub, "DELETE FROM rebuild_ivf WHERE id <= 4");

  auto *index = GetIVFFlatIndex(*bustub, "rebuild_ivf", "rebuild_ivf_idx");
  ASSERT_NE(index, nullptr);
  EXPECT_EQ(index->GetStaleEntryCount(), 4U);
  EXPECT_EQ(index->GetRebuildCount(), 0U);

  const auto sql = "SELECT id FROM rebuild_ivf ORDER BY l2_distance(v, ARRAY [45.0, 0.0, 0.0]) LIMIT 2";
  const auto before_rows = QueryRows(*bustub, sql);
  EXPECT_EQ(before_rows, (std::vector<std::vector<std::string>>{{"5"}, {"6"}}));
  EXPECT_EQ(index->GetRebuildCount(), 1U);
  EXPECT_EQ(index->GetStaleEntryCount(), 0U);

  const auto after_rows = QueryRows(*bustub, sql);
  EXPECT_EQ(before_rows, after_rows);
}

TEST(IVFFlatIndexTest, AnnSearchOptionsMapSearchBudgetToProbeCount) {
  auto bustub = std::make_unique<BusTubInstance>();
  ExecuteStatement(*bustub, "CREATE TABLE ann_budget(v VECTOR(3), id INTEGER)");
  ExecuteStatement(*bustub,
                   "INSERT INTO ann_budget VALUES "
                   "(ARRAY [0.0, 0.0, 0.0], 1), "
                   "(ARRAY [40.0, 0.0, 0.0], 2), "
                   "(ARRAY [100.0, 0.0, 0.0], 3)");
  ExecuteStatement(*bustub, "CREATE INDEX ann_budget_idx ON ann_budget USING ivfflat (v) WITH (nlist = 2, nprobe = 1)");

  auto *index = GetIVFFlatIndex(*bustub, "ann_budget", "ann_budget_idx");
  ASSERT_NE(index, nullptr);
  auto table_info = bustub->catalog_->GetTable("ann_budget");
  ASSERT_NE(table_info, nullptr);
  const auto entries = CollectIndexedEntries(table_info.get(), index);

  const auto query = MakeQueryTuple({30.0, 0.0, 0.0}, index);
  std::vector<VectorIndexCandidate> budget1_result;
  std::vector<VectorIndexCandidate> budget2_result;
  index->SearchVector(query, AnnSearchOptions{1, 1, 1}, &budget1_result, nullptr);
  index->SearchVector(query, AnnSearchOptions{1, 1, 2}, &budget2_result, nullptr);

  ASSERT_EQ(budget1_result.size(), 1U);
  ASSERT_EQ(budget2_result.size(), 1U);
  EXPECT_EQ(budget1_result[0].rid_, FindRidById(entries, 1));
  EXPECT_EQ(budget2_result[0].rid_, FindRidById(entries, 2));
}

}  // namespace bustub
