#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/bustub_instance.h"
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

}  // namespace bustub
