//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_snapshot_test.cpp
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog_snapshot.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/extendible_hash_table_index.h"
#include "type/value_factory.h"

namespace bustub {
namespace {

auto Hex(std::string_view text) -> std::vector<std::byte> {
  if (text.size() % 2 != 0) {
    throw std::runtime_error("invalid test hex literal");
  }
  const auto nibble = [](char value) -> uint8_t {
    if (value >= '0' && value <= '9') {
      return static_cast<uint8_t>(value - '0');
    }
    if (value >= 'a' && value <= 'f') {
      return static_cast<uint8_t>(value - 'a' + 10);
    }
    throw std::runtime_error("invalid test hex digit");
  };
  std::vector<std::byte> result;
  result.reserve(text.size() / 2);
  for (size_t offset = 0; offset < text.size(); offset += 2) {
    result.push_back(static_cast<std::byte>((nibble(text[offset]) << 4U) | nibble(text[offset + 1])));
  }
  return result;
}

}  // namespace

TEST(CatalogSnapshotTest, MinimalReplicatedCatalogMatchesFixedV1GoldenFrame) {
  CatalogSnapshot source;
  source.schema_epoch_ = 9;
  source.next_table_oid_ = 8;
  source.next_index_oid_ = 12;
  const Schema schema({Column("account_id", TypeId::BIGINT), Column("memo", TypeId::VARCHAR, 32)});
  source.tables_.push_back({7, "ledger", schema, 42, ReplicatedPrimaryKeyDefinition{0, TypeId::BIGINT, 1}});
  source.indexes_.push_back({11, 7, "ledger_pk", {0}, IndexType::BPlusTreeIndex, IndexConstraintKind::PRIMARY_KEY});

  // Hand-assembled from the V1 field table. The 134-byte payload is followed by the independently calculated
  // CRC-32C 0x9663e5da; constructing this expected frame does not call a production codec or checksum helper.
  const auto golden =
      Hex("425354434154303100000001000000860000000000000009000000080000000c"
          "000000010000000100000007000000066c65646765720000002a010000000000"
          "00000500000001000000020000000a6163636f756e745f696400000005000000"
          "08000000046d656d6f00000007000000200000000b00000007000000096c6564"
          "6765725f706b000000000000000100000001000000009663e5da");
  ASSERT_EQ(golden.size(), 154);
  EXPECT_EQ(CatalogSnapshotCodec::Encode(source), golden);

  const auto decoded = CatalogSnapshotCodec::Decode(golden);
  EXPECT_EQ(decoded.format_version_, 1);
  EXPECT_EQ(decoded.schema_epoch_, 9);
  EXPECT_EQ(decoded.next_table_oid_, 8);
  EXPECT_EQ(decoded.next_index_oid_, 12);
  ASSERT_EQ(decoded.tables_.size(), 1);
  const auto &table = decoded.tables_[0];
  EXPECT_EQ(table.table_oid_, 7);
  EXPECT_EQ(table.table_name_, "ledger");
  EXPECT_EQ(table.first_page_id_, 42);
  ASSERT_TRUE(table.replicated_primary_key_.has_value());
  EXPECT_EQ(table.replicated_primary_key_->column_oid_, 0);
  EXPECT_EQ(table.replicated_primary_key_->type_, TypeId::BIGINT);
  EXPECT_EQ(table.replicated_primary_key_->codec_version_, 1);
  ASSERT_EQ(table.schema_.GetColumnCount(), 2);
  EXPECT_EQ(table.schema_.GetColumn(0).GetName(), "account_id");
  EXPECT_EQ(table.schema_.GetColumn(0).GetType(), TypeId::BIGINT);
  EXPECT_EQ(table.schema_.GetColumn(0).GetStorageSize(), 8);
  EXPECT_EQ(table.schema_.GetColumn(1).GetName(), "memo");
  EXPECT_EQ(table.schema_.GetColumn(1).GetType(), TypeId::VARCHAR);
  EXPECT_EQ(table.schema_.GetColumn(1).GetStorageSize(), 32);

  ASSERT_EQ(decoded.indexes_.size(), 1);
  const auto &index = decoded.indexes_[0];
  EXPECT_EQ(index.index_oid_, 11);
  EXPECT_EQ(index.table_oid_, 7);
  EXPECT_EQ(index.index_name_, "ledger_pk");
  EXPECT_EQ(index.key_attrs_, std::vector<uint32_t>({0}));
  EXPECT_EQ(index.index_type_, IndexType::BPlusTreeIndex);
  EXPECT_EQ(index.constraint_kind_, IndexConstraintKind::PRIMARY_KEY);
  EXPECT_NO_THROW(ValidateReplicatedCatalogV1(decoded));

  auto unsupported_version = source;
  unsupported_version.format_version_ = 2;
  EXPECT_THROW(CatalogSnapshotCodec::Encode(unsupported_version), std::runtime_error);
  auto unsupported_frame_version = golden;
  unsupported_frame_version[11] = std::byte{2};
  EXPECT_THROW(CatalogSnapshotCodec::Decode(unsupported_frame_version), std::runtime_error);
}

// M0-T02: catalog.bin is self-contained metadata and rebuilt indexes do not depend on physical root page IDs.
TEST(CatalogSnapshotTest, RoundTripAndRebuildDerivedIndex) {
  DiskManagerUnlimitedMemory disk;
  BufferPoolManager bpm(64, &disk);
  Catalog source(&bpm, nullptr, nullptr);
  const Schema schema({Column("id", TypeId::INTEGER), Column("value", TypeId::VARCHAR, 32)});
  const ReplicatedPrimaryKeyDefinition primary_key{0, TypeId::INTEGER, 1};
  auto table = source.CreateTableWithOid(nullptr, "items", schema, 0, primary_key);
  ASSERT_NE(table, nullptr);
  for (int32_t id : {7, 11}) {
    Tuple tuple({ValueFactory::GetIntegerValue(id), ValueFactory::GetVarcharValue("value-" + std::to_string(id))},
                &schema);
    ASSERT_TRUE(table->table_->InsertTuple({static_cast<timestamp_t>(id), false}, tuple).has_value());
  }
  auto key_schema = Schema::CopySchema(&schema, {0});
  auto index = source.CreateIndex<IntegerKeyType, IntegerValueType, IntegerComparatorType>(
      nullptr, "items_pk", "items", schema, key_schema, {0}, TWO_INTEGER_SIZE, IntegerHashFunctionType{}, true);
  ASSERT_NE(index, nullptr);
  source.AdvanceSchemaEpoch();

  const auto bytes = CatalogSnapshotCodec::Encode(CatalogSnapshotCodec::Capture(source));
  const auto decoded = CatalogSnapshotCodec::Decode(bytes);
  ASSERT_EQ(decoded.tables_.size(), 1);
  ASSERT_EQ(decoded.indexes_.size(), 1);
  EXPECT_EQ(decoded.tables_[0].replicated_primary_key_, primary_key);

  Catalog restored(&bpm, nullptr, nullptr);
  CatalogSnapshotCodec::Restore(decoded, &restored, &bpm, nullptr);
  EXPECT_EQ(restored.GetSchemaEpoch(), 1);
  EXPECT_EQ(restored.GetNextTableOid(), 1);
  EXPECT_EQ(restored.GetNextIndexOid(), 1);
  auto restored_table = restored.GetTable("items");
  ASSERT_NE(restored_table, nullptr);
  EXPECT_EQ(restored_table->table_->GetFirstPageId(), table->table_->GetFirstPageId());
  auto restored_index = restored.GetIndex("items_pk", "items");
  ASSERT_NE(restored_index, nullptr);
  EXPECT_EQ(restored_index->key_attrs_, std::vector<uint32_t>({0}));
  EXPECT_EQ(restored_index->constraint_kind_, IndexConstraintKind::PRIMARY_KEY);

  std::vector<RID> hits;
  Tuple key({ValueFactory::GetIntegerValue(11)}, &key_schema);
  restored_index->index_->ScanKey(key, &hits, nullptr);
  ASSERT_EQ(hits.size(), 1);
  auto [meta, tuple] = restored_table->table_->GetTuple(hits[0]);
  EXPECT_EQ(meta.ts_, 11);
  EXPECT_EQ(tuple.GetValue(&schema, 1).ToString(), "value-11");
}

// M0-T03: corruption and unsupported secondary UNIQUE definitions fail closed.
TEST(CatalogSnapshotTest, RejectsCorruptionAndDeferredUniqueConstraint) {
  DiskManagerUnlimitedMemory disk;
  BufferPoolManager bpm(16, &disk);
  Catalog catalog(&bpm, nullptr, nullptr);
  const Schema schema({Column("id", TypeId::INTEGER)});
  ASSERT_NE(catalog.CreateTableWithOid(nullptr, "t", schema, 0, ReplicatedPrimaryKeyDefinition{0, TypeId::INTEGER, 1}),
            nullptr);

  auto bytes = CatalogSnapshotCodec::Encode(CatalogSnapshotCodec::Capture(catalog));
  bytes[bytes.size() - 1] ^= std::byte{1};
  EXPECT_THROW(CatalogSnapshotCodec::Decode(bytes), std::runtime_error);

  auto snapshot = CatalogSnapshotCodec::Capture(catalog);
  snapshot.indexes_.push_back(
      {0, 0, "bad_unique", {0}, IndexType::BPlusTreeIndex, IndexConstraintKind::SECONDARY_UNIQUE});
  snapshot.next_index_oid_ = 1;
  EXPECT_THROW(CatalogSnapshotCodec::Encode(snapshot), std::runtime_error);

  auto unknown_type = snapshot;
  unknown_type.indexes_[0].constraint_kind_ = IndexConstraintKind::NON_UNIQUE_SECONDARY;
  unknown_type.indexes_[0].index_type_ = static_cast<IndexType>(999);
  EXPECT_THROW(CatalogSnapshotCodec::Encode(unknown_type), std::runtime_error);

  auto unknown_constraint = snapshot;
  unknown_constraint.indexes_[0].constraint_kind_ = static_cast<IndexConstraintKind>(999);
  EXPECT_THROW(CatalogSnapshotCodec::Encode(unknown_constraint), std::runtime_error);

  auto invalid_table_allocator = CatalogSnapshotCodec::Capture(catalog);
  invalid_table_allocator.next_table_oid_ = 0;
  EXPECT_THROW(CatalogSnapshotCodec::Encode(invalid_table_allocator), std::runtime_error);

  auto invalid_index_allocator = snapshot;
  invalid_index_allocator.indexes_[0].constraint_kind_ = IndexConstraintKind::NON_UNIQUE_SECONDARY;
  invalid_index_allocator.next_index_oid_ = 0;
  EXPECT_THROW(CatalogSnapshotCodec::Encode(invalid_index_allocator), std::runtime_error);
}

TEST(CatalogSnapshotTest, RestoreRejectsDuplicatePrimaryKeyButAllowsDuplicateSecondaryKey) {
  const Schema schema({Column("id", TypeId::INTEGER), Column("value", TypeId::INTEGER)});
  const auto primary_key = ReplicatedPrimaryKeyDefinition{0, TypeId::INTEGER, 1};

  {
    DiskManagerUnlimitedMemory disk;
    BufferPoolManager bpm(32, &disk);
    TableHeap heap(&bpm);
    const auto first_page_id = heap.GetFirstPageId();
    ASSERT_TRUE(heap.InsertTuple({1, false},
                                 Tuple({ValueFactory::GetIntegerValue(7), ValueFactory::GetIntegerValue(100)}, &schema))
                    .has_value());
    ASSERT_TRUE(heap.InsertTuple({2, false},
                                 Tuple({ValueFactory::GetIntegerValue(7), ValueFactory::GetIntegerValue(200)}, &schema))
                    .has_value());
    CatalogSnapshot duplicate_primary;
    duplicate_primary.schema_epoch_ = 1;
    duplicate_primary.next_table_oid_ = 1;
    duplicate_primary.next_index_oid_ = 1;
    duplicate_primary.tables_.push_back({0, "items", schema, first_page_id, primary_key});
    duplicate_primary.indexes_.push_back(
        {0, 0, "items_pk", {0}, IndexType::BPlusTreeIndex, IndexConstraintKind::PRIMARY_KEY});
    Catalog restored(&bpm, nullptr, nullptr);
    EXPECT_THROW(CatalogSnapshotCodec::Restore(duplicate_primary, &restored, &bpm, nullptr), std::runtime_error);
  }

  {
    DiskManagerUnlimitedMemory disk;
    BufferPoolManager bpm(32, &disk);
    TableHeap heap(&bpm);
    const auto first_page_id = heap.GetFirstPageId();
    ASSERT_TRUE(heap.InsertTuple({3, false},
                                 Tuple({ValueFactory::GetIntegerValue(1), ValueFactory::GetIntegerValue(500)}, &schema))
                    .has_value());
    ASSERT_TRUE(heap.InsertTuple({4, false},
                                 Tuple({ValueFactory::GetIntegerValue(2), ValueFactory::GetIntegerValue(500)}, &schema))
                    .has_value());
    CatalogSnapshot duplicate_secondary;
    duplicate_secondary.schema_epoch_ = 2;
    duplicate_secondary.next_table_oid_ = 1;
    duplicate_secondary.next_index_oid_ = 2;
    duplicate_secondary.tables_.push_back({0, "items", schema, first_page_id, primary_key});
    duplicate_secondary.indexes_.push_back(
        {0, 0, "items_pk", {0}, IndexType::BPlusTreeIndex, IndexConstraintKind::PRIMARY_KEY});
    duplicate_secondary.indexes_.push_back(
        {1, 0, "items_value", {1}, IndexType::BPlusTreeIndex, IndexConstraintKind::NON_UNIQUE_SECONDARY});
    Catalog restored(&bpm, nullptr, nullptr);
    ASSERT_NO_THROW(CatalogSnapshotCodec::Restore(duplicate_secondary, &restored, &bpm, nullptr));
    auto secondary = restored.GetIndex("items_value", "items");
    ASSERT_NE(secondary, nullptr);
    std::vector<RID> hits;
    const auto secondary_schema = Schema::CopySchema(&schema, {1});
    secondary->index_->ScanKey(Tuple({ValueFactory::GetIntegerValue(500)}, &secondary_schema), &hits, nullptr);
    EXPECT_EQ(hits.size(), 2);
    EXPECT_EQ(restored.GetNextTableOid(), 1);
    EXPECT_EQ(restored.GetNextIndexOid(), 2);
    EXPECT_EQ(restored.GetSchemaEpoch(), 2);
  }
}

// M0-T05: replicated-V1 restore rejects ambiguous logical identity before any state is opened to clients.
TEST(CatalogSnapshotTest, ReplicatedAdmissionRequiresExactlyOneMatchingPrimaryIndex) {
  const Schema schema({Column("id", TypeId::INTEGER)});
  CatalogSnapshot missing_primary;
  missing_primary.next_table_oid_ = 1;
  missing_primary.tables_.push_back({0, "t", schema, 0, std::nullopt});
  EXPECT_THROW(ValidateReplicatedCatalogV1(missing_primary), std::runtime_error);

  CatalogSnapshot missing_index;
  missing_index.next_table_oid_ = 1;
  missing_index.tables_.push_back({0, "t", schema, 0, ReplicatedPrimaryKeyDefinition{0, TypeId::INTEGER, 1}});
  EXPECT_THROW(ValidateReplicatedCatalogV1(missing_index), std::runtime_error);

  missing_index.indexes_.push_back({0, 0, "t_pk", {0}, IndexType::BPlusTreeIndex, IndexConstraintKind::PRIMARY_KEY});
  missing_index.next_index_oid_ = 1;
  EXPECT_NO_THROW(ValidateReplicatedCatalogV1(missing_index));

  auto mismatched = missing_index;
  mismatched.indexes_[0].key_attrs_ = {1};
  EXPECT_THROW(ValidateReplicatedCatalogV1(mismatched), std::runtime_error);

  const Schema wide_schema({Column("id", TypeId::INTEGER), Column("payload", TypeId::VARCHAR, 200)});
  CatalogSnapshot oversized;
  oversized.next_table_oid_ = 1;
  oversized.next_index_oid_ = 2;
  oversized.tables_.push_back({0, "wide", wide_schema, 0, ReplicatedPrimaryKeyDefinition{0, TypeId::INTEGER, 1}});
  oversized.indexes_.push_back({0, 0, "wide_pk", {0}, IndexType::BPlusTreeIndex, IndexConstraintKind::PRIMARY_KEY});
  oversized.indexes_.push_back(
      {1, 0, "too_wide", {1}, IndexType::BPlusTreeIndex, IndexConstraintKind::NON_UNIQUE_SECONDARY});
  EXPECT_THROW(ValidateReplicatedCatalogV1(oversized), std::runtime_error);
}

}  // namespace bustub
