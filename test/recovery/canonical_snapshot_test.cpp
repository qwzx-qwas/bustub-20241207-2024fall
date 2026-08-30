//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// canonical_snapshot_test.cpp
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include <filesystem>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "catalog/catalog_snapshot.h"
#include "common/config.h"
#include "distributed/session_table.h"
#include "gtest/gtest.h"
#include "recovery/canonical_snapshot.h"
#include "recovery/durable_storage.h"
#include "storage/disk/disk_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/extendible_hash_table_index.h"
#include "type/value_factory.h"

namespace bustub {

// M0-T04: canonical db + catalog.bin + session.bin express the committed state without source index pages or undo logs.
TEST(CanonicalSnapshotTest, MaterializesSelfContainedCommittedState) {
  DiskManagerUnlimitedMemory source_disk;
  BufferPoolManager source_bpm(64, &source_disk);
  Catalog source(&source_bpm, nullptr, nullptr);
  const Schema schema({Column("id", TypeId::INTEGER), Column("payload", TypeId::VARCHAR, 32)});
  auto table =
      source.CreateTableWithOid(nullptr, "items", schema, 0, ReplicatedPrimaryKeyDefinition{0, TypeId::INTEGER, 1});
  ASSERT_NE(table, nullptr);
  Tuple old_row({ValueFactory::GetIntegerValue(1), ValueFactory::GetVarcharValue("old")}, &schema);
  Tuple live_row({ValueFactory::GetIntegerValue(2), ValueFactory::GetVarcharValue("live")}, &schema);
  ASSERT_TRUE(table->table_->InsertTuple({7, true}, old_row).has_value());
  ASSERT_TRUE(table->table_->InsertTuple({11, false}, live_row).has_value());
  auto key_schema = Schema::CopySchema(&schema, {0});
  auto primary_index = source.CreateIndex<IntegerKeyType, IntegerValueType, IntegerComparatorType>(
      nullptr, "items_pk", "items", schema, key_schema, {0}, TWO_INTEGER_SIZE, IntegerHashFunctionType{}, true);
  ASSERT_NE(primary_index, nullptr);
  source.AdvanceSchemaEpoch();

  SessionTable sessions;
  const auto encoded_response = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 1, 0, 11});
  // M0/M1 fixture: restore a pre-existing persisted record without exercising
  // the M2 exact-once transition API.
  sessions.RestoreRecords({{99, SessionRecord{1, encoded_response}}});

  PosixDurableStorage storage;
  const auto root = std::filesystem::temp_directory_path() / ("bustub-canonical-snapshot-" + std::to_string(getpid()));
  storage.RemoveTree(root);
  storage.CreateDirectories(root);
  const CanonicalSnapshotPaths paths{root / "db.bustub", root / "catalog.bin", root / "session.bin"};
  const auto result = CanonicalSnapshotBuilder::Build(source, sessions, paths, &storage, 16);
  EXPECT_EQ(result.row_count_, 1);

  const auto catalog_bytes = storage.ReadFile(paths.catalog_file_, CatalogSnapshotCodec::MAX_CATALOG_BYTES);
  const auto catalog_snapshot = CatalogSnapshotCodec::Decode(catalog_bytes);
  ASSERT_EQ(catalog_snapshot.indexes_.size(), 1);

  DiskManager restored_disk(paths.database_file_);
  BufferPoolManager restored_bpm(32, &restored_disk);
  Catalog restored(&restored_bpm, nullptr, nullptr);
  CatalogSnapshotCodec::Restore(catalog_snapshot, &restored, &restored_bpm, nullptr);
  auto restored_table = restored.GetTable("items");
  ASSERT_NE(restored_table, nullptr);
  size_t row_count = 0;
  for (auto iterator = restored_table->table_->MakeIterator(); !iterator.IsEnd(); ++iterator) {
    auto [meta, tuple] = iterator.GetTuple();
    EXPECT_FALSE(meta.is_deleted_);
    EXPECT_EQ(meta.ts_, 11);
    EXPECT_EQ(tuple.GetValue(&schema, 0).GetAs<int32_t>(), 2);
    row_count++;
  }
  EXPECT_EQ(row_count, 1);

  SessionTable restored_sessions;
  SessionSnapshotCodec::DecodeInto(storage.ReadFile(paths.session_file_, 1024 * 1024), &restored_sessions);
  ASSERT_TRUE(restored_sessions.GetLastResponse(99).has_value());
  EXPECT_EQ(*restored_sessions.GetLastResponse(99), encoded_response);
  restored_disk.ShutDown();
  storage.RemoveTree(root);
}

TEST(CanonicalSnapshotTest, RejectsSpeculativeLiveRowsAndTombstonesBeforePublication) {
  for (const bool deleted : {false, true}) {
    SCOPED_TRACE(deleted ? "speculative tombstone" : "speculative live row");
    DiskManagerUnlimitedMemory source_disk;
    BufferPoolManager source_bpm(32, &source_disk);
    Catalog source(&source_bpm, nullptr, nullptr);
    const Schema schema({Column("id", TypeId::INTEGER), Column("payload", TypeId::VARCHAR, 16)});
    auto table =
        source.CreateTableWithOid(nullptr, "items", schema, 0, ReplicatedPrimaryKeyDefinition{0, TypeId::INTEGER, 1});
    ASSERT_NE(table, nullptr);
    const auto key_schema = Schema::CopySchema(&schema, {0});
    const auto primary_index = source.CreateIndex<IntegerKeyType, IntegerValueType, IntegerComparatorType>(
        nullptr, "items_pk", "items", schema, key_schema, {0}, TWO_INTEGER_SIZE, IntegerHashFunctionType{}, true);
    ASSERT_NE(primary_index, nullptr);
    ASSERT_TRUE(
        table->table_
            ->InsertTuple({static_cast<timestamp_t>(TXN_START_ID + 7), deleted},
                          Tuple({ValueFactory::GetIntegerValue(7), ValueFactory::GetVarcharValue("pending")}, &schema))
            .has_value());

    PosixDurableStorage storage;
    const auto root = std::filesystem::temp_directory_path() /
                      ("bustub-canonical-speculative-" + std::to_string(getpid()) + (deleted ? "-delete" : "-live"));
    storage.RemoveTree(root);
    const CanonicalSnapshotPaths paths{root / "db.bustub", root / "catalog.bin", root / "session.bin"};
    SessionTable sessions;
    EXPECT_THROW(CanonicalSnapshotBuilder::Build(source, sessions, paths, &storage, 16), std::runtime_error);
    EXPECT_FALSE(storage.Exists(paths.catalog_file_));
    EXPECT_FALSE(storage.Exists(paths.session_file_));
    storage.RemoveTree(root);
  }
}

}  // namespace bustub
