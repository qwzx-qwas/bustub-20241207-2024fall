//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// snapshot_manager_test.cpp
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include <chrono>  // NOLINT(build/c++11)
#include <filesystem>
#include <future>  // NOLINT(build/c++11)
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "common/state_visibility.h"
#include "distributed/session_table.h"
#include "gtest/gtest.h"
#include "power_loss_storage.h"  // NOLINT(build/include_subdir)
#include "recovery/node_directory.h"
#include "recovery/snapshot_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/extendible_hash_table_index.h"
#include "type/value_factory.h"

namespace bustub {
namespace {

class RecordingPosixStorage final : public PosixDurableStorage {
 public:
  void SyncDirectory(const std::filesystem::path &path) override {
    synced_directories_.push_back(std::filesystem::absolute(path).lexically_normal());
    PosixDurableStorage::SyncDirectory(path);
  }

  std::vector<std::filesystem::path> synced_directories_;
};

auto Bytes(std::string_view value) -> std::vector<std::byte> {
  const auto *begin = reinterpret_cast<const std::byte *>(value.data());
  return {begin, begin + value.size()};
}

}  // namespace

TEST(NodeDirectoryTest, FirstCreationSynchronizesEveryNewAncestorEntry) {
  auto storage = std::make_shared<RecordingPosixStorage>();
  const auto anchor =
      std::filesystem::temp_directory_path() / ("bustub-node-directory-durable-" + std::to_string(getpid()));
  const auto root = anchor / "level-one" / "level-two" / "node";
  storage->RemoveTree(anchor);

  auto node_directory = NodeDirectory::Open(root, storage);
  ASSERT_GE(storage->synced_directories_.size(), 5);
  EXPECT_EQ(storage->synced_directories_[0], std::filesystem::absolute(root).lexically_normal());
  EXPECT_EQ(storage->synced_directories_[1], std::filesystem::absolute(root.parent_path()).lexically_normal());
  EXPECT_EQ(storage->synced_directories_[2],
            std::filesystem::absolute(root.parent_path().parent_path()).lexically_normal());
  EXPECT_EQ(storage->synced_directories_[3], std::filesystem::absolute(anchor).lexically_normal());
  EXPECT_EQ(storage->synced_directories_[4], std::filesystem::absolute(anchor.parent_path()).lexically_normal());

  node_directory.reset();
  storage->RemoveTree(anchor);
}

// M2-IT02 (cumulatively rechecking M1): runtime-directed pruning keeps two bridged generations and recovery rebuilds
// a usable database plus exact-once Session state.
TEST(SnapshotManagerTest, PublishPruneAndRecover) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto root = std::filesystem::temp_directory_path() / ("bustub-snapshot-manager-" + std::to_string(getpid()));
  storage->RemoveTree(root);
  auto node_directory = NodeDirectory::Open(root, storage);
  EXPECT_THROW(NodeDirectory::Open(root, storage), std::system_error);

  DiskManagerUnlimitedMemory source_disk;
  BufferPoolManager source_bpm(64, &source_disk);
  Catalog source(&source_bpm, nullptr, nullptr);
  const Schema schema({Column("id", TypeId::INTEGER), Column("value", TypeId::INTEGER)});
  auto table =
      source.CreateTableWithOid(nullptr, "items", schema, 0, ReplicatedPrimaryKeyDefinition{0, TypeId::INTEGER, 1});
  ASSERT_NE(table, nullptr);
  auto key_schema = Schema::CopySchema(&schema, {0});
  auto primary_index = source.CreateIndex<IntegerKeyType, IntegerValueType, IntegerComparatorType>(
      nullptr, "items_pk", "items", schema, key_schema, {0}, TWO_INTEGER_SIZE, IntegerHashFunctionType{}, true);
  ASSERT_NE(primary_index, nullptr);
  source.AdvanceSchemaEpoch();
  SessionTable sessions;
  StateVisibilityLatch latch;
  SnapshotManager snapshots(node_directory.get(), storage);
  std::vector<std::byte> final_response;

  for (uint64_t generation = 1; generation <= 3; generation++) {
    Tuple tuple({ValueFactory::GetIntegerValue(static_cast<int32_t>(generation)),
                 ValueFactory::GetIntegerValue(static_cast<int32_t>(generation * 10))},
                &schema);
    ASSERT_TRUE(table->table_->InsertTuple({static_cast<timestamp_t>(generation * 10), false}, tuple).has_value());
    final_response = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, generation, 0, generation * 10});
    sessions.RestoreRecords({{700, SessionRecord{generation, final_response}}});
    snapshots.CreateSnapshot(source, sessions, &latch, generation, generation * 10, 0);
  }

  const auto state_directory = node_directory->StateDirectory();
  const auto manifest_one = state_directory / StateManifestStore::ManifestFileName(1);
  auto corrupt_manifest = storage->ReadFile(manifest_one, StateManifestCodec::MAX_MANIFEST_BYTES);
  corrupt_manifest.back() ^= std::byte{1};
  storage->WriteFile(manifest_one, corrupt_manifest);
  storage->SyncFile(manifest_one);
  storage->CreateDirectories(state_directory / "SNAPSHOT-00000000000000000099");
  storage->WriteFile(state_directory / "SNAPSHOT-00000000000000000099/orphan.bin", Bytes("orphan"));
  storage->WriteFile(state_directory / "MANIFEST-00000000000000000099", Bytes("damaged"));
  storage->CreateDirectories(state_directory / "SNAPSHOT-00000000000000000098.tmp");
  storage->WriteFile(state_directory / "SNAPSHOT-00000000000000000098.tmp/partial.bin", Bytes("partial"));
  storage->WriteFile(state_directory / "MANIFEST-00000000000000000098.tmp", Bytes("partial manifest"));
  storage->WriteFile(state_directory / "CURRENT.tmp", Bytes("partial current"));
  storage->WriteFile(state_directory / "operator-note.txt", Bytes("preserve unrelated operator data"));

  uint64_t compacted_through = 0;
  snapshots.PruneToTwo([](uint64_t) { return true; },
                       [&](uint64_t oldest_boundary) { compacted_through = oldest_boundary; });
  EXPECT_EQ(compacted_through, 20);
  EXPECT_FALSE(storage->Exists(node_directory->StateDirectory() / "SNAPSHOT-00000000000000000001"));
  EXPECT_TRUE(storage->Exists(node_directory->StateDirectory() / "SNAPSHOT-00000000000000000002"));
  EXPECT_TRUE(storage->Exists(node_directory->StateDirectory() / "SNAPSHOT-00000000000000000003"));
  EXPECT_FALSE(storage->Exists(manifest_one));
  EXPECT_FALSE(storage->Exists(state_directory / "SNAPSHOT-00000000000000000099"));
  EXPECT_FALSE(storage->Exists(state_directory / "MANIFEST-00000000000000000099"));
  EXPECT_FALSE(storage->Exists(state_directory / "SNAPSHOT-00000000000000000098.tmp"));
  EXPECT_FALSE(storage->Exists(state_directory / "MANIFEST-00000000000000000098.tmp"));
  EXPECT_FALSE(storage->Exists(state_directory / "CURRENT.tmp"));
  EXPECT_TRUE(storage->Exists(state_directory / "operator-note.txt"));

  auto recovered = snapshots.Recover([](uint64_t) { return true; }, 32);
  ASSERT_NE(recovered, nullptr);
  EXPECT_EQ(recovered->manifest_.generation_, 3);
  EXPECT_EQ(recovered->last_applied_, 30);
  EXPECT_EQ(recovered->published_applied_index_, 30);
  auto recovered_table = recovered->catalog_->GetTable("items");
  ASSERT_NE(recovered_table, nullptr);
  size_t count = 0;
  for (auto iterator = recovered_table->table_->MakeIterator(); !iterator.IsEnd(); ++iterator) {
    auto [meta, tuple] = iterator.GetTuple();
    EXPECT_EQ(meta.ts_, static_cast<timestamp_t>((count + 1) * 10));
    EXPECT_FALSE(meta.is_deleted_);
    EXPECT_EQ(tuple.GetValue(&recovered_table->schema_, 0).GetAs<int32_t>(), static_cast<int32_t>(count + 1));
    EXPECT_EQ(tuple.GetValue(&recovered_table->schema_, 1).GetAs<int32_t>(), static_cast<int32_t>((count + 1) * 10));
    count++;
  }
  EXPECT_EQ(count, 3);
  auto recovered_index = recovered->catalog_->GetIndex("items_pk", "items");
  ASSERT_NE(recovered_index, nullptr);
  for (int32_t id = 1; id <= 3; id++) {
    std::vector<RID> hits;
    recovered_index->index_->ScanKey(Tuple({ValueFactory::GetIntegerValue(id)}, &key_schema), &hits, nullptr);
    ASSERT_EQ(hits.size(), 1);
    const auto [meta, tuple] = recovered_table->table_->GetTuple(hits[0]);
    EXPECT_FALSE(meta.is_deleted_);
    EXPECT_EQ(tuple.GetValue(&recovered_table->schema_, 0).GetAs<int32_t>(), id);
    EXPECT_EQ(tuple.GetValue(&recovered_table->schema_, 1).GetAs<int32_t>(), id * 10);
  }
  ASSERT_TRUE(recovered->sessions_->GetLastResponse(700).has_value());
  EXPECT_EQ(*recovered->sessions_->GetLastResponse(700), final_response);
  EXPECT_EQ(recovered->sessions_->Classify(700, 3), RequestDisposition::RETRY_LAST);
  const auto table_tail = recovered_table->table_->GetLastPageId();
  EXPECT_GT(recovered->buffer_pool_manager_->NewPage(), table_tail);

  recovered.reset();
  node_directory.reset();
  storage->RemoveTree(root);
}

// M1-IT02: every named durability event in snapshot/CURRENT publication uses the shared old-or-new recovery oracle.
TEST(SnapshotManagerTest, PublicationPowerLossMatrix) {
  const auto run = [](std::optional<StorageFaultPlan> fault) -> AtomicDurabilityRun<uint64_t> {
    const auto suffix = fault.has_value() ? fault->Name() : "complete";
    const auto root = std::filesystem::temp_directory_path() /
                      ("bustub-snapshot-power-loss-" + std::to_string(getpid()) + "-" + suffix);
    auto storage = std::make_shared<PowerLossStorage>(root);
    storage->RemoveTree(root);
    storage->CreateDirectories(root);
    storage->SyncDirectory(root);
    storage->ResetEventHistory();

    DiskManagerUnlimitedMemory source_disk;
    BufferPoolManager source_bpm(32, &source_disk);
    Catalog source(&source_bpm, nullptr, nullptr);
    const Schema schema({Column("id", TypeId::INTEGER), Column("value", TypeId::INTEGER)});
    auto table =
        source.CreateTableWithOid(nullptr, "t", schema, 0, ReplicatedPrimaryKeyDefinition{0, TypeId::INTEGER, 1});
    if (table == nullptr) {
      throw std::runtime_error("test source table creation failed");
    }
    if (!table->table_
             ->InsertTuple({1, false},
                           Tuple({ValueFactory::GetIntegerValue(1), ValueFactory::GetIntegerValue(100)}, &schema))
             .has_value()) {
      throw std::runtime_error("test source tuple insertion failed");
    }
    const auto key_schema = Schema::CopySchema(&schema, {0});
    if (source.CreateIndex<IntegerKeyType, IntegerValueType, IntegerComparatorType>(
            nullptr, "t_pk", "t", schema, key_schema, {0}, TWO_INTEGER_SIZE, IntegerHashFunctionType{}, true) ==
        nullptr) {
      throw std::runtime_error("test source primary-index creation failed");
    }
    source.AdvanceSchemaEpoch();
    SessionTable sessions;
    const auto old_response = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 1, 0, 1});
    const auto new_response = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 2, 0, 2});
    sessions.RestoreRecords({{91, SessionRecord{1, old_response}}});
    StateVisibilityLatch latch;

    auto node_directory = NodeDirectory::Open(root, storage);
    SnapshotManager snapshots(node_directory.get(), storage);
    snapshots.CreateSnapshot(source, sessions, &latch, 1, 1, 0);
    if (!table->table_
             ->InsertTuple({2, false},
                           Tuple({ValueFactory::GetIntegerValue(2), ValueFactory::GetIntegerValue(900)}, &schema))
             .has_value()) {
      throw std::runtime_error("test source generation-two tuple insertion failed");
    }
    sessions.RestoreRecords({{91, SessionRecord{2, new_response}}});
    storage->ResetEventHistory();
    if (fault.has_value()) {
      storage->FailAt(*fault);
    }
    try {
      snapshots.CreateSnapshot(source, sessions, &latch, 2, 2, 0);
    } catch (const std::runtime_error &) {
      if (!fault.has_value()) {
        throw;
      }
    }
    const auto events = storage->Events();
    const auto fault_triggered = storage->FaultTriggered();
    node_directory.reset();

    storage->DisableFailure();
    storage->PowerLoss();
    node_directory = NodeDirectory::Open(root, storage);
    SnapshotManager recovered_snapshots(node_directory.get(), storage);
    auto recovered = recovered_snapshots.Recover([](uint64_t) { return true; }, 16);
    if (recovered == nullptr) {
      throw std::runtime_error("power-loss image has no complete recovery point");
    }
    const auto generation = recovered->manifest_.generation_;
    if (recovered->manifest_.last_included_index_ != generation) {
      throw std::runtime_error("power-loss recovery mixed snapshot and Manifest generations");
    }
    auto recovered_table = recovered->catalog_->GetTable("t");
    auto recovered_index = recovered->catalog_->GetIndex("t_pk", "t");
    if (recovered_table == nullptr || recovered_index == nullptr) {
      throw std::runtime_error("power-loss recovery lost the literal Catalog objects");
    }
    std::vector<std::pair<int32_t, int32_t>> rows;
    for (auto iterator = recovered_table->table_->MakeIterator(); !iterator.IsEnd(); ++iterator) {
      auto [meta, tuple] = iterator.GetTuple();
      if (!meta.is_deleted_) {
        rows.emplace_back(tuple.GetValue(&recovered_table->schema_, 0).GetAs<int32_t>(),
                          tuple.GetValue(&recovered_table->schema_, 1).GetAs<int32_t>());
      }
    }
    const std::vector<std::pair<int32_t, int32_t>> old_rows{{1, 100}};
    const std::vector<std::pair<int32_t, int32_t>> new_rows{{1, 100}, {2, 900}};
    const auto &expected_rows = generation == 1 ? old_rows : new_rows;
    for (const auto &[id, value] : expected_rows) {
      std::vector<RID> hits;
      recovered_index->index_->ScanKey(Tuple({ValueFactory::GetIntegerValue(id)}, &key_schema), &hits, nullptr);
      if (hits.size() != 1) {
        throw std::runtime_error("power-loss recovery did not rebuild the primary-index entry");
      }
      const auto [meta, tuple] = recovered_table->table_->GetTuple(hits[0]);
      if (meta.is_deleted_ || tuple.GetValue(&recovered_table->schema_, 0).GetAs<int32_t>() != id ||
          tuple.GetValue(&recovered_table->schema_, 1).GetAs<int32_t>() != value) {
        throw std::runtime_error("power-loss recovery primary index resolves to the wrong literal row");
      }
    }
    const auto response = recovered->sessions_->GetLastResponse(91);
    if (!response.has_value() || (generation == 1 && (rows != old_rows || *response != old_response)) ||
        (generation == 2 && (rows != new_rows || *response != new_response)) || (generation != 1 && generation != 2)) {
      throw std::runtime_error("power-loss recovery produced a mixed database/Catalog/Session generation");
    }
    recovered.reset();
    node_directory.reset();
    storage->RemoveTree(root);
    return {generation, events, fault_triggered};
  };

  const StorageEventTopology expected_events{
      {StorageFaultPoint::BEFORE_WRITE, 1, "state/SNAPSHOT-00000000000000000002.tmp/db.log", {}},
      {StorageFaultPoint::BEFORE_WRITE, 2, "state/SNAPSHOT-00000000000000000002.tmp/catalog.bin", {}},
      {StorageFaultPoint::BEFORE_WRITE, 3, "state/SNAPSHOT-00000000000000000002.tmp/session.bin", {}},
      {StorageFaultPoint::AFTER_FSYNC, 1, "state/SNAPSHOT-00000000000000000002.tmp/db.bustub", {}},
      {StorageFaultPoint::AFTER_FSYNC, 2, "state/SNAPSHOT-00000000000000000002.tmp/catalog.bin", {}},
      {StorageFaultPoint::AFTER_FSYNC, 3, "state/SNAPSHOT-00000000000000000002.tmp/session.bin", {}},
      {StorageFaultPoint::AFTER_DIR_FSYNC, 1, "state/SNAPSHOT-00000000000000000002.tmp", {}},
      {StorageFaultPoint::AFTER_RENAME, 1, "state/SNAPSHOT-00000000000000000002",
       "state/SNAPSHOT-00000000000000000002.tmp"},
      {StorageFaultPoint::AFTER_DIR_FSYNC, 2, "state", {}},
      {StorageFaultPoint::BEFORE_WRITE, 4, "state/MANIFEST-00000000000000000002.tmp", {}},
      {StorageFaultPoint::AFTER_FSYNC, 4, "state/MANIFEST-00000000000000000002.tmp", {}},
      {StorageFaultPoint::AFTER_RENAME, 2, "state/MANIFEST-00000000000000000002",
       "state/MANIFEST-00000000000000000002.tmp"},
      {StorageFaultPoint::AFTER_DIR_FSYNC, 3, "state", {}},
      {StorageFaultPoint::BEFORE_WRITE, 5, "state/CURRENT.tmp", {}},
      {StorageFaultPoint::AFTER_FSYNC, 5, "state/CURRENT.tmp", {}},
      {StorageFaultPoint::AFTER_RENAME, 3, "state/CURRENT", "state/CURRENT.tmp"},
      {StorageFaultPoint::AFTER_DIR_FSYNC, 4, "state", {}},
  };
  EXPECT_NO_THROW(VerifyAtomicDurableTransition<uint64_t>(1, 2, expected_events, run));
}

// M1-IT03: logical capture waits for an in-flight reader; only durability/publication continues after latch release.
TEST(SnapshotManagerTest, LogicalCaptureWaitsForReaderBarrier) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto root =
      std::filesystem::temp_directory_path() / ("bustub-snapshot-reader-barrier-" + std::to_string(getpid()));
  storage->RemoveTree(root);
  auto node_directory = NodeDirectory::Open(root, storage);

  DiskManagerUnlimitedMemory source_disk;
  BufferPoolManager source_bpm(32, &source_disk);
  Catalog source(&source_bpm, nullptr, nullptr);
  const Schema schema({Column("id", TypeId::INTEGER), Column("value", TypeId::INTEGER)});
  auto table = source.CreateTableWithOid(nullptr, "barrier_items", schema, 0,
                                         ReplicatedPrimaryKeyDefinition{0, TypeId::INTEGER, 1});
  ASSERT_NE(table, nullptr);
  ASSERT_TRUE(table->table_
                  ->InsertTuple({4, false},
                                Tuple({ValueFactory::GetIntegerValue(7), ValueFactory::GetIntegerValue(70)}, &schema))
                  .has_value());
  const auto key_schema = Schema::CopySchema(&schema, {0});
  const auto primary_index = source.CreateIndex<IntegerKeyType, IntegerValueType, IntegerComparatorType>(
      nullptr, "barrier_items_pk", "barrier_items", schema, key_schema, {0}, TWO_INTEGER_SIZE,
      IntegerHashFunctionType{}, true);
  ASSERT_NE(primary_index, nullptr);
  source.AdvanceSchemaEpoch();
  SessionTable sessions;
  const auto response = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 1, 0, 4});
  sessions.RestoreRecords({{42, SessionRecord{1, response}}});
  StateVisibilityLatch latch;
  SnapshotManager snapshots(node_directory.get(), storage);

  auto reader = latch.LockShared();
  auto publication =
      std::async(std::launch::async, [&] { return snapshots.CreateSnapshot(source, sessions, &latch, 1, 4, 0); });
  EXPECT_EQ(publication.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
  reader.unlock();
  const auto manifest = publication.get();
  EXPECT_EQ(manifest.last_included_index_, 4);
  EXPECT_EQ(manifest.last_included_term_, 0);

  auto recovered = snapshots.Recover([](uint64_t index) { return index == 4; }, 16);
  ASSERT_NE(recovered, nullptr);
  auto recovered_table = recovered->catalog_->GetTable("barrier_items");
  ASSERT_NE(recovered_table, nullptr);
  auto iterator = recovered_table->table_->MakeIterator();
  ASSERT_FALSE(iterator.IsEnd());
  auto [meta, tuple] = iterator.GetTuple();
  EXPECT_FALSE(meta.is_deleted_);
  EXPECT_EQ(meta.ts_, 4);
  EXPECT_EQ(tuple.GetValue(&recovered_table->schema_, 0).GetAs<int32_t>(), 7);
  EXPECT_EQ(tuple.GetValue(&recovered_table->schema_, 1).GetAs<int32_t>(), 70);
  ++iterator;
  EXPECT_TRUE(iterator.IsEnd());
  ASSERT_TRUE(recovered->sessions_->GetLastResponse(42).has_value());
  EXPECT_EQ(*recovered->sessions_->GetLastResponse(42), response);

  // SnapshotManager is the term-0 publisher. Distributed snapshots and their
  // nonzero Raft terms belong to SnapshotStore and must not enter this format.
  EXPECT_THROW(snapshots.CreateSnapshot(source, sessions, &latch, 2, 4, 2), std::runtime_error);
  EXPECT_FALSE(storage->Exists(node_directory->StateDirectory() / "SNAPSHOT-00000000000000000002"));
  EXPECT_FALSE(storage->Exists(node_directory->StateDirectory() / "SNAPSHOT-00000000000000000002.tmp"));

  const auto nonzero_term_response = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 2, 3, 4});
  sessions.RestoreRecords({{42, SessionRecord{2, nonzero_term_response}}});
  EXPECT_THROW(snapshots.CreateSnapshot(source, sessions, &latch, 2, 4, 0), std::runtime_error);
  EXPECT_FALSE(storage->Exists(node_directory->StateDirectory() / "SNAPSHOT-00000000000000000002"));
  EXPECT_FALSE(storage->Exists(node_directory->StateDirectory() / "SNAPSHOT-00000000000000000002.tmp"));

  const auto future_response = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 2, 0, 5});
  sessions.RestoreRecords({{42, SessionRecord{2, future_response}}});
  EXPECT_THROW(snapshots.CreateSnapshot(source, sessions, &latch, 2, 4, 0), std::runtime_error);
  EXPECT_FALSE(storage->Exists(node_directory->StateDirectory() / "SNAPSHOT-00000000000000000002"));
  EXPECT_FALSE(storage->Exists(node_directory->StateDirectory() / "SNAPSHOT-00000000000000000002.tmp"));

  recovered.reset();
  node_directory.reset();
  storage->RemoveTree(root);
}

}  // namespace bustub
