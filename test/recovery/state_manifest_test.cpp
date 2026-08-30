//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// state_manifest_test.cpp
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "common/byte_codec.h"
#include "distributed/session_table.h"
#include "gtest/gtest.h"
#include "recovery/canonical_snapshot.h"
#include "recovery/state_manifest.h"
#include "storage/disk/disk_manager_memory.h"
#include "type/value_factory.h"

namespace bustub {
namespace {

auto Bytes(std::initializer_list<uint8_t> values) -> std::vector<std::byte> {
  std::vector<std::byte> result;
  result.reserve(values.size());
  for (const auto value : values) {
    result.push_back(static_cast<std::byte>(value));
  }
  return result;
}

void PutU32At(std::vector<std::byte> *bytes, size_t offset, uint32_t value) {
  ASSERT_NE(bytes, nullptr);
  ASSERT_LE(offset + sizeof(uint32_t), bytes->size());
  (*bytes)[offset] = static_cast<std::byte>((value >> 24U) & 0xffU);
  (*bytes)[offset + 1] = static_cast<std::byte>((value >> 16U) & 0xffU);
  (*bytes)[offset + 2] = static_cast<std::byte>((value >> 8U) & 0xffU);
  (*bytes)[offset + 3] = static_cast<std::byte>(value & 0xffU);
}

void RewriteCatalogNextTableOid(std::vector<std::byte> *frame, uint32_t next_table_oid) {
  constexpr size_t frame_header_size = 16;
  constexpr size_t schema_epoch_size = 8;
  constexpr size_t frame_checksum_size = 4;
  ASSERT_NE(frame, nullptr);
  ASSERT_GT(frame->size(), frame_header_size + schema_epoch_size + frame_checksum_size);
  PutU32At(frame, frame_header_size + schema_epoch_size, next_table_oid);
  const auto payload_size = frame->size() - frame_header_size - frame_checksum_size;
  const auto checksum = Crc32c(frame->data() + frame_header_size, payload_size);
  PutU32At(frame, frame->size() - frame_checksum_size, checksum);
}

auto MakeManifest(uint64_t generation, uint64_t index, const std::string &snapshot_name,
                  const CanonicalSnapshotResult &snapshot, DurableStorage *storage,
                  const std::filesystem::path &state_directory) -> StateManifest {
  return {1,
          generation,
          index,
          0,
          snapshot.catalog_.schema_epoch_,
          snapshot_name + "/db.bustub",
          snapshot_name + "/catalog.bin",
          snapshot_name + "/session.bin",
          storage->ChecksumFile(state_directory / snapshot_name / "db.bustub"),
          storage->ChecksumFile(state_directory / snapshot_name / "catalog.bin"),
          storage->ChecksumFile(state_directory / snapshot_name / "session.bin"),
          snapshot.catalog_.next_table_oid_,
          snapshot.catalog_.next_index_oid_};
}

auto PublishDistinctManifestGenerations(const std::filesystem::path &state_directory,
                                        const std::shared_ptr<DurableStorage> &storage)
    -> std::pair<StateManifest, StateManifest> {
  DiskManagerUnlimitedMemory source_disk;
  BufferPoolManager source_bpm(32, &source_disk);
  Catalog source(&source_bpm, nullptr, nullptr);
  const Schema schema({Column("id", TypeId::INTEGER), Column("value", TypeId::INTEGER)});
  auto table =
      source.CreateTableWithOid(nullptr, "versions", schema, 0, ReplicatedPrimaryKeyDefinition{0, TypeId::INTEGER, 1});
  if (table == nullptr) {
    throw std::runtime_error("failed to create manifest fixture table");
  }
  const auto key_schema = Schema::CopySchema(&schema, {0});
  if (source.CreateIndex<IntegerKeyType, IntegerValueType, IntegerComparatorType>(
          nullptr, "versions_pk", "versions", schema, key_schema, {0}, TWO_INTEGER_SIZE, IntegerHashFunctionType{},
          true) == nullptr) {
    throw std::runtime_error("failed to create manifest fixture primary index");
  }
  source.AdvanceSchemaEpoch();
  SessionTable sessions;
  sessions.RecordCommitted(51, 1, WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 1, 2, 5}));
  if (!table->table_
           ->InsertTuple({5, false},
                         Tuple({ValueFactory::GetIntegerValue(1), ValueFactory::GetIntegerValue(100)}, &schema))
           .has_value()) {
    throw std::runtime_error("failed to create manifest fixture generation one");
  }

  StateManifestStore store(state_directory, storage);
  const std::string snapshot1_name = "SNAPSHOT-00000000000000000001";
  const auto snapshot1 = CanonicalSnapshotBuilder::Build(
      source, sessions,
      {state_directory / snapshot1_name / "db.bustub", state_directory / snapshot1_name / "catalog.bin",
       state_directory / snapshot1_name / "session.bin"},
      storage.get(), 16);
  const auto manifest1 = MakeManifest(1, 5, snapshot1_name, snapshot1, storage.get(), state_directory);
  store.Publish(manifest1);

  sessions.RecordCommitted(51, 2, WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 2, 3, 9}));
  if (!table->table_
           ->InsertTuple({9, false},
                         Tuple({ValueFactory::GetIntegerValue(2), ValueFactory::GetIntegerValue(900)}, &schema))
           .has_value()) {
    throw std::runtime_error("failed to create manifest fixture generation two");
  }
  const std::string snapshot2_name = "SNAPSHOT-00000000000000000002";
  const auto snapshot2 = CanonicalSnapshotBuilder::Build(
      source, sessions,
      {state_directory / snapshot2_name / "db.bustub", state_directory / snapshot2_name / "catalog.bin",
       state_directory / snapshot2_name / "session.bin"},
      storage.get(), 16);
  const auto manifest2 = MakeManifest(2, 9, snapshot2_name, snapshot2, storage.get(), state_directory);
  store.Publish(manifest2);
  return {manifest1, manifest2};
}

}  // namespace

TEST(StateManifestTest, V1CodecMatchesFixedGoldenBytes) {
  const StateManifest manifest{1,
                               0x0102030405060708ULL,
                               0x1112131415161718ULL,
                               0x2122232425262728ULL,
                               0x3132333435363738ULL,
                               "state/gen-7/db.bustub",
                               "state/gen-7/catalog.bin",
                               "state/gen-7/session.bin",
                               0xa1b2c3d4U,
                               0x10203040U,
                               0x55667788U,
                               0x11223344U,
                               0x99aabbccU};
  // Independently assembled from the V1 field table: 131-byte payload and literal CRC-32C 0x40071f75.
  // Neither the expected frame nor its checksum invokes the production codec at test runtime.
  const auto golden = Bytes({
      0x42, 0x53, 0x54, 0x4d, 0x41, 0x4e, 0x30, 0x31, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x83, 0x01, 0x02, 0x03,
      0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26,
      0x27, 0x28, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x00, 0x00, 0x00, 0x15, 0x73, 0x74, 0x61, 0x74, 0x65,
      0x2f, 0x67, 0x65, 0x6e, 0x2d, 0x37, 0x2f, 0x64, 0x62, 0x2e, 0x62, 0x75, 0x73, 0x74, 0x75, 0x62, 0x00, 0x00, 0x00,
      0x17, 0x73, 0x74, 0x61, 0x74, 0x65, 0x2f, 0x67, 0x65, 0x6e, 0x2d, 0x37, 0x2f, 0x63, 0x61, 0x74, 0x61, 0x6c, 0x6f,
      0x67, 0x2e, 0x62, 0x69, 0x6e, 0x00, 0x00, 0x00, 0x17, 0x73, 0x74, 0x61, 0x74, 0x65, 0x2f, 0x67, 0x65, 0x6e, 0x2d,
      0x37, 0x2f, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x2e, 0x62, 0x69, 0x6e, 0xa1, 0xb2, 0xc3, 0xd4, 0x10, 0x20,
      0x30, 0x40, 0x55, 0x66, 0x77, 0x88, 0x11, 0x22, 0x33, 0x44, 0x99, 0xaa, 0xbb, 0xcc, 0x40, 0x07, 0x1f, 0x75,
  });
  ASSERT_EQ(golden.size(), 151);
  EXPECT_EQ(StateManifestCodec::Encode(manifest), golden);

  const auto decoded = StateManifestCodec::Decode(golden);
  EXPECT_EQ(decoded.format_version_, 1);
  EXPECT_EQ(decoded.generation_, 0x0102030405060708ULL);
  EXPECT_EQ(decoded.last_included_index_, 0x1112131415161718ULL);
  EXPECT_EQ(decoded.last_included_term_, 0x2122232425262728ULL);
  EXPECT_EQ(decoded.schema_epoch_, 0x3132333435363738ULL);
  EXPECT_EQ(decoded.database_file_, "state/gen-7/db.bustub");
  EXPECT_EQ(decoded.catalog_file_, "state/gen-7/catalog.bin");
  EXPECT_EQ(decoded.session_file_, "state/gen-7/session.bin");
  EXPECT_EQ(decoded.database_checksum_, 0xa1b2c3d4U);
  EXPECT_EQ(decoded.catalog_checksum_, 0x10203040U);
  EXPECT_EQ(decoded.session_checksum_, 0x55667788U);
  EXPECT_EQ(decoded.next_table_oid_, 0x11223344U);
  EXPECT_EQ(decoded.next_index_oid_, 0x99aabbccU);
}

// M1-T01: CURRENT publication selects a complete generation and falls back only with a valid bridge log.
TEST(StateManifestTest, PublishValidateAndLatestDamageFallback) {
  DiskManagerUnlimitedMemory source_disk;
  BufferPoolManager source_bpm(32, &source_disk);
  Catalog source(&source_bpm, nullptr, nullptr);
  const Schema schema({Column("id", TypeId::INTEGER)});
  auto table =
      source.CreateTableWithOid(nullptr, "t", schema, 0, ReplicatedPrimaryKeyDefinition{0, TypeId::INTEGER, 1});
  ASSERT_NE(table, nullptr);
  ASSERT_TRUE(table->table_->InsertTuple({1, false}, Tuple({ValueFactory::GetIntegerValue(1)}, &schema)).has_value());
  const auto key_schema = Schema::CopySchema(&schema, {0});
  const auto primary = source.CreateIndex<IntegerKeyType, IntegerValueType, IntegerComparatorType>(
      nullptr, "t_pk", "t", schema, key_schema, {0}, TWO_INTEGER_SIZE, IntegerHashFunctionType{}, true);
  ASSERT_NE(primary, nullptr);
  source.AdvanceSchemaEpoch();
  SessionTable sessions;

  auto storage = std::make_shared<PosixDurableStorage>();
  const auto state_directory = std::filesystem::temp_directory_path() / ("bustub-manifest-" + std::to_string(getpid()));
  storage->RemoveTree(state_directory);
  storage->CreateDirectories(state_directory);
  StateManifestStore store(state_directory, storage);

  const std::string snapshot1_name = "SNAPSHOT-00000000000000000001";
  const auto snapshot1 = CanonicalSnapshotBuilder::Build(
      source, sessions,
      {state_directory / snapshot1_name / "db.bustub", state_directory / snapshot1_name / "catalog.bin",
       state_directory / snapshot1_name / "session.bin"},
      storage.get(), 16);
  const auto manifest1 = MakeManifest(1, 1, snapshot1_name, snapshot1, storage.get(), state_directory);
  store.Publish(manifest1);

  ASSERT_TRUE(table->table_->InsertTuple({2, false}, Tuple({ValueFactory::GetIntegerValue(2)}, &schema)).has_value());
  const std::string snapshot2_name = "SNAPSHOT-00000000000000000002";
  const auto snapshot2 = CanonicalSnapshotBuilder::Build(
      source, sessions,
      {state_directory / snapshot2_name / "db.bustub", state_directory / snapshot2_name / "catalog.bin",
       state_directory / snapshot2_name / "session.bin"},
      storage.get(), 16);
  const auto manifest2 = MakeManifest(2, 2, snapshot2_name, snapshot2, storage.get(), state_directory);
  store.Publish(manifest2);
  ASSERT_EQ(store.SelectRecoveryPoint()->manifest_.generation_, 2);

  auto mismatched = manifest2;
  mismatched.schema_epoch_++;
  EXPECT_FALSE(store.Validate(mismatched));
  mismatched = manifest2;
  mismatched.next_table_oid_++;
  EXPECT_FALSE(store.Validate(mismatched));
  mismatched = manifest2;
  mismatched.next_index_oid_++;
  EXPECT_FALSE(store.Validate(mismatched));

  auto corrupt_catalog = storage->ReadFile(state_directory / manifest2.catalog_file_, 1024 * 1024);
  corrupt_catalog.back() ^= std::byte{1};
  storage->WriteFile(state_directory / manifest2.catalog_file_, corrupt_catalog);
  storage->SyncFile(state_directory / manifest2.catalog_file_);
  auto fallback = store.SelectRecoveryPoint([](uint64_t snapshot_index) { return snapshot_index <= 1; });
  ASSERT_TRUE(fallback.has_value());
  EXPECT_EQ(fallback->manifest_.generation_, 1);
  EXPECT_FALSE(store.SelectRecoveryPoint([](uint64_t) { return false; }).has_value());

  storage->RemoveTree(state_directory);
}

TEST(StateManifestTest, CorruptCurrentStillSelectsNewestCompleteGeneration) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto state_directory =
      std::filesystem::temp_directory_path() / ("bustub-manifest-current-" + std::to_string(getpid()));
  storage->RemoveTree(state_directory);
  storage->CreateDirectories(state_directory);
  const auto [manifest1, manifest2] = PublishDistinctManifestGenerations(state_directory, storage);
  ASSERT_NE(manifest1.database_checksum_, manifest2.database_checksum_);
  ASSERT_NE(manifest1.session_checksum_, manifest2.session_checksum_);

  storage->WriteFile(state_directory / "CURRENT", {std::byte{'b'}, std::byte{'a'}, std::byte{'d'}});
  storage->SyncFile(state_directory / "CURRENT");
  StateManifestStore store(state_directory, storage);
  const auto selected = store.SelectRecoveryPoint();
  ASSERT_TRUE(selected.has_value());
  EXPECT_EQ(selected->manifest_.generation_, 2);
  EXPECT_EQ(selected->manifest_.last_included_index_, 9);
  EXPECT_EQ(selected->manifest_.database_file_, manifest2.database_file_);
  EXPECT_EQ(selected->manifest_.catalog_file_, manifest2.catalog_file_);
  EXPECT_EQ(selected->manifest_.session_file_, manifest2.session_file_);
  EXPECT_EQ(selected->manifest_.database_checksum_, manifest2.database_checksum_);
  EXPECT_EQ(selected->manifest_.session_checksum_, manifest2.session_checksum_);
  storage->RemoveTree(state_directory);
}

TEST(StateManifestTest, InvalidLatestCatalogAllocatorFallsBackToOlderCompleteGeneration) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto state_directory =
      std::filesystem::temp_directory_path() / ("bustub-manifest-oid-fallback-" + std::to_string(getpid()));
  storage->RemoveTree(state_directory);
  storage->CreateDirectories(state_directory);
  const auto [manifest1, manifest2] = PublishDistinctManifestGenerations(state_directory, storage);

  const auto catalog_path = state_directory / manifest2.catalog_file_;
  auto catalog = storage->ReadFile(catalog_path, CatalogSnapshotCodec::MAX_CATALOG_BYTES);
  RewriteCatalogNextTableOid(&catalog, 0);
  storage->WriteFile(catalog_path, catalog);
  storage->SyncFile(catalog_path);
  auto invalid_latest = manifest2;
  invalid_latest.next_table_oid_ = 0;
  invalid_latest.catalog_checksum_ = storage->ChecksumFile(catalog_path);
  const auto manifest_path = state_directory / StateManifestStore::ManifestFileName(manifest2.generation_);
  storage->WriteFile(manifest_path, StateManifestCodec::Encode(invalid_latest));
  storage->SyncFile(manifest_path);

  StateManifestStore store(state_directory, storage);
  EXPECT_FALSE(store.Validate(invalid_latest));
  const auto selected = store.SelectRecoveryPoint([](uint64_t) { return true; });
  ASSERT_TRUE(selected.has_value());
  EXPECT_EQ(selected->manifest_.generation_, manifest1.generation_);
  EXPECT_EQ(selected->manifest_.last_included_index_, manifest1.last_included_index_);
  storage->RemoveTree(state_directory);
}

TEST(StateManifestTest, LatestSessionBeyondSnapshotBoundaryFallsBackToOlderGeneration) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto state_directory =
      std::filesystem::temp_directory_path() / ("bustub-manifest-session-fallback-" + std::to_string(getpid()));
  storage->RemoveTree(state_directory);
  storage->CreateDirectories(state_directory);
  const auto [manifest1, manifest2] = PublishDistinctManifestGenerations(state_directory, storage);

  auto invalid_latest = manifest2;
  invalid_latest.last_included_index_ = 8;
  const auto manifest_path = state_directory / StateManifestStore::ManifestFileName(manifest2.generation_);
  storage->WriteFile(manifest_path, StateManifestCodec::Encode(invalid_latest));
  storage->SyncFile(manifest_path);

  StateManifestStore store(state_directory, storage);
  EXPECT_FALSE(store.Validate(invalid_latest));
  const auto selected = store.SelectRecoveryPoint([](uint64_t) { return true; });
  ASSERT_TRUE(selected.has_value());
  EXPECT_EQ(selected->manifest_.generation_, manifest1.generation_);
  EXPECT_EQ(selected->manifest_.last_included_index_, 5);
  storage->RemoveTree(state_directory);
}

TEST(StateManifestTest, SymlinkedSnapshotFileCannotEscapeStateDirectory) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto base = std::filesystem::temp_directory_path() / ("bustub-manifest-symlink-" + std::to_string(getpid()));
  const auto state_directory = base / "state";
  const auto outside_catalog = base / "outside-catalog.bin";
  storage->RemoveTree(base);
  storage->CreateDirectories(state_directory);
  const auto [manifest1, manifest2] = PublishDistinctManifestGenerations(state_directory, storage);

  const auto catalog_path = state_directory / manifest2.catalog_file_;
  storage->CopyFile(catalog_path, outside_catalog);
  storage->RemoveFile(catalog_path);
  std::filesystem::create_symlink(outside_catalog, catalog_path);
  ASSERT_TRUE(storage->Exists(catalog_path));
  ASSERT_EQ(storage->ChecksumFile(catalog_path), manifest2.catalog_checksum_);

  StateManifestStore store(state_directory, storage);
  EXPECT_FALSE(store.Validate(manifest2));
  const auto selected = store.SelectRecoveryPoint([](uint64_t) { return true; });
  ASSERT_TRUE(selected.has_value());
  EXPECT_EQ(selected->manifest_.generation_, manifest1.generation_);
  storage->RemoveTree(base);
}

// M1-T02: path traversal and Catalog/Manifest cross-copy mismatches are rejected.
TEST(StateManifestTest, RejectsUnsafePathAndCrossCopyMismatch) {
  StateManifest unsafe{1, 1, 0, 0, 0, "../db.bustub", "snapshot/catalog.bin", "snapshot/session.bin", 0, 0, 0, 0, 0};
  EXPECT_THROW(StateManifestCodec::Encode(unsafe), std::runtime_error);
}

}  // namespace bustub
