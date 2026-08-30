//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// raft_state_machine_test.cpp
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "distributed/client_protocol.h"
#include "distributed/raft_state_machine.h"
#include "gtest/gtest.h"
#include "type/value_factory.h"

namespace bustub {
namespace {

auto AdapterRoot(std::string_view suffix) -> std::filesystem::path {
  return std::filesystem::temp_directory_path() /
         ("bustub-raft-fsm-" + std::to_string(getpid()) + "-" + std::string(suffix));
}

auto Key(int32_t value) -> EncodedPrimaryKeyV1 {
  return PrimaryKeyCodecV1::Encode(ValueFactory::GetIntegerValue(value));
}

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

using FileTreeImage = std::pair<std::vector<std::string>, std::map<std::string, std::vector<std::byte>>>;

auto CaptureFileTree(const std::filesystem::path &root, DurableStorage *storage) -> FileTreeImage {
  if (storage == nullptr) {
    throw std::runtime_error("test storage is null");
  }
  FileTreeImage image;
  if (!std::filesystem::exists(root)) {
    return image;
  }
  for (const auto &entry : std::filesystem::recursive_directory_iterator(root)) {
    const auto relative = std::filesystem::relative(entry.path(), root).generic_string();
    image.first.push_back(entry.is_directory() ? relative + "/" : relative);
    if (entry.is_regular_file()) {
      const auto size = storage->FileSize(entry.path());
      image.second.emplace(relative, storage->ReadFile(entry.path(), static_cast<size_t>(size)));
    }
  }
  std::sort(image.first.begin(), image.first.end());
  return image;
}

class ScopedStorageRoot {
 public:
  ScopedStorageRoot(std::shared_ptr<DurableStorage> storage, std::filesystem::path root)
      : storage_(std::move(storage)), root_(std::move(root)) {}

  ~ScopedStorageRoot() {
    try {
      storage_->RemoveTree(root_);
    } catch (...) {
    }
  }

 private:
  std::shared_ptr<DurableStorage> storage_;
  std::filesystem::path root_;
};

}  // namespace

TEST(BusTubRaftStateMachineTest, BundleLayoutMatchesIndependentV1Golden) {
  constexpr uint64_t last_included_index = 0x0102030405060708ULL;
  const auto database = Hex("deadbeef");
  const auto catalog = Hex("102030");
  const auto sessions = Hex("00ff");
  // Hand-authored BSBUND01 V1 frame: big-endian fields, three literal blobs, and fixed CRC-32C 0x57cc22e3.
  // Neither a production encoder nor the production checksum helper participates in constructing this oracle.
  const auto golden =
      Hex("425342554e4430310000000101020304050607080000000000000004deadbeef0000000000000003102030000000000000"
          "000200ff57cc22e3");

  const BusTubSnapshotBundleV1 bundle{last_included_index, database, catalog, sessions};
  EXPECT_EQ(BusTubSnapshotBundleCodec::Encode(bundle), golden);
  const auto decoded = BusTubSnapshotBundleCodec::Decode(golden);
  EXPECT_EQ(decoded.last_included_index_, last_included_index);
  EXPECT_EQ(decoded.database_, database);
  EXPECT_EQ(decoded.catalog_, catalog);
  EXPECT_EQ(decoded.sessions_, sessions);

  auto storage = std::make_shared<PosixDurableStorage>();
  const auto root = AdapterRoot("bundle-golden");
  storage->RemoveTree(root);
  ScopedStorageRoot cleanup(storage, root);
  storage->CreateDirectories(root);
  const CanonicalSnapshotPaths paths{root / "db.literal", root / "catalog.literal", root / "sessions.literal"};
  storage->WriteFile(paths.database_file_, database);
  storage->WriteFile(paths.catalog_file_, catalog);
  storage->WriteFile(paths.session_file_, sessions);
  const auto encoded_path = root / "encoded.bundle";
  BusTubSnapshotBundleCodec::EncodeFiles(last_included_index, paths, encoded_path, storage.get());
  EXPECT_EQ(storage->ReadFile(encoded_path, golden.size()), golden);

  auto wrapped = Hex("a1b2c3");
  wrapped.insert(wrapped.end(), golden.begin(), golden.end());
  const auto suffix = Hex("d4e5");
  wrapped.insert(wrapped.end(), suffix.begin(), suffix.end());
  const auto wrapped_path = root / "wrapped.bundle";
  storage->WriteFile(wrapped_path, wrapped);
  const auto file_view = BusTubSnapshotBundleCodec::DecodeFile({wrapped_path, 3, golden.size()}, storage.get());
  EXPECT_EQ(file_view.last_included_index_, last_included_index);
  EXPECT_EQ(file_view.database_.path_, wrapped_path);
  EXPECT_EQ(file_view.database_.offset_, 31);
  EXPECT_EQ(file_view.database_.size_, database.size());
  EXPECT_EQ(file_view.catalog_.path_, wrapped_path);
  EXPECT_EQ(file_view.catalog_.offset_, 43);
  EXPECT_EQ(file_view.catalog_.size_, catalog.size());
  EXPECT_EQ(file_view.sessions_.path_, wrapped_path);
  EXPECT_EQ(file_view.sessions_.offset_, 54);
  EXPECT_EQ(file_view.sessions_.size_, sessions.size());
  EXPECT_EQ(storage->ReadFileRange(file_view.database_.path_, file_view.database_.offset_, file_view.database_.size_),
            database);
  EXPECT_EQ(storage->ReadFileRange(file_view.catalog_.path_, file_view.catalog_.offset_, file_view.catalog_.size_),
            catalog);
  EXPECT_EQ(storage->ReadFileRange(file_view.sessions_.path_, file_view.sessions_.offset_, file_view.sessions_.size_),
            sessions);
}

TEST(BusTubRaftStateMachineTest, CanonicalPayloadInstallAndSuffixApply) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto left_root = AdapterRoot("left");
  const auto right_root = AdapterRoot("right");
  storage->RemoveTree(left_root);
  storage->RemoveTree(right_root);
  auto left_directory = NodeDirectory::Open(left_root, storage);
  auto right_directory = NodeDirectory::Open(right_root, storage);
  auto left = BusTubRaftStateMachine::Open(left_directory.get(), storage, 64);
  auto right = BusTubRaftStateMachine::Open(right_directory.get(), storage, 64);

  const std::string create_sql = "CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32));";
  const auto create_fingerprint = ComputeWriteIntentFingerprintV1(create_sql);
  const auto create = left->PrepareSql(create_sql, 44, 1, create_fingerprint);
  left->ValidateProposal(create);
  left->Apply({1, 1, 3, EntryType::COMMAND_BATCH, CommandBatchCodec::Encode(create)});
  const std::string insert_sql = "INSERT INTO accounts VALUES (1, 'before');";
  const auto insert_fingerprint = ComputeWriteIntentFingerprintV1(insert_sql);
  const auto insert = left->PrepareSql(insert_sql, 44, 2, insert_fingerprint);
  left->ValidateProposal(insert);
  left->Apply({1, 2, 3, EntryType::COMMAND_BATCH, CommandBatchCodec::Encode(insert)});

  const auto payload_path = left_root / "streamed-snapshot.bundle";
  left->CreateSnapshotFile(payload_path);
  const DurableFileSlice payload{payload_path, 0, storage->FileSize(payload_path)};
  const auto decoded = BusTubSnapshotBundleCodec::DecodeFile(payload, storage.get());
  EXPECT_EQ(decoded.last_included_index_, 2);
  const auto compatibility_bytes =
      storage->ReadFile(payload_path, BusTubSnapshotBundleCodec::MAX_IN_MEMORY_BUNDLE_BYTES);
  const auto compatibility_bundle = BusTubSnapshotBundleCodec::Decode(compatibility_bytes);
  EXPECT_EQ(compatibility_bundle.last_included_index_, 2);
  EXPECT_EQ(compatibility_bundle.database_,
            storage->ReadFileRange(decoded.database_.path_, decoded.database_.offset_, decoded.database_.size_));
  EXPECT_EQ(compatibility_bundle.catalog_,
            storage->ReadFileRange(decoded.catalog_.path_, decoded.catalog_.offset_, decoded.catalog_.size_));
  EXPECT_EQ(compatibility_bundle.sessions_,
            storage->ReadFileRange(decoded.sessions_.path_, decoded.sessions_.offset_, decoded.sessions_.size_));
  const auto snapshot_catalog = CatalogSnapshotCodec::Decode(compatibility_bundle.catalog_);
  ASSERT_EQ(snapshot_catalog.tables_.size(), 1);
  EXPECT_EQ(snapshot_catalog.tables_[0].table_name_, "accounts");
  SessionTable snapshot_sessions;
  SessionSnapshotCodec::DecodeInto(compatibility_bundle.sessions_, &snapshot_sessions);
  EXPECT_EQ(snapshot_sessions.Classify(44, 2, insert_fingerprint), RequestDisposition::RETRY_LAST);
  EXPECT_EQ(
      snapshot_sessions.Classify(44, 2, ComputeWriteIntentFingerprintV1("INSERT INTO accounts VALUES (1, 'changed');")),
      RequestDisposition::PAYLOAD_MISMATCH);
  EXPECT_EQ(snapshot_sessions.GetLastResponse(44), left->GetLastResponse(44));
  const auto right_runtime = right_directory->WorkingDirectory() / "bustub-raft-fsm";
  const auto right_entries_before_validation = storage->ListDirectory(right_runtime);
  right->ValidateSnapshotFile(payload, 2);
  EXPECT_EQ(right->LastApplied(), 0);
  EXPECT_TRUE(right->CatalogSnapshotForRead().tables_.empty());
  EXPECT_EQ(storage->ListDirectory(right_runtime), right_entries_before_validation);
  right->InstallSnapshotFile(payload, 2);
  ASSERT_TRUE(right->GetRow(0, Key(1)).has_value());
  EXPECT_EQ(right->GetRow(0, Key(1))->first.ts_, 2);
  EXPECT_EQ(right->ClassifyRequest(44, 2, insert_fingerprint), RequestDisposition::RETRY_LAST);
  EXPECT_EQ(
      right->ClassifyRequest(44, 2, ComputeWriteIntentFingerprintV1("INSERT INTO accounts VALUES (1, 'changed');")),
      RequestDisposition::PAYLOAD_MISMATCH);

  const std::string update_sql = "UPDATE accounts SET name = 'after' WHERE id = 1;";
  const auto update = right->PrepareSql(update_sql, 44, 3, ComputeWriteIntentFingerprintV1(update_sql));
  right->ValidateProposal(update);
  right->Apply({1, 3, 4, EntryType::COMMAND_BATCH, CommandBatchCodec::Encode(update)});
  const auto updated = right->GetRow(0, Key(1));
  ASSERT_TRUE(updated.has_value());
  EXPECT_EQ(updated->first.ts_, 3);
  const auto catalog = right->CatalogSnapshotForRead();
  ASSERT_EQ(catalog.tables_.size(), 1);
  EXPECT_EQ(updated->second.GetValue(&catalog.tables_[0].schema_, 1).ToString(), "after");
  EXPECT_EQ(right->PublishedAppliedIndex(), 3);
  const auto query =
      ClientQueryResultCodec::Decode(right->ExecuteReadSql("SELECT id, name FROM accounts WHERE id = 1;", 3));
  EXPECT_EQ(query.columns_, (std::vector<std::string>{"accounts.id", "accounts.name"}));
  EXPECT_EQ(query.rows_, (std::vector<std::vector<std::string>>{{"1", "after"}}));
  EXPECT_THROW(right->ExecuteReadSql("SELECT * FROM accounts;", 2), std::runtime_error);

  right.reset();
  left.reset();
  right_directory.reset();
  left_directory.reset();
  storage->RemoveTree(right_root);
  storage->RemoveTree(left_root);
}

TEST(BusTubRaftStateMachineTest, BundleRejectsCorruptionAndWrongBoundary) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto root = AdapterRoot("invalid");
  storage->RemoveTree(root);
  auto directory = NodeDirectory::Open(root, storage);
  auto fsm = BusTubRaftStateMachine::Open(directory.get(), storage, 32);
  const auto payload_path = root / "valid-snapshot.bundle";
  const auto corrupt_path = root / "corrupt-snapshot.bundle";
  fsm->CreateSnapshotFile(payload_path);
  auto corrupt = storage->ReadFile(payload_path, 16U * 1024U * 1024U);
  corrupt.back() ^= std::byte{1};
  storage->WriteFile(corrupt_path, corrupt);
  EXPECT_THROW(fsm->InstallSnapshotFile({corrupt_path, 0, corrupt.size()}, 0), std::runtime_error);
  EXPECT_THROW(fsm->InstallSnapshotFile({payload_path, 0, storage->FileSize(payload_path)}, 1), std::runtime_error);
  fsm.reset();
  directory.reset();
  storage->RemoveTree(root);
}

TEST(BusTubRaftStateMachineTest, InstallRejectsSessionCommittedPastBundleBoundaryWithoutReplacingState) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto source_root = AdapterRoot("future-session-source");
  const auto target_root = AdapterRoot("future-session-target");
  storage->RemoveTree(source_root);
  storage->RemoveTree(target_root);
  auto source_directory = NodeDirectory::Open(source_root, storage);
  auto target_directory = NodeDirectory::Open(target_root, storage);
  auto source = BusTubRaftStateMachine::Open(source_directory.get(), storage, 32);
  auto target = BusTubRaftStateMachine::Open(target_directory.get(), storage, 32);

  const std::string sentinel_create_sql = "CREATE TABLE sentinel(id int PRIMARY KEY, note varchar(32));";
  const auto sentinel_create_fingerprint = ComputeWriteIntentFingerprintV1(sentinel_create_sql);
  const auto sentinel_create = target->PrepareSql(sentinel_create_sql, 91, 1, sentinel_create_fingerprint);
  target->ValidateProposal(sentinel_create);
  target->Apply({1, 1, 9, EntryType::COMMAND_BATCH, CommandBatchCodec::Encode(sentinel_create)});
  const std::string sentinel_insert_sql = "INSERT INTO sentinel VALUES (9, 'target-must-survive');";
  const auto sentinel_insert_fingerprint = ComputeWriteIntentFingerprintV1(sentinel_insert_sql);
  const auto sentinel_insert = target->PrepareSql(sentinel_insert_sql, 91, 2, sentinel_insert_fingerprint);
  target->ValidateProposal(sentinel_insert);
  target->Apply({1, 2, 9, EntryType::COMMAND_BATCH, CommandBatchCodec::Encode(sentinel_insert)});

  const auto target_runtime = target_directory->WorkingDirectory() / "bustub-raft-fsm";
  const auto target_files_before = CaptureFileTree(target_runtime, storage.get());
  const auto target_response_before = target->GetLastResponse(91);
  ASSERT_TRUE(target_response_before.has_value());
  const auto assert_target_unchanged = [&] {
    EXPECT_EQ(target->PublishedAppliedIndex(), 2);
    const auto target_catalog = target->CatalogSnapshotForRead();
    ASSERT_EQ(target_catalog.tables_.size(), 1);
    EXPECT_EQ(target_catalog.tables_[0].table_name_, "sentinel");
    const auto sentinel_row = target->GetRow(0, Key(9));
    ASSERT_TRUE(sentinel_row.has_value());
    EXPECT_EQ(sentinel_row->first.ts_, 2);
    EXPECT_EQ(sentinel_row->second.GetValue(&target_catalog.tables_[0].schema_, 1).ToString(), "target-must-survive");
    EXPECT_EQ(target->ClassifyRequest(91, 2, sentinel_insert_fingerprint), RequestDisposition::RETRY_LAST);
    EXPECT_EQ(target->GetLastResponse(91), target_response_before);
    EXPECT_EQ(CaptureFileTree(target_runtime, storage.get()), target_files_before);
  };
  assert_target_unchanged();

  const std::string create_sql = "CREATE TABLE accounts(id int PRIMARY KEY, balance int);";
  const auto create_fingerprint = ComputeWriteIntentFingerprintV1(create_sql);
  const auto create = source->PrepareSql(create_sql, 81, 1, create_fingerprint);
  source->ValidateProposal(create);
  source->Apply({1, 1, 3, EntryType::COMMAND_BATCH, CommandBatchCodec::Encode(create)});
  const auto valid_path = source_root / "snapshot-at-one.bundle";
  source->CreateSnapshotFile(valid_path);
  auto bundle = BusTubSnapshotBundleCodec::Decode(
      storage->ReadFile(valid_path, BusTubSnapshotBundleCodec::MAX_IN_MEMORY_BUNDLE_BYTES));
  ASSERT_EQ(bundle.last_included_index_, 1);
  SessionTable sessions;
  SessionSnapshotCodec::DecodeInto(bundle.sessions_, &sessions);
  ASSERT_EQ(sessions.Classify(81, 1, create_fingerprint), RequestDisposition::RETRY_LAST);
  const auto session_response = sessions.GetLastResponse(81);
  ASSERT_TRUE(session_response.has_value());
  ASSERT_EQ(WriteResponseCodec::Decode(*session_response).commit_index_, 1);
  EXPECT_EQ(*session_response, Hex("0000000100000001000000000000000100000000000000030000000000000001"));

  auto legacy_session_bundle = bundle;
  // Hand-authored valid SessionV1 frame for client 81/request 1/term 3/index 1. The literal CRC32C is 0x68f064ac;
  // no production framing or Session codec constructs the old-format rejection fixture.
  legacy_session_bundle.sessions_ =
      Hex("425354534553303100000001000000380000000100000000000000510000000000000001000000200000000100000001"
          "00000000000000010000000000000003000000000000000168f064ac");
  const auto legacy_session_bytes = BusTubSnapshotBundleCodec::Encode(legacy_session_bundle);
  const auto legacy_session_path = source_root / "snapshot-with-session-v1.bundle";
  storage->WriteFile(legacy_session_path, legacy_session_bytes);
  EXPECT_THROW(target->ValidateSnapshotFile({legacy_session_path, 0, legacy_session_bytes.size()}, 1),
               std::runtime_error);
  assert_target_unchanged();
  EXPECT_THROW(target->InstallSnapshotFile({legacy_session_path, 0, legacy_session_bytes.size()}, 1),
               std::runtime_error);
  assert_target_unchanged();

  bundle.last_included_index_ = 0;
  const auto invalid_bytes = BusTubSnapshotBundleCodec::Encode(bundle);
  const auto invalid_path = source_root / "snapshot-with-future-session.bundle";
  storage->WriteFile(invalid_path, invalid_bytes);
  EXPECT_THROW(target->ValidateSnapshotFile({invalid_path, 0, invalid_bytes.size()}, 0), std::runtime_error);
  assert_target_unchanged();
  EXPECT_THROW(target->InstallSnapshotFile({invalid_path, 0, invalid_bytes.size()}, 0), std::runtime_error);
  assert_target_unchanged();

  target.reset();
  source.reset();
  target_directory.reset();
  source_directory.reset();
  storage->RemoveTree(target_root);
  storage->RemoveTree(source_root);
}

}  // namespace bustub
