//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// raft_state_machine_test.cpp
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
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

  const auto create = left->PrepareSql("CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32));", 44, 1);
  left->ValidateProposal(create);
  left->Apply({1, 1, 3, EntryType::COMMAND_BATCH, CommandBatchCodec::Encode(create)});
  const auto insert = left->PrepareSql("INSERT INTO accounts VALUES (1, 'before');", 44, 2);
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
  EXPECT_EQ(snapshot_sessions.Classify(44, 2), RequestDisposition::RETRY_LAST);
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

  const auto update = right->PrepareSql("UPDATE accounts SET name = 'after' WHERE id = 1;", 44, 3);
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

  const auto create = source->PrepareSql("CREATE TABLE accounts(id int PRIMARY KEY, balance int);", 81, 1);
  source->ValidateProposal(create);
  source->Apply({1, 1, 3, EntryType::COMMAND_BATCH, CommandBatchCodec::Encode(create)});
  const auto valid_path = source_root / "snapshot-at-one.bundle";
  source->CreateSnapshotFile(valid_path);
  auto bundle = BusTubSnapshotBundleCodec::Decode(
      storage->ReadFile(valid_path, BusTubSnapshotBundleCodec::MAX_IN_MEMORY_BUNDLE_BYTES));
  ASSERT_EQ(bundle.last_included_index_, 1);
  SessionTable sessions;
  SessionSnapshotCodec::DecodeInto(bundle.sessions_, &sessions);
  ASSERT_EQ(sessions.Classify(81, 1), RequestDisposition::RETRY_LAST);
  const auto session_response = sessions.GetLastResponse(81);
  ASSERT_TRUE(session_response.has_value());
  ASSERT_EQ(WriteResponseCodec::Decode(*session_response).commit_index_, 1);

  bundle.last_included_index_ = 0;
  const auto invalid_bytes = BusTubSnapshotBundleCodec::Encode(bundle);
  const auto invalid_path = source_root / "snapshot-with-future-session.bundle";
  storage->WriteFile(invalid_path, invalid_bytes);
  const auto target_runtime = target_directory->WorkingDirectory() / "bustub-raft-fsm";
  const auto target_entries_before_validation = storage->ListDirectory(target_runtime);
  EXPECT_THROW(target->ValidateSnapshotFile({invalid_path, 0, invalid_bytes.size()}, 0), std::runtime_error);
  EXPECT_EQ(target->PublishedAppliedIndex(), 0);
  EXPECT_TRUE(target->CatalogSnapshotForRead().tables_.empty());
  EXPECT_EQ(storage->ListDirectory(target_runtime), target_entries_before_validation);
  EXPECT_THROW(target->InstallSnapshotFile({invalid_path, 0, invalid_bytes.size()}, 0), std::runtime_error);
  EXPECT_EQ(target->PublishedAppliedIndex(), 0);
  EXPECT_TRUE(target->CatalogSnapshotForRead().tables_.empty());

  target.reset();
  source.reset();
  target_directory.reset();
  source_directory.reset();
  storage->RemoveTree(target_root);
  storage->RemoveTree(source_root);
}

}  // namespace bustub
