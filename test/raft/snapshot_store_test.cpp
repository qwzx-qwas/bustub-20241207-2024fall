//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// snapshot_store_test.cpp
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/byte_codec.h"
#include "gtest/gtest.h"
#include "raft/snapshot_store.h"

namespace bustub {

TEST(SnapshotStoreTest, ChunkStagingPublicationAndRecovery) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory = std::filesystem::temp_directory_path() / ("bustub-raft-snapshot-" + std::to_string(getpid()));
  storage->RemoveTree(directory);
  auto store = SnapshotStore::Open(directory, storage);
  EXPECT_FALSE(store->Latest().has_value());

  const std::vector<std::byte> payload{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}, std::byte{5},
                                       std::byte{6}, std::byte{7}, std::byte{8}, std::byte{9}};
  const auto checksum = Crc32c(payload);
  SnapshotChunk first{"remote-7", 7, 3, 0, payload.size(), checksum, false, {payload.begin(), payload.begin() + 4}};
  EXPECT_EQ(store->StageChunk(first).status_, SnapshotStageStatus::IN_PROGRESS);
  EXPECT_EQ(store->StageChunk(first).status_, SnapshotStageStatus::IN_PROGRESS);
  SnapshotChunk second{"remote-7", 7, 3, 4, payload.size(), checksum, true, {payload.begin() + 4, payload.end()}};
  EXPECT_EQ(store->StageChunk(second).status_, SnapshotStageStatus::COMPLETE);
  EXPECT_EQ(store->StageChunk(second).status_, SnapshotStageStatus::DUPLICATE_COMPLETE);
  ASSERT_TRUE(store->Staged("remote-7").has_value());
  ASSERT_TRUE(store->StagedPayloadFile("remote-7").has_value());
  EXPECT_EQ(storage->ReadFileRange(store->StagedPayloadFile("remote-7")->path_, 0, payload.size()), payload);

  const auto published = store->PublishFile(7, 3, store->StagedPayloadFile("remote-7")->path_);
  store->CancelStaged("remote-7");
  EXPECT_EQ(published.last_included_index_, 7);
  EXPECT_EQ(published.payload_size_, payload.size());
  EXPECT_EQ(published.payload_checksum_, checksum);
  EXPECT_EQ(store->ReadPayloadChunk(published, 0, payload.size()), payload);
  store.reset();
  auto reopened = SnapshotStore::Open(directory, storage);
  ASSERT_TRUE(reopened->Latest().has_value());
  EXPECT_EQ(reopened->ReadPayloadChunk(*reopened->Latest(), 0, payload.size()), payload);
  EXPECT_EQ(reopened->Latest()->last_included_term_, 3);

  storage->WriteFile(directory / "CURRENT.tmp", std::vector<std::byte>{std::byte{0}});
  auto ignores_tmp = SnapshotStore::Open(directory, storage);
  EXPECT_EQ(ignores_tmp->Latest()->snapshot_id_, published.snapshot_id_);
  storage->RemoveTree(directory);
}

TEST(SnapshotStoreTest, DuplicateOldChunkReportsDurableHighWater) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory =
      std::filesystem::temp_directory_path() / ("bustub-raft-snapshot-high-water-" + std::to_string(getpid()));
  storage->RemoveTree(directory);
  auto store = SnapshotStore::Open(directory, storage);

  const std::vector<std::byte> payload{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4},
                                       std::byte{5}, std::byte{6}, std::byte{7}, std::byte{8}};
  const auto checksum = Crc32c(payload);
  const SnapshotChunk first{"high-water",   12,       2,     0,
                            payload.size(), checksum, false, {payload.begin(), payload.begin() + 2}};
  const SnapshotChunk second{"high-water",   12,       2,     2,
                             payload.size(), checksum, false, {payload.begin() + 2, payload.begin() + 5}};

  const auto first_result = store->StageChunk(first);
  EXPECT_EQ(first_result.status_, SnapshotStageStatus::IN_PROGRESS);
  EXPECT_EQ(first_result.next_offset_, 2);
  const auto second_result = store->StageChunk(second);
  EXPECT_EQ(second_result.status_, SnapshotStageStatus::IN_PROGRESS);
  EXPECT_EQ(second_result.next_offset_, 5);

  // A delayed retransmission must acknowledge all bytes already made durable,
  // rather than pulling the sender back to the end of this old chunk.
  const auto duplicate_result = store->StageChunk(first);
  EXPECT_EQ(duplicate_result.status_, SnapshotStageStatus::IN_PROGRESS);
  EXPECT_EQ(duplicate_result.next_offset_, 5);
  const auto download_path = directory / "SNAPSHOT-DOWNLOAD.tmp";
  ASSERT_TRUE(storage->Exists(download_path));
  EXPECT_EQ(storage->FileSize(download_path), 5);
  EXPECT_EQ(storage->ReadFileRange(download_path, 0, 5), std::vector<std::byte>(payload.begin(), payload.begin() + 5));

  storage->RemoveTree(directory);
}

TEST(SnapshotStoreTest, CorruptCurrentScansImmutableGeneration) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory =
      std::filesystem::temp_directory_path() / ("bustub-raft-snapshot-corrupt-" + std::to_string(getpid()));
  storage->RemoveTree(directory);
  auto store = SnapshotStore::Open(directory, storage);
  const std::vector<std::byte> first_payload{std::byte{1}, std::byte{2}};
  const std::vector<std::byte> second_payload{std::byte{8}, std::byte{9}, std::byte{10}};
  static_cast<void>(store->Publish(2, 1, first_payload));
  const auto second = store->Publish(4, 2, second_payload);
  store.reset();
  auto current = storage->ReadFile(directory / "CURRENT", 4096);
  current.back() ^= std::byte{1};
  storage->WriteFile(directory / "CURRENT", current);
  storage->SyncFile(directory / "CURRENT");
  auto recovered = SnapshotStore::Open(directory, storage);
  ASSERT_TRUE(recovered->Latest().has_value());
  EXPECT_EQ(recovered->Latest()->generation_, 2);
  EXPECT_EQ(recovered->Latest()->last_included_index_, 4);
  EXPECT_EQ(recovered->Latest()->last_included_term_, 2);
  EXPECT_EQ(recovered->Latest()->snapshot_id_, second.snapshot_id_);
  EXPECT_EQ(recovered->ReadPayloadChunk(*recovered->Latest(), 0, second_payload.size()), second_payload);
  ASSERT_TRUE(recovered->OldestRetained().has_value());
  EXPECT_EQ(recovered->OldestRetained()->last_included_index_, 2);
  EXPECT_EQ(recovered->ReadPayloadChunk(*recovered->OldestRetained(), 0, first_payload.size()), first_payload);
  storage->RemoveTree(directory);
}

TEST(SnapshotStoreTest, CorruptLatestGenerationFallsBackToPrevious) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory =
      std::filesystem::temp_directory_path() / ("bustub-raft-snapshot-fallback-" + std::to_string(getpid()));
  storage->RemoveTree(directory);
  auto store = SnapshotStore::Open(directory, storage);
  const std::vector<std::byte> previous_payload{std::byte{2}, std::byte{3}};
  static_cast<void>(store->Publish(2, 1, previous_payload));
  static_cast<void>(store->Publish(4, 2, {std::byte{4}, std::byte{5}, std::byte{6}}));
  store.reset();

  const auto latest_path = directory / "SNAPSHOT-00000000000000000002";
  auto latest = storage->ReadFile(latest_path, 4096);
  latest[latest.size() / 2] ^= std::byte{1};
  storage->WriteFile(latest_path, latest);
  storage->SyncFile(latest_path);

  auto recovered = SnapshotStore::Open(directory, storage);
  ASSERT_TRUE(recovered->Latest().has_value());
  EXPECT_EQ(recovered->Latest()->generation_, 1);
  EXPECT_EQ(recovered->Latest()->last_included_index_, 2);
  EXPECT_EQ(recovered->Latest()->last_included_term_, 1);
  EXPECT_EQ(recovered->ReadPayloadChunk(*recovered->Latest(), 0, previous_payload.size()), previous_payload);
  EXPECT_EQ(recovered->OldestRetained()->last_included_index_, 2);
  storage->RemoveTree(directory);
}

TEST(SnapshotStoreTest, RejectsOutOfOrderConflictingDuplicateAndMetadataDrift) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory =
      std::filesystem::temp_directory_path() / ("bustub-raft-snapshot-chunk-order-" + std::to_string(getpid()));
  storage->RemoveTree(directory);
  auto store = SnapshotStore::Open(directory, storage);

  const std::vector<std::byte> payload{std::byte{10}, std::byte{11}, std::byte{12},
                                       std::byte{13}, std::byte{14}, std::byte{15}};
  const auto checksum = Crc32c(payload);
  const SnapshotChunk first{"remote-order", 19,       5,     0,
                            payload.size(), checksum, false, {payload.begin(), payload.begin() + 3}};
  ASSERT_EQ(store->StageChunk(first).status_, SnapshotStageStatus::IN_PROGRESS);

  const SnapshotChunk gap{"remote-order", 19,       5,     4,
                          payload.size(), checksum, false, {payload.begin() + 4, payload.end()}};
  EXPECT_THROW(store->StageChunk(gap), std::runtime_error);

  auto conflicting = first;
  conflicting.data_[1] = std::byte{99};
  EXPECT_THROW(store->StageChunk(conflicting), std::runtime_error);

  auto changed_index = first;
  changed_index.last_included_index_ = 20;
  EXPECT_THROW(store->StageChunk(changed_index), std::runtime_error);
  auto changed_term = first;
  changed_term.last_included_term_ = 6;
  EXPECT_THROW(store->StageChunk(changed_term), std::runtime_error);
  auto changed_size = first;
  changed_size.total_size_++;
  EXPECT_THROW(store->StageChunk(changed_size), std::runtime_error);
  auto changed_checksum = first;
  changed_checksum.payload_checksum_++;
  EXPECT_THROW(store->StageChunk(changed_checksum), std::runtime_error);

  // Rejections above must not poison the valid in-progress download.
  EXPECT_EQ(store->StageChunk(first).status_, SnapshotStageStatus::IN_PROGRESS);
  const SnapshotChunk final{"remote-order", 19,       5,    3,
                            payload.size(), checksum, true, {payload.begin() + 3, payload.end()}};
  EXPECT_EQ(store->StageChunk(final).status_, SnapshotStageStatus::COMPLETE);
  ASSERT_TRUE(store->StagedPayloadFile("remote-order").has_value());
  EXPECT_EQ(storage->ReadFileRange(store->StagedPayloadFile("remote-order")->path_, 0, payload.size()), payload);
  storage->RemoveTree(directory);
}

TEST(SnapshotStoreTest, RejectsPrematureDoneMissingDoneAndChecksumMismatch) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory =
      std::filesystem::temp_directory_path() / ("bustub-raft-snapshot-chunk-final-" + std::to_string(getpid()));
  storage->RemoveTree(directory);
  auto store = SnapshotStore::Open(directory, storage);
  const std::vector<std::byte> payload{std::byte{21}, std::byte{22}, std::byte{23}, std::byte{24}};
  const auto checksum = Crc32c(payload);

  const SnapshotChunk premature{"premature",    8,        2,    0,
                                payload.size(), checksum, true, {payload.begin(), payload.begin() + 2}};
  EXPECT_THROW(store->StageChunk(premature), std::runtime_error);
  EXPECT_FALSE(store->Staged("premature").has_value());
  store->CancelStaged("premature");

  const SnapshotChunk missing_done{"missing-done", 8, 2, 0, payload.size(), checksum, false, payload};
  EXPECT_THROW(store->StageChunk(missing_done), std::runtime_error);
  EXPECT_FALSE(store->Staged("missing-done").has_value());
  store->CancelStaged("missing-done");

  const SnapshotChunk bad_checksum{"bad-checksum", 8, 2, 0, payload.size(), checksum + 1, true, payload};
  EXPECT_THROW(store->StageChunk(bad_checksum), std::runtime_error);
  EXPECT_FALSE(store->Staged("bad-checksum").has_value());
  store->CancelStaged("bad-checksum");

  const SnapshotChunk complete{"complete", 8, 2, 0, payload.size(), checksum, true, payload};
  EXPECT_EQ(store->StageChunk(complete).status_, SnapshotStageStatus::COMPLETE);
  EXPECT_EQ(store->StageChunk(complete).status_, SnapshotStageStatus::DUPLICATE_COMPLETE);
  storage->RemoveTree(directory);
}

TEST(SnapshotStoreTest, RestartDiscardsPartialDownloadAndRequiresOffsetZero) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory =
      std::filesystem::temp_directory_path() / ("bustub-raft-snapshot-chunk-restart-" + std::to_string(getpid()));
  storage->RemoveTree(directory);
  auto store = SnapshotStore::Open(directory, storage);
  const std::vector<std::byte> payload{std::byte{31}, std::byte{32}, std::byte{33}, std::byte{34}, std::byte{35}};
  const auto checksum = Crc32c(payload);
  const SnapshotChunk first{"restart",      13,       4,     0,
                            payload.size(), checksum, false, {payload.begin(), payload.begin() + 2}};
  ASSERT_EQ(store->StageChunk(first).status_, SnapshotStageStatus::IN_PROGRESS);
  store.reset();

  auto reopened = SnapshotStore::Open(directory, storage);
  EXPECT_FALSE(reopened->Staged("restart").has_value());
  const SnapshotChunk suffix{"restart", 13, 4, 2, payload.size(), checksum, true, {payload.begin() + 2, payload.end()}};
  EXPECT_THROW(reopened->StageChunk(suffix), std::runtime_error);

  // Starting again at zero replaces the orphaned partial file instead of appending to it.
  const SnapshotChunk complete{"restart", 13, 4, 0, payload.size(), checksum, true, payload};
  EXPECT_EQ(reopened->StageChunk(complete).status_, SnapshotStageStatus::COMPLETE);
  ASSERT_TRUE(reopened->StagedPayloadFile("restart").has_value());
  EXPECT_EQ(storage->ReadFileRange(reopened->StagedPayloadFile("restart")->path_, 0, payload.size()), payload);
  storage->RemoveTree(directory);
}

TEST(SnapshotStoreTest, RetainsOnlyNewestTwoPublishedGenerations) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory =
      std::filesystem::temp_directory_path() / ("bustub-raft-snapshot-retention-" + std::to_string(getpid()));
  storage->RemoveTree(directory);
  auto store = SnapshotStore::Open(directory, storage);
  static_cast<void>(store->Publish(2, 1, {std::byte{2}}));
  static_cast<void>(store->Publish(3, 1, {std::byte{3}}));
  static_cast<void>(store->Publish(4, 2, {std::byte{4}}));
  EXPECT_FALSE(storage->Exists(directory / "SNAPSHOT-00000000000000000001"));
  EXPECT_TRUE(storage->Exists(directory / "SNAPSHOT-00000000000000000002"));
  EXPECT_TRUE(storage->Exists(directory / "SNAPSHOT-00000000000000000003"));
  store.reset();
  auto reopened = SnapshotStore::Open(directory, storage);
  ASSERT_TRUE(reopened->Latest().has_value());
  EXPECT_EQ(reopened->Latest()->last_included_index_, 4);
  ASSERT_TRUE(reopened->OldestRetained().has_value());
  EXPECT_EQ(reopened->OldestRetained()->last_included_index_, 3);
  storage->RemoveTree(directory);
}

TEST(SnapshotStoreTest, RejectsPayloadBeyondExperimentalFileLimitWithoutAllocation) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory =
      std::filesystem::temp_directory_path() / ("bustub-raft-snapshot-limit-" + std::to_string(getpid()));
  storage->RemoveTree(directory);
  auto store = SnapshotStore::Open(directory, storage);

  const SnapshotChunk oversized{"oversized", 1, 1, 0, SnapshotStore::MAX_SNAPSHOT_BYTES + 1, 0, false, {}};
  EXPECT_THROW(store->StageChunk(oversized), std::runtime_error);
  EXPECT_FALSE(store->Staged("oversized").has_value());

  storage->RemoveTree(directory);
}

}  // namespace bustub
