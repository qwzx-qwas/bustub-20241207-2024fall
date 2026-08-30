//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// command_log_test.cpp
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "power_loss_storage.h"  // NOLINT(build/include_subdir)
#include "recovery/command_log.h"

namespace bustub {
namespace {

auto TempLogDirectory(const std::string &suffix) -> std::filesystem::path {
  return std::filesystem::temp_directory_path() / ("bustub-command-log-" + std::to_string(getpid()) + "-" + suffix);
}

}  // namespace

// M2-T03: the shared empty-state sentinel and a durable multi-entry append survive reopen.
TEST(CommandLogTest, EmptySentinelAndDurableBatchAppend) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory = TempLogDirectory("sentinel");
  storage->RemoveTree(directory);
  auto log = CommandLog::Open(directory, storage, 0);
  EXPECT_EQ(log->LastLogIndex(), 0);
  EXPECT_EQ(log->TermAt(0), 0);
  EXPECT_FALSE(log->EntryAt(0).has_value());

  std::vector<ReplicatedLogEntry> entries{{1, 1, 0, EntryType::COMMAND_BATCH, {std::byte{1}}},
                                          {1, 2, 0, EntryType::COMMAND_BATCH, {std::byte{2}}}};
  EXPECT_NO_THROW(log->Append(entries));
  EXPECT_EQ(log->LastLogIndex(), 2);
  log.reset();

  auto reopened = CommandLog::Open(directory, storage, 2);
  EXPECT_EQ(reopened->Entries(1, 2), entries);
  storage->RemoveTree(directory);
}

// One accepted Append is one durable segment even when the batch is larger than the rolling segment target. The
// explicit batch boundary is also the exact recovery read boundary, so success can never create an unreopenable log.
TEST(CommandLogTest, MultiEntryBatchBoundaryAlwaysReopens) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory = TempLogDirectory("batch-boundary");
  storage->RemoveTree(directory);
  const CommandLogOptions options{/*segment_max_bytes=*/40, /*batch_max_bytes=*/80};
  auto log = CommandLog::Open(directory, storage, 0, 0, 0, options);
  const std::vector<ReplicatedLogEntry> accepted{{1, 1, 1, EntryType::NOOP, {}}, {1, 2, 1, EntryType::NOOP, {}}};
  ASSERT_NO_THROW(log->Append(accepted));
  EXPECT_EQ(storage->FileSize(directory / "LOG-00000000000000000001"), 80);

  const auto bytes_before_rejection =
      storage->ReadFile(directory / "LOG-00000000000000000001", options.batch_max_bytes_);
  EXPECT_THROW(log->Append({{1, 3, 1, EntryType::NOOP, {}}, {1, 4, 1, EntryType::KV_COMMAND, {std::byte{1}}}}),
               std::runtime_error);
  EXPECT_EQ(log->LastLogIndex(), 2);
  EXPECT_EQ(storage->ReadFile(directory / "LOG-00000000000000000001", options.batch_max_bytes_),
            bytes_before_rejection);
  log.reset();

  auto reopened = CommandLog::Open(directory, storage, 2, 0, 0, options);
  EXPECT_EQ(reopened->Entries(1, 2), accepted);
  EXPECT_EQ(reopened->LastLogIndex(), 2);
  storage->RemoveTree(directory);
}

// M2-T04: a torn uncommitted tail is truncated, while committed corruption is fail-stop.
TEST(CommandLogTest, TailRepairRespectsCommittedBoundary) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory = TempLogDirectory("tail");
  storage->RemoveTree(directory);
  auto log = CommandLog::Open(directory, storage, 0);
  const ReplicatedLogEntry first{1, 1, 0, EntryType::COMMAND_BATCH, {std::byte{1}, std::byte{2}}};
  EXPECT_NO_THROW(log->Append({first}));
  log.reset();

  const auto segment = directory / "LOG-00000000000000000001";
  const auto second_frame = LogCodec::Encode({1, 2, 0, EntryType::COMMAND_BATCH, {std::byte{3}, std::byte{4}}});
  storage->AppendFileDurable(
      segment, std::vector<std::byte>(second_frame.begin(), second_frame.begin() + second_frame.size() / 2));
  auto repaired = CommandLog::Open(directory, storage, 1);
  EXPECT_EQ(repaired->LastLogIndex(), 1);
  EXPECT_EQ(repaired->EntryAt(1), first);
  repaired.reset();

  auto bytes = storage->ReadFile(segment, 1024 * 1024);
  bytes.back() ^= std::byte{1};
  storage->WriteFile(segment, bytes);
  storage->SyncFile(segment);
  EXPECT_THROW(CommandLog::Open(directory, storage, 1), std::runtime_error);
  storage->RemoveTree(directory);
}

// M1-T01: selecting a snapshot must not require a segment to begin exactly at S + 1. The retained bridge log may
// intentionally start at an older fallback snapshot boundary, including in the middle of the same physical segment.
TEST(CommandLogTest, SnapshotBaseCanReuseRetainedBridgePrefix) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory = TempLogDirectory("snapshot-base");
  storage->RemoveTree(directory);
  auto log = CommandLog::Open(directory, storage, 0);
  std::vector<ReplicatedLogEntry> entries;
  for (uint64_t index = 1; index <= 4; index++) {
    entries.push_back({1, index, 0, EntryType::COMMAND_BATCH, {static_cast<std::byte>(index)}});
  }
  EXPECT_NO_THROW(log->Append(entries));
  log.reset();

  auto from_current_snapshot = CommandLog::Open(directory, storage, 4, 2, 0);
  EXPECT_EQ(from_current_snapshot->SnapshotBaseIndex(), 2);
  EXPECT_EQ(from_current_snapshot->TermAt(2), 0);
  EXPECT_FALSE(from_current_snapshot->EntryAt(2).has_value());
  EXPECT_EQ(from_current_snapshot->Entries(3, 4), (std::vector<ReplicatedLogEntry>{entries[2], entries[3]}));
  from_current_snapshot.reset();

  auto from_fallback_snapshot = CommandLog::Open(directory, storage, 4, 1, 0);
  EXPECT_EQ(from_fallback_snapshot->Entries(2, 4),
            (std::vector<ReplicatedLogEntry>{entries[1], entries[2], entries[3]}));
  storage->RemoveTree(directory);
}

// M2-T05: a durable but not-yet-committed suffix is removed before the single-node writer accepts a replacement.
TEST(CommandLogTest, DurableUncommittedSuffixCanBeTruncated) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory = TempLogDirectory("truncate-suffix");
  storage->RemoveTree(directory);
  auto log = CommandLog::Open(directory, storage, 0);
  EXPECT_NO_THROW(
      log->Append({{1, 1, 0, EntryType::NOOP, {}}, {1, 2, 0, EntryType::NOOP, {}}, {1, 3, 0, EntryType::NOOP, {}}}));
  EXPECT_NO_THROW(log->TruncateSuffix(1));
  EXPECT_EQ(log->LastLogIndex(), 1);
  EXPECT_NO_THROW(log->Append({{1, 2, 0, EntryType::COMMAND_BATCH, {std::byte{9}}}}));
  log.reset();

  auto reopened = CommandLog::Open(directory, storage, 2);
  EXPECT_EQ(reopened->LastLogIndex(), 2);
  EXPECT_EQ(reopened->EntryAt(2)->payload_, std::vector<std::byte>{std::byte{9}});
  storage->RemoveTree(directory);
}

// M2-T06: prefix reclamation follows the oldest retained snapshot and never removes an overlapping bridge segment.
TEST(CommandLogTest, CompactionDeletesOnlyFullyCoveredSegments) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory = TempLogDirectory("compact-prefix");
  storage->RemoveTree(directory);
  const CommandLogOptions options{40};
  auto log = CommandLog::Open(directory, storage, 0, 0, 0, options);
  for (uint64_t index = 1; index <= 4; index++) {
    EXPECT_NO_THROW(log->Append({{1, index, 0, EntryType::NOOP, {}}}));
  }
  log.reset();

  auto current = CommandLog::Open(directory, storage, 4, 3, 0, options);
  EXPECT_NO_THROW(current->CompactPrefix(2));
  EXPECT_FALSE(storage->Exists(directory / "LOG-00000000000000000001"));
  EXPECT_FALSE(storage->Exists(directory / "LOG-00000000000000000002"));
  EXPECT_TRUE(storage->Exists(directory / "LOG-00000000000000000003"));
  current.reset();

  auto fallback = CommandLog::Open(directory, storage, 4, 2, 0, options);
  EXPECT_EQ(fallback->Entries(3, 4).size(), 2);
  storage->RemoveTree(directory);
}

// M2-T07: synchronous Append returns only after durability; named crashes recover one whole logical prefix.
TEST(CommandLogTest, NamedPowerLossMatrix) {
  const ReplicatedLogEntry first{1, 1, 0, EntryType::COMMAND_BATCH, {std::byte{1}}};
  const ReplicatedLogEntry second{1, 2, 0, EntryType::COMMAND_BATCH, {std::byte{2}}};
  const std::vector<ReplicatedLogEntry> old_state{first};
  const std::vector<ReplicatedLogEntry> new_state{first, second};
  const auto run = [&](std::optional<StorageFaultPlan> fault) -> AtomicDurabilityRun<std::vector<ReplicatedLogEntry>> {
    const auto suffix = fault.has_value() ? fault->Name() : "complete";
    const auto directory = TempLogDirectory("power-loss-" + suffix);
    auto storage = std::make_shared<PowerLossStorage>(directory);
    storage->RemoveTree(directory);
    auto log = CommandLog::Open(directory, storage, 0);
    log->Append({first});
    storage->ResetEventHistory();
    if (fault.has_value()) {
      storage->FailAt(*fault);
    }
    try {
      log->Append({second});
    } catch (const std::runtime_error &) {
      if (!fault.has_value()) {
        throw;
      }
    }
    const auto events = storage->Events();
    const auto fault_triggered = storage->FaultTriggered();
    log.reset();
    storage->DisableFailure();
    storage->PowerLoss();
    auto recovered = CommandLog::Open(directory, storage, 1);
    const auto recovered_state = recovered->Entries(1, recovered->LastLogIndex());
    recovered.reset();
    storage->RemoveTree(directory);
    return {recovered_state, events, fault_triggered};
  };

  const StorageEventTopology expected_events{
      {StorageFaultPoint::BEFORE_WRITE, 1, "LOG-00000000000000000001", {}},
      {StorageFaultPoint::AFTER_FSYNC, 1, "LOG-00000000000000000001", {}},
  };
  EXPECT_NO_THROW(VerifyAtomicDurableTransition(old_state, new_state, expected_events, run));
}

}  // namespace bustub
