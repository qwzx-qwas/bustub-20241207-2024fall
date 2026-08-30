//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_store_test.cpp
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "../recovery/power_loss_storage.h"  // NOLINT(build/include_subdir)
#include "gtest/gtest.h"
#include "raft/log_store.h"

namespace bustub {
namespace {

auto Entry(uint64_t index, uint64_t term, std::string payload = {}) -> ReplicatedLogEntry {
  return {1,
          index,
          term,
          payload.empty() ? EntryType::NOOP : EntryType::KV_COMMAND,
          {reinterpret_cast<const std::byte *>(payload.data()),
           reinterpret_cast<const std::byte *>(payload.data() + payload.size())}};
}

auto TestDirectory(std::string_view suffix) -> std::filesystem::path {
  return std::filesystem::temp_directory_path() /
         ("bustub-log-store-" + std::to_string(getpid()) + "-" + std::string(suffix));
}

struct LogState {
  uint64_t snapshot_base_index_;
  uint64_t snapshot_base_term_;
  std::vector<ReplicatedLogEntry> entries_;

  friend auto operator==(const LogState &lhs, const LogState &rhs) -> bool {
    return lhs.snapshot_base_index_ == rhs.snapshot_base_index_ && lhs.snapshot_base_term_ == rhs.snapshot_base_term_ &&
           lhs.entries_ == rhs.entries_;
  }
};

auto Observe(const LogStore &store) -> LogState {
  const auto first = store.SnapshotBaseIndex() + 1;
  const auto last = store.LastLogIndex();
  return {store.SnapshotBaseIndex(), store.SnapshotBaseTerm(),
          first <= last ? store.Entries(first, last) : std::vector<ReplicatedLogEntry>{}};
}

constexpr size_t MUTATION_ENVELOPE_BYTES = 8 + (4 + 4 + 8 + 8 + 1 + 4) + 4;
constexpr size_t NOOP_ENTRY_WRAPPER_BYTES =
    sizeof(uint32_t) + LogCodec::FRAME_HEADER_BYTES + LogCodec::FRAME_BODY_FIXED_BYTES;
constexpr size_t NOOP_APPEND_MUTATION_BYTES = MUTATION_ENVELOPE_BYTES + NOOP_ENTRY_WRAPPER_BYTES;

auto CanonicalRewriteEvents() -> StorageEventTopology {
  return {
      {StorageFaultPoint::BEFORE_WRITE, 1, "LOG-MUTATIONS.tmp", {}},
      {StorageFaultPoint::AFTER_FSYNC, 1, "LOG-MUTATIONS.tmp", {}},
      {StorageFaultPoint::AFTER_RENAME, 1, "LOG-MUTATIONS", "LOG-MUTATIONS.tmp"},
      {StorageFaultPoint::AFTER_DIR_FSYNC, 1, ".", {}},
  };
}

}  // namespace

// M3-T03: sentinel, append, committed boundary, and conflict replacement all survive restart.
TEST(LogStoreTest, DurableLogicalSuffixReplacement) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory = TestDirectory("replace");
  storage->RemoveTree(directory);
  auto store = LogStore::Open(directory, storage, 0);
  EXPECT_EQ(store->TermAt(0), 0);
  EXPECT_EQ(store->LastLogIndex(), 0);
  EXPECT_NO_THROW(store->Append({Entry(1, 1, "a"), Entry(2, 1, "old-b"), Entry(3, 1, "old-c")}));
  store->AdvanceCommittedIndex(1);
  EXPECT_THROW(store->ReplaceSuffix(1, {Entry(1, 2, "illegal")}), std::runtime_error);
  EXPECT_NO_THROW(store->ReplaceSuffix(2, {Entry(2, 2, "new-b"), Entry(3, 2, "new-c"), Entry(4, 2)}));
  EXPECT_EQ(store->LastLogIndex(), 4);
  EXPECT_EQ(store->TermAt(1), 1);
  EXPECT_EQ(store->TermAt(2), 2);
  store.reset();

  auto reopened = LogStore::Open(directory, storage, 3);
  ASSERT_TRUE(reopened->EntryAt(1).has_value());
  EXPECT_EQ(reopened->EntryAt(1)->index_, 1);
  EXPECT_EQ(reopened->EntryAt(1)->term_, 1);
  EXPECT_EQ(reopened->EntryAt(1)->type_, EntryType::KV_COMMAND);
  EXPECT_EQ(reopened->EntryAt(1)->payload_, Entry(1, 1, "a").payload_);
  EXPECT_EQ(reopened->Entries(1, 4), (std::vector<ReplicatedLogEntry>{Entry(1, 1, "a"), Entry(2, 2, "new-b"),
                                                                      Entry(3, 2, "new-c"), Entry(4, 2)}));
  EXPECT_THROW(reopened->ReplaceSuffix(3, {}), std::runtime_error);
  storage->RemoveTree(directory);
}

// A successful replacement physically removes the old suffix. Damage to the canonical frame that carries a committed
// replacement is fail-stop, while a later uncommitted append remains a safely repairable tail.
TEST(LogStoreTest, CommittedReplacementDamageCannotReviveOldSuffix) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto complete_directory = TestDirectory("complete");
  const auto damaged_directory = TestDirectory("damaged");
  storage->RemoveTree(complete_directory);
  storage->RemoveTree(damaged_directory);

  auto complete = LogStore::Open(complete_directory, storage, 0);
  EXPECT_NO_THROW(complete->Append({Entry(1, 1, "a"), Entry(2, 1, "old"), Entry(3, 1, "tail")}));
  EXPECT_NO_THROW(complete->ReplaceSuffix(2, {Entry(2, 4, "replacement"), Entry(3, 4)}));
  complete.reset();
  const auto new_journal = storage->ReadFile(complete_directory / "LOG-MUTATIONS", 1024 * 1024);
  ASSERT_GT(new_journal.size(), MUTATION_ENVELOPE_BYTES);

  for (const bool truncate : {false, true}) {
    storage->RemoveTree(damaged_directory);
    storage->CreateDirectories(damaged_directory);
    auto damaged = new_journal;
    if (truncate) {
      damaged.pop_back();
    } else {
      damaged.back() ^= std::byte{1};
    }
    storage->WriteFile(damaged_directory / "LOG-MUTATIONS", damaged);
    storage->SyncFile(damaged_directory / "LOG-MUTATIONS");
    EXPECT_THROW(LogStore::Open(damaged_directory, storage, 3), std::runtime_error)
        << (truncate ? "truncated" : "checksum-corrupt");
  }

  storage->RemoveTree(damaged_directory);
  storage->CreateDirectories(damaged_directory);
  storage->WriteFile(damaged_directory / "LOG-MUTATIONS", new_journal);
  storage->SyncFile(damaged_directory / "LOG-MUTATIONS");
  auto with_uncommitted_tail = LogStore::Open(damaged_directory, storage, 3);
  with_uncommitted_tail->Append({Entry(4, 5, "uncommitted")});
  with_uncommitted_tail.reset();
  auto with_tail = storage->ReadFile(damaged_directory / "LOG-MUTATIONS", 1024 * 1024);
  with_tail.pop_back();
  storage->WriteFile(damaged_directory / "LOG-MUTATIONS", with_tail);
  storage->SyncFile(damaged_directory / "LOG-MUTATIONS");
  auto repaired = LogStore::Open(damaged_directory, storage, 3);
  EXPECT_EQ(repaired->Entries(1, 3),
            (std::vector<ReplicatedLogEntry>{Entry(1, 1, "a"), Entry(2, 4, "replacement"), Entry(3, 4)}));
  EXPECT_EQ(repaired->LastLogIndex(), 3);
  storage->RemoveTree(complete_directory);
  storage->RemoveTree(damaged_directory);
}

// Both append and canonical rewrite preflight the exact durable size. A rejected operation changes neither memory nor
// disk, and an operation exactly at the boundary remains reopenable.
TEST(LogStoreTest, JournalMaximumIsAnExactWriteBoundary) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto append_directory = TestDirectory("append-limit");
  const auto rewrite_directory = TestDirectory("rewrite-limit");
  storage->RemoveTree(append_directory);
  storage->RemoveTree(rewrite_directory);

  const LogStoreOptions append_options{2 * NOOP_APPEND_MUTATION_BYTES};
  auto append_store = LogStore::Open(append_directory, storage, 0, 0, 0, append_options);
  ASSERT_NO_THROW(append_store->Append({Entry(1, 1)}));
  ASSERT_NO_THROW(append_store->Append({Entry(2, 1)}));
  const auto full_append = storage->ReadFile(append_directory / "LOG-MUTATIONS", append_options.maximum_journal_bytes_);
  EXPECT_EQ(full_append.size(), append_options.maximum_journal_bytes_);
  EXPECT_THROW(append_store->Append({Entry(3, 1)}), std::runtime_error);
  EXPECT_EQ(storage->ReadFile(append_directory / "LOG-MUTATIONS", append_options.maximum_journal_bytes_), full_append);
  append_store.reset();
  EXPECT_EQ(LogStore::Open(append_directory, storage, 2, 0, 0, append_options)->LastLogIndex(), 2);

  const LogStoreOptions rewrite_options{2 * MUTATION_ENVELOPE_BYTES + 2 * NOOP_ENTRY_WRAPPER_BYTES};
  auto rewrite_store = LogStore::Open(rewrite_directory, storage, 0, 0, 0, rewrite_options);
  ASSERT_NO_THROW(rewrite_store->Append({Entry(1, 1), Entry(2, 1)}));
  const auto old_journal =
      storage->ReadFile(rewrite_directory / "LOG-MUTATIONS", rewrite_options.maximum_journal_bytes_);
  EXPECT_THROW(rewrite_store->ReplaceSuffix(1, {Entry(1, 2, "x"), Entry(2, 2)}), std::runtime_error);
  EXPECT_EQ(Observe(*rewrite_store), (LogState{0, 0, {Entry(1, 1), Entry(2, 1)}}));
  EXPECT_EQ(storage->ReadFile(rewrite_directory / "LOG-MUTATIONS", rewrite_options.maximum_journal_bytes_),
            old_journal);
  ASSERT_NO_THROW(rewrite_store->ReplaceSuffix(1, {Entry(1, 2), Entry(2, 2)}));
  EXPECT_EQ(storage->FileSize(rewrite_directory / "LOG-MUTATIONS"), rewrite_options.maximum_journal_bytes_);
  rewrite_store.reset();
  auto reopened = LogStore::Open(rewrite_directory, storage, 2, 0, 0, rewrite_options);
  EXPECT_EQ(reopened->Entries(1, 2), (std::vector<ReplicatedLogEntry>{Entry(1, 2), Entry(2, 2)}));

  storage->RemoveTree(append_directory);
  storage->RemoveTree(rewrite_directory);
}

// A verified Snapshot@S may explicitly replace a corrupt bridge only when it covers the exact effective commit.
TEST(LogStoreTest, VerifiedSnapshotRebuildIsExplicitAndStrict) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory = TestDirectory("verified-rebuild");
  storage->RemoveTree(directory);
  auto seed = LogStore::Open(directory, storage, 0);
  seed->Append({Entry(1, 1), Entry(2, 2), Entry(3, 3)});
  seed.reset();

  auto corrupted = storage->ReadFile(directory / "LOG-MUTATIONS", 1024 * 1024);
  corrupted.back() ^= std::byte{1};
  storage->WriteFile(directory / "LOG-MUTATIONS", corrupted);
  storage->SyncFile(directory / "LOG-MUTATIONS");
  EXPECT_THROW(LogStore::Open(directory, storage, 3, 3, 3), std::runtime_error);
  EXPECT_THROW(LogStore::RebuildFromVerifiedSnapshot(directory, storage, 2, 3, 3), std::runtime_error);

  auto rebuilt = LogStore::RebuildFromVerifiedSnapshot(directory, storage, 3, 3, 3);
  EXPECT_EQ(Observe(*rebuilt), (LogState{3, 3, {}}));
  rebuilt.reset();
  auto reopened = LogStore::Open(directory, storage, 3, 3, 3);
  EXPECT_EQ(Observe(*reopened), (LogState{3, 3, {}}));
  storage->RemoveTree(directory);
}

// M4-T01: suffix retention uses only the pre-install TermAt(S) proof and the base mutation is durable.
TEST(LogStoreTest, SnapshotBaseUsesPreInstallTerm) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto matching_directory = TestDirectory("snapshot-match");
  const auto mismatch_directory = TestDirectory("snapshot-mismatch");
  storage->RemoveTree(matching_directory);
  storage->RemoveTree(mismatch_directory);

  for (const auto &directory : {matching_directory, mismatch_directory}) {
    auto seed = LogStore::Open(directory, storage, 0);
    EXPECT_NO_THROW(seed->Append({Entry(1, 1), Entry(2, 2), Entry(3, 3), Entry(4, 3)}));
  }

  // Opening after CURRENT advanced but before the log-base mutation durably landed redoes the term decision.
  auto matching = LogStore::Open(matching_directory, storage, 2, 2, 2);
  EXPECT_EQ(matching->SnapshotBaseIndex(), 2);
  EXPECT_EQ(matching->SnapshotBaseTerm(), 2);
  EXPECT_EQ(matching->Entries(3, 4), (std::vector<ReplicatedLogEntry>{Entry(3, 3), Entry(4, 3)}));
  matching.reset();
  auto matching_reopened = LogStore::Open(matching_directory, storage, 2, 2, 2);
  EXPECT_EQ(matching_reopened->LastLogIndex(), 4);

  auto mismatch = LogStore::Open(mismatch_directory, storage, 2, 2, 9);
  EXPECT_EQ(mismatch->SnapshotBaseIndex(), 2);
  EXPECT_EQ(mismatch->SnapshotBaseTerm(), 9);
  EXPECT_EQ(mismatch->LastLogIndex(), 2);
  EXPECT_EQ(mismatch->TermAt(2), 9);
  EXPECT_FALSE(mismatch->TermAt(3).has_value());

  storage->RemoveTree(matching_directory);
  storage->RemoveTree(mismatch_directory);
}

// InstallSnapshot persists HARD_STATE before it publishes the log base. InstallSnapshotBase explicitly consumes that
// ordering contract and advances its cached commit even when the follower did not previously contain index S.
TEST(LogStoreTest, SnapshotBaseConsumesAlreadyDurableExternalCommit) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory = TestDirectory("snapshot-ahead-of-cached-commit");
  storage->RemoveTree(directory);
  auto store = LogStore::Open(directory, storage, 0);
  store->Append({Entry(1, 1)});
  store->AdvanceCommittedIndex(1);

  ASSERT_NO_THROW(store->InstallSnapshotBase(3, 2, false));
  EXPECT_EQ(store->SnapshotBaseIndex(), 3);
  EXPECT_EQ(store->CommittedIndex(), 3);
  ASSERT_NO_THROW(store->AdvanceCommittedIndex(3));
  store.reset();

  auto reopened = LogStore::Open(directory, storage, 3, 3, 2);
  EXPECT_EQ(Observe(*reopened), (LogState{3, 2, {}}));
  storage->RemoveTree(directory);
}

// M7-T01: establishing a snapshot base atomically rewrites away obsolete physical mutation history.
TEST(LogStoreTest, SnapshotBasePhysicallyCompactsJournal) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory = TestDirectory("snapshot-compact");
  storage->RemoveTree(directory);
  auto store = LogStore::Open(directory, storage, 0);
  std::vector<ReplicatedLogEntry> entries;
  for (uint64_t index = 1; index <= 100; index++) {
    entries.push_back(Entry(index, index <= 90 ? 4 : 5, std::string(256, static_cast<char>('a' + index % 26))));
  }
  ASSERT_NO_THROW(store->Append(entries));
  store->AdvanceCommittedIndex(90);
  const auto before = storage->ReadFile(directory / "LOG-MUTATIONS", 1024 * 1024).size();
  ASSERT_NO_THROW(store->InstallSnapshotBase(90, 4, true));
  const auto after = storage->ReadFile(directory / "LOG-MUTATIONS", 1024 * 1024).size();
  EXPECT_LT(after, before);
  EXPECT_EQ(store->SnapshotBaseIndex(), 90);
  EXPECT_EQ(store->Entries(91, 100), std::vector<ReplicatedLogEntry>(entries.begin() + 90, entries.end()));
  store.reset();

  auto reopened = LogStore::Open(directory, storage, 90, 90, 4);
  EXPECT_EQ(reopened->SnapshotBaseIndex(), 90);
  EXPECT_EQ(reopened->Entries(91, 100), std::vector<ReplicatedLogEntry>(entries.begin() + 90, entries.end()));
  storage->RemoveTree(directory);
}

// M3-T04: conflicting suffix replacement uses the shared named-fault recovery oracle.
TEST(LogStoreTest, ReplaceSuffixNamedPowerLossMatrix) {
  const std::vector<ReplicatedLogEntry> original{Entry(1, 1), Entry(2, 1), Entry(3, 1)};
  const std::vector<ReplicatedLogEntry> replacement{Entry(2, 2), Entry(3, 2), Entry(4, 2)};
  const LogState old_state{0, 0, original};
  const LogState new_state{0, 0, {original[0], replacement[0], replacement[1], replacement[2]}};
  const auto run = [&](std::optional<StorageFaultPlan> fault) -> AtomicDurabilityRun<LogState> {
    const auto suffix = fault.has_value() ? fault->Name() : "complete";
    const auto directory = TestDirectory("replace-power-loss-" + suffix);
    auto storage = std::make_shared<PowerLossStorage>(directory);
    storage->RemoveTree(directory);
    auto store = LogStore::Open(directory, storage, 0);
    store->Append(original);
    store->AdvanceCommittedIndex(1);
    storage->ResetEventHistory();
    if (fault.has_value()) {
      storage->FailAt(*fault);
    }
    try {
      store->ReplaceSuffix(2, replacement);
    } catch (const std::runtime_error &) {
      if (!fault.has_value()) {
        throw;
      }
    }
    const auto events = storage->Events();
    const auto fault_triggered = storage->FaultTriggered();
    store.reset();
    storage->DisableFailure();
    storage->PowerLoss();
    auto recovered = LogStore::Open(directory, storage, 1);
    const auto recovered_state = Observe(*recovered);
    recovered.reset();
    storage->RemoveTree(directory);
    return {recovered_state, events, fault_triggered};
  };

  EXPECT_NO_THROW(VerifyAtomicDurableTransition(old_state, new_state, CanonicalRewriteEvents(), run));
}

// M4-T05: snapshot-base journal rewrite and recovery use the same named-fault oracle.
TEST(LogStoreTest, InstallSnapshotBaseNamedPowerLossMatrix) {
  const std::vector<ReplicatedLogEntry> original{Entry(1, 1), Entry(2, 2), Entry(3, 3), Entry(4, 3)};
  const LogState old_state{0, 0, original};
  const LogState new_state{2, 2, {original[2], original[3]}};
  const auto run = [&](std::optional<StorageFaultPlan> fault) -> AtomicDurabilityRun<LogState> {
    const auto suffix = fault.has_value() ? fault->Name() : "complete";
    const auto directory = TestDirectory("install-power-loss-" + suffix);
    auto storage = std::make_shared<PowerLossStorage>(directory);
    storage->RemoveTree(directory);
    auto store = LogStore::Open(directory, storage, 0);
    store->Append(original);
    store->AdvanceCommittedIndex(2);
    storage->ResetEventHistory();
    if (fault.has_value()) {
      storage->FailAt(*fault);
    }
    try {
      store->InstallSnapshotBase(2, 2, true);
    } catch (const std::runtime_error &) {
      if (!fault.has_value()) {
        throw;
      }
    }
    const auto events = storage->Events();
    const auto fault_triggered = storage->FaultTriggered();
    store.reset();
    storage->DisableFailure();
    storage->PowerLoss();
    auto recovered = LogStore::Open(directory, storage, 2, 2, 2);
    const auto recovered_state = Observe(*recovered);
    recovered.reset();
    storage->RemoveTree(directory);
    return {recovered_state, events, fault_triggered};
  };

  EXPECT_NO_THROW(VerifyAtomicDurableTransition(old_state, new_state, CanonicalRewriteEvents(), run));
}

// The explicit verified-snapshot recovery path is itself an old-or-new atomic transition and is safe to retry after
// every named crash point.
TEST(LogStoreTest, VerifiedSnapshotRebuildNamedPowerLossRetryMatrix) {
  const std::vector<ReplicatedLogEntry> original{Entry(1, 1), Entry(2, 2), Entry(3, 3)};
  const LogState old_state{0, 0, original};
  const LogState new_state{3, 3, {}};
  const auto run = [&](std::optional<StorageFaultPlan> fault) -> AtomicDurabilityRun<LogState> {
    const auto suffix = fault.has_value() ? fault->Name() : "complete";
    const auto directory = TestDirectory("rebuild-power-loss-" + suffix);
    auto storage = std::make_shared<PowerLossStorage>(directory);
    storage->RemoveTree(directory);
    auto seed = LogStore::Open(directory, storage, 0);
    seed->Append(original);
    seed.reset();
    storage->ResetEventHistory();
    if (fault.has_value()) {
      storage->FailAt(*fault);
    }
    try {
      static_cast<void>(LogStore::RebuildFromVerifiedSnapshot(directory, storage, 3, 3, 3));
    } catch (const std::runtime_error &) {
      if (!fault.has_value()) {
        throw;
      }
    }
    const auto events = storage->Events();
    const auto fault_triggered = storage->FaultTriggered();
    storage->DisableFailure();
    storage->PowerLoss();

    LogState recovered_state;
    try {
      auto old = LogStore::Open(directory, storage, 3);
      recovered_state = Observe(*old);
    } catch (const std::runtime_error &) {
      auto current = LogStore::Open(directory, storage, 3, 3, 3);
      recovered_state = Observe(*current);
    }

    auto retried = LogStore::RebuildFromVerifiedSnapshot(directory, storage, 3, 3, 3);
    EXPECT_EQ(Observe(*retried), new_state);
    retried.reset();
    storage->PowerLoss();
    auto reopened = LogStore::Open(directory, storage, 3, 3, 3);
    EXPECT_EQ(Observe(*reopened), new_state);
    reopened.reset();
    storage->RemoveTree(directory);
    return {recovered_state, events, fault_triggered};
  };

  EXPECT_NO_THROW(VerifyAtomicDurableTransition(old_state, new_state, CanonicalRewriteEvents(), run));
}

}  // namespace bustub
