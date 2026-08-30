//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// stable_store_test.cpp
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include <cstddef>
#include <filesystem>
#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

#include "../recovery/power_loss_storage.h"  // NOLINT(build/include_subdir)
#include "gtest/gtest.h"
#include "raft/stable_store.h"

namespace bustub {
namespace {

auto Bytes(std::initializer_list<unsigned char> values) -> std::vector<std::byte> {
  std::vector<std::byte> bytes;
  bytes.reserve(values.size());
  for (const auto value : values) {
    bytes.push_back(static_cast<std::byte>(value));
  }
  return bytes;
}

}  // namespace

TEST(StableStoreTest, HardStateV1MatchesFixedGoldenFrameAndLiteralFields) {
  const HardState expected{1, 0x0102030405060708ULL, 0x1112131415161718ULL, NodeId{0x2122232425262728ULL},
                           0x3132333435363738ULL};
  const auto golden = Bytes({
      0x42, 0x53, 0x54, 0x48, 0x41, 0x52, 0x44, 0x31,  // "BSTHARD1"
      0x00, 0x00, 0x00, 0x01,                          // frame version
      0x00, 0x00, 0x00, 0x21,                          // 33-byte payload
      0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,  // generation
      0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,  // current term
      0x01,                                            // voted_for is present
      0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,  // voted_for
      0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,  // commit index
      0xc7, 0xe5, 0xdf, 0xca,                          // payload CRC32C
  });

  ASSERT_EQ(golden.size(), 53);
  EXPECT_EQ(HardStateCodec::Encode(expected), golden);

  const auto decoded = HardStateCodec::Decode(golden);
  EXPECT_EQ(decoded.format_version_, 1U);
  EXPECT_EQ(decoded.generation_, 0x0102030405060708ULL);
  EXPECT_EQ(decoded.current_term_, 0x1112131415161718ULL);
  ASSERT_TRUE(decoded.voted_for_.has_value());
  EXPECT_EQ(*decoded.voted_for_, 0x2122232425262728ULL);
  EXPECT_EQ(decoded.commit_index_, 0x3132333435363738ULL);
}

// M3-T01: term, vote, and commit survive as one checksummed generation.
TEST(StableStoreTest, AtomicGenerationRoundTrip) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory = std::filesystem::temp_directory_path() / ("bustub-stable-store-" + std::to_string(getpid()));
  storage->RemoveTree(directory);
  auto store = StableStore::Open(directory, storage);
  EXPECT_EQ(store->State(), HardState{});
  EXPECT_NO_THROW(store->Update(1, NodeId{2}, 0));
  EXPECT_NO_THROW(store->Update(1, NodeId{2}, 9));
  EXPECT_EQ(store->State(), (HardState{1, 2, 1, NodeId{2}, 9}));
  store.reset();

  auto reopened = StableStore::Open(directory, storage);
  EXPECT_EQ(reopened->State(), (HardState{1, 2, 1, NodeId{2}, 9}));
  EXPECT_THROW(reopened->Update(1, NodeId{3}, 9), std::runtime_error);
  EXPECT_THROW(reopened->Update(0, std::nullopt, 9), std::runtime_error);
  EXPECT_THROW(reopened->Update(2, std::nullopt, 8), std::runtime_error);
  storage->RemoveTree(directory);
}

// M3-T02: a corrupt formal generation is fail-stop; an unacknowledged tmp is ignored.
TEST(StableStoreTest, RejectsFormalCorruptionAndIgnoresTmp) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto directory = std::filesystem::temp_directory_path() / ("bustub-stable-corrupt-" + std::to_string(getpid()));
  storage->RemoveTree(directory);
  auto store = StableStore::Open(directory, storage);
  EXPECT_NO_THROW(store->Update(4, NodeId{1}, 3));
  store.reset();
  storage->WriteFile(directory / "HARD_STATE.tmp", HardStateCodec::Encode({1, 2, 5, NodeId{2}, 3}));
  auto reopened = StableStore::Open(directory, storage);
  EXPECT_EQ(reopened->State(), (HardState{1, 1, 4, NodeId{1}, 3}));
  reopened.reset();

  auto bytes = storage->ReadFile(directory / "HARD_STATE", 1024);
  bytes.back() ^= std::byte{1};
  storage->WriteFile(directory / "HARD_STATE", bytes);
  storage->SyncFile(directory / "HARD_STATE");
  EXPECT_THROW(StableStore::Open(directory, storage), std::runtime_error);
  storage->RemoveTree(directory);
}

// M3-T02: all named HARD_STATE publication failures share the old-or-new recovery oracle.
TEST(StableStoreTest, NamedPowerLossMatrix) {
  const HardState old_state{1, 1, 1, NodeId{1}, 0};
  const HardState new_state{1, 2, 2, NodeId{2}, 1};
  const auto run = [&](std::optional<StorageFaultPlan> fault) -> AtomicDurabilityRun<HardState> {
    const auto suffix = fault.has_value() ? fault->Name() : "complete";
    const auto directory =
        std::filesystem::temp_directory_path() / ("bustub-stable-power-loss-" + std::to_string(getpid()) + suffix);
    auto storage = std::make_shared<PowerLossStorage>(directory);
    storage->RemoveTree(directory);
    auto store = StableStore::Open(directory, storage);
    store->Update(1, NodeId{1}, 0);
    storage->ResetEventHistory();
    if (fault.has_value()) {
      storage->FailAt(*fault);
    }
    try {
      store->Update(2, NodeId{2}, 1);
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
    auto recovered = StableStore::Open(directory, storage);
    const auto recovered_state = recovered->State();
    recovered.reset();
    storage->RemoveTree(directory);
    return {recovered_state, events, fault_triggered};
  };

  const StorageEventTopology expected_events{
      {StorageFaultPoint::BEFORE_WRITE, 1, "HARD_STATE.tmp", {}},
      {StorageFaultPoint::AFTER_FSYNC, 1, "HARD_STATE.tmp", {}},
      {StorageFaultPoint::AFTER_RENAME, 1, "HARD_STATE", "HARD_STATE.tmp"},
      {StorageFaultPoint::AFTER_DIR_FSYNC, 1, ".", {}},
  };
  EXPECT_NO_THROW(VerifyAtomicDurableTransition(old_state, new_state, expected_events, run));
}

}  // namespace bustub
