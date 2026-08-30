//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// power_loss_storage_test.cpp
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include <filesystem>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "power_loss_storage.h"  // NOLINT(build/include_subdir)

namespace bustub {
namespace {

auto Bytes(std::initializer_list<unsigned char> values) -> std::vector<std::byte> {
  std::vector<std::byte> result;
  result.reserve(values.size());
  for (const auto value : values) {
    result.push_back(static_cast<std::byte>(value));
  }
  return result;
}

}  // namespace

TEST(PowerLossStorageTest, NewNameNeedsDirectorySyncButExistingFileFsyncSurvives) {
  const auto root =
      std::filesystem::temp_directory_path() / ("bustub-power-loss-file-contract-" + std::to_string(getpid()));
  PowerLossStorage storage(root);
  storage.RemoveTree(root);
  storage.CreateDirectories(root);
  storage.SyncDirectory(root);

  const auto new_file = root / "new.bin";
  storage.WriteFile(new_file, Bytes({1, 2, 3}));
  storage.SyncFile(new_file);
  storage.PowerLoss();
  EXPECT_FALSE(storage.Exists(new_file));

  const auto existing_file = root / "existing.bin";
  const auto old_bytes = Bytes({10, 11});
  const auto new_bytes = Bytes({20, 21, 22});
  storage.WriteFile(existing_file, old_bytes);
  storage.SyncFile(existing_file);
  storage.SyncDirectory(root);
  storage.WriteFile(existing_file, new_bytes);
  storage.SyncFile(existing_file);
  storage.PowerLoss();
  ASSERT_TRUE(storage.Exists(existing_file));
  EXPECT_EQ(storage.ReadFile(existing_file, 32), new_bytes);
  storage.RemoveTree(root);
}

TEST(PowerLossStorageTest, RenameRequiresDirectorySyncToReplaceDurableImage) {
  const auto root =
      std::filesystem::temp_directory_path() / ("bustub-power-loss-rename-contract-" + std::to_string(getpid()));
  PowerLossStorage storage(root);
  storage.RemoveTree(root);
  storage.CreateDirectories(root);
  const auto current = root / "CURRENT";
  const auto temporary = root / "CURRENT.tmp";
  const auto old_bytes = Bytes({1, 1, 1});
  const auto new_bytes = Bytes({2, 2, 2, 2});
  storage.WriteFile(current, old_bytes);
  storage.SyncFile(current);
  storage.SyncDirectory(root);

  storage.FailAt({StorageFaultPoint::AFTER_RENAME, 1});
  storage.WriteFile(temporary, new_bytes);
  storage.SyncFile(temporary);
  EXPECT_THROW(storage.Rename(temporary, current), std::runtime_error);
  EXPECT_TRUE(storage.FaultTriggered());
  storage.PowerLoss();
  ASSERT_TRUE(storage.Exists(current));
  EXPECT_EQ(storage.ReadFile(current, 32), old_bytes);
  EXPECT_FALSE(storage.Exists(temporary));

  storage.ResetEventHistory();
  storage.WriteFile(temporary, new_bytes);
  storage.SyncFile(temporary);
  storage.Rename(temporary, current);
  storage.SyncDirectory(root);
  storage.PowerLoss();
  ASSERT_TRUE(storage.Exists(current));
  EXPECT_EQ(storage.ReadFile(current, 32), new_bytes);
  EXPECT_FALSE(storage.Exists(temporary));
  storage.RemoveTree(root);
}

TEST(PowerLossStorageTest, DirectorySyncDoesNotPublishSiblingEntries) {
  const auto root =
      std::filesystem::temp_directory_path() / ("bustub-power-loss-directory-isolation-" + std::to_string(getpid()));
  PowerLossStorage storage(root);
  storage.RemoveTree(root);
  const auto left = root / "left";
  const auto right = root / "right";
  storage.CreateDirectories(left);
  storage.CreateDirectories(right);
  storage.SyncDirectory(root);

  const auto kept = right / "kept.bin";
  storage.WriteFile(kept, Bytes({1, 2}));
  storage.SyncFile(kept);
  storage.SyncDirectory(right);
  storage.WriteFile(kept, Bytes({8, 9, 10}));
  storage.SyncFile(kept);

  const auto sibling_pending = right / "pending.bin";
  storage.WriteFile(sibling_pending, Bytes({3, 4, 5}));
  storage.SyncFile(sibling_pending);
  const auto left_committed = left / "committed.bin";
  storage.WriteFile(left_committed, Bytes({6, 7}));
  storage.SyncFile(left_committed);
  storage.SyncDirectory(left);
  storage.PowerLoss();

  ASSERT_TRUE(storage.Exists(left_committed));
  EXPECT_EQ(storage.ReadFile(left_committed, 32), Bytes({6, 7}));
  ASSERT_TRUE(storage.Exists(kept));
  EXPECT_EQ(storage.ReadFile(kept, 32), Bytes({8, 9, 10}));
  EXPECT_FALSE(storage.Exists(sibling_pending));
  storage.RemoveTree(root);
}

TEST(PowerLossStorageTest, RecordsLiteralNamedEventTopology) {
  const auto root =
      std::filesystem::temp_directory_path() / ("bustub-power-loss-event-contract-" + std::to_string(getpid()));
  PowerLossStorage storage(root);
  storage.RemoveTree(root);
  storage.CreateDirectories(root);
  storage.SyncDirectory(root);
  storage.ResetEventHistory();

  const auto temporary = root / "MANIFEST.tmp";
  const auto formal = root / "MANIFEST";
  storage.WriteFile(temporary, Bytes({7, 8}));
  storage.SyncFile(temporary);
  storage.Rename(temporary, formal);
  storage.SyncDirectory(root);

  const auto &events = storage.Events();
  ASSERT_EQ(events.size(), 4);
  EXPECT_EQ(events[0].point_, StorageFaultPoint::BEFORE_WRITE);
  EXPECT_EQ(events[0].occurrence_, 1);
  EXPECT_EQ(events[0].path_, std::filesystem::path("MANIFEST.tmp"));
  EXPECT_TRUE(events[0].related_path_.empty());
  EXPECT_EQ(events[1].point_, StorageFaultPoint::AFTER_FSYNC);
  EXPECT_EQ(events[1].occurrence_, 1);
  EXPECT_EQ(events[1].path_, std::filesystem::path("MANIFEST.tmp"));
  EXPECT_EQ(events[2].point_, StorageFaultPoint::AFTER_RENAME);
  EXPECT_EQ(events[2].occurrence_, 1);
  EXPECT_EQ(events[2].path_, std::filesystem::path("MANIFEST"));
  EXPECT_EQ(events[2].related_path_, std::filesystem::path("MANIFEST.tmp"));
  EXPECT_EQ(events[3].point_, StorageFaultPoint::AFTER_DIR_FSYNC);
  EXPECT_EQ(events[3].occurrence_, 1);
  EXPECT_EQ(events[3].path_, std::filesystem::path("."));
  storage.RemoveTree(root);
}

// DurableStorage exposes whole write/append operations. This model deliberately does not claim to simulate a
// short write or torn sector inside one operation; byte-level corruption is covered by the format-specific tests.

}  // namespace bustub
