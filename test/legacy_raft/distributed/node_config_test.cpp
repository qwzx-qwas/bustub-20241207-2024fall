//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// node_config_test.cpp
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include <filesystem>
#include <memory>

#include "distributed/node.h"
#include "gtest/gtest.h"
#include "recovery/node_directory.h"

namespace bustub {
namespace {

auto ValidConfig() -> DistributedNodeConfig {
  return {1,
          "demo",
          "/tmp/bustub-node-config-test",
          {"127.0.0.1", 7101},
          {"127.0.0.1", 7201},
          {{2, {{"127.0.0.1", 7102}, {"127.0.0.1", 7202}}}, {3, {{"127.0.0.1", 7103}, {"127.0.0.1", 7203}}}},
          300,
          600,
          50,
          10,
          5000,
          128,
          10000};
}

}  // namespace

TEST(DistributedNodeConfigTest, StaticThreeMemberConfigurationIsFailClosed) {
  auto config = ValidConfig();
  EXPECT_NO_THROW(config.Validate());
  config.group_id_.clear();
  EXPECT_THROW(config.Validate(), std::runtime_error);
  config = ValidConfig();
  config.peers_.at(3).raft_endpoint_ = config.peers_.at(2).raft_endpoint_;
  EXPECT_THROW(config.Validate(), std::runtime_error);
  config = ValidConfig();
  config.peers_.erase(3);
  EXPECT_THROW(config.Validate(), std::runtime_error);
  config = ValidConfig();
  config.election_timeout_min_ms_ = 100;
  EXPECT_THROW(config.Validate(), std::runtime_error);
  config = ValidConfig();
  config.election_timeout_max_ms_ = config.election_timeout_min_ms_;
  EXPECT_THROW(config.Validate(), std::runtime_error);
  config = ValidConfig();
  config.snapshot_threshold_entries_ = 0;
  EXPECT_THROW(config.Validate(), std::runtime_error);
}

TEST(DistributedNodeConfigTest, NodeDirectoryIdentityIsDurableAndImmutable) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto root = std::filesystem::temp_directory_path() / ("bustub-node-identity-" + std::to_string(getpid()));
  storage->RemoveTree(root);

  {
    auto directory = NodeDirectory::Open(root, storage);
    EXPECT_NO_THROW(directory->EnsureIdentity(1, "demo", {1, 2, 3}));
    EXPECT_TRUE(storage->Exists(root / "node.conf"));
    EXPECT_FALSE(storage->Exists(root / "node.conf.tmp"));
  }
  {
    auto directory = NodeDirectory::Open(root, storage);
    EXPECT_NO_THROW(directory->EnsureIdentity(1, "demo", {1, 2, 3}));
    EXPECT_THROW(directory->EnsureIdentity(2, "demo", {1, 2, 3}), std::runtime_error);
    EXPECT_THROW(directory->EnsureIdentity(1, "other", {1, 2, 3}), std::runtime_error);
    EXPECT_THROW(directory->EnsureIdentity(1, "demo", {1, 2, 4}), std::runtime_error);
  }

  auto bytes = storage->ReadFile(root / "node.conf", 4096);
  bytes.back() ^= std::byte{1};
  storage->WriteFile(root / "node.conf", bytes);
  storage->SyncFile(root / "node.conf");
  {
    auto directory = NodeDirectory::Open(root, storage);
    EXPECT_THROW(directory->EnsureIdentity(1, "demo", {1, 2, 3}), std::runtime_error);
  }
  storage->RemoveTree(root);
}

TEST(DistributedNodeConfigTest, ProductionOpenRejectsIdentityDriftBeforeStart) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto root = std::filesystem::temp_directory_path() / ("bustub-node-open-identity-" + std::to_string(getpid()));
  storage->RemoveTree(root);

  auto original = ValidConfig();
  original.data_directory_ = root;
  EXPECT_NO_THROW(DistributedNode::Open(original, storage));
  const auto identity = storage->ReadFile(root / "node.conf", 4096);

  auto changed_node = original;
  changed_node.node_id_ = 2;
  changed_node.raft_listen_ = {"127.0.0.1", 7102};
  changed_node.client_listen_ = {"127.0.0.1", 7202};
  changed_node.peers_ = {{1, {{"127.0.0.1", 7101}, {"127.0.0.1", 7201}}},
                         {3, {{"127.0.0.1", 7103}, {"127.0.0.1", 7203}}}};
  EXPECT_THROW(DistributedNode::Open(changed_node, storage), std::runtime_error);

  auto changed_group = original;
  changed_group.group_id_ = "other";
  EXPECT_THROW(DistributedNode::Open(changed_group, storage), std::runtime_error);

  auto changed_voters = original;
  changed_voters.peers_.erase(3);
  changed_voters.peers_.emplace(4, DistributedPeerConfig{{"127.0.0.1", 7104}, {"127.0.0.1", 7204}});
  EXPECT_THROW(DistributedNode::Open(changed_voters, storage), std::runtime_error);

  EXPECT_EQ(storage->ReadFile(root / "node.conf", 4096), identity);
  EXPECT_NO_THROW(DistributedNode::Open(original, storage));
  storage->RemoveTree(root);
}

}  // namespace bustub
