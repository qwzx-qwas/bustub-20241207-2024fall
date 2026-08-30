//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// node_directory.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "recovery/durable_storage.h"

namespace bustub {

/** Owns the process-wide exclusive LOCK for one node data directory. */
class NodeDirectory {
 public:
  static auto Open(std::filesystem::path root, std::shared_ptr<DurableStorage> storage)
      -> std::unique_ptr<NodeDirectory>;

  ~NodeDirectory();
  NodeDirectory(const NodeDirectory &) = delete;
  auto operator=(const NodeDirectory &) -> NodeDirectory & = delete;

  auto Root() const -> const std::filesystem::path & { return root_; }
  auto RaftDirectory() const -> std::filesystem::path { return root_ / "raft"; }
  auto LogDirectory() const -> std::filesystem::path { return root_ / "raft" / "log"; }
  auto StateDirectory() const -> std::filesystem::path { return root_ / "state"; }
  auto WorkingDirectory() const -> std::filesystem::path { return root_ / "working"; }
  auto WorkingDatabase() const -> std::filesystem::path { return WorkingDirectory() / "db.bustub"; }

  /**
   * Creates the immutable node.conf identity on first use and fail-closes if a
   * later process attempts to reuse this directory for another Raft member.
   * Network endpoints are deliberately excluded so operators may move a node
   * without changing its logical identity.
   */
  void EnsureIdentity(uint64_t node_id, const std::string &group_id, const std::vector<uint64_t> &voters);

 private:
  NodeDirectory(std::filesystem::path root, std::shared_ptr<DurableStorage> storage, int lock_fd)
      : root_(std::move(root)), storage_(std::move(storage)), lock_fd_(lock_fd) {}

  std::filesystem::path root_;
  std::shared_ptr<DurableStorage> storage_;
  int lock_fd_{-1};
};

}  // namespace bustub
