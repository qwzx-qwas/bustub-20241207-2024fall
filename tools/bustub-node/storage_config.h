// Private command-line deployment adapter. Storage ownership remains in NodeStorage.
#pragma once
#include <filesystem>
#include "distributed/node.h"
namespace bustub::cli {
struct StorageConfig {
  RaftObjectDeployment deployment_;
  BootstrapLayout layout_;
};
auto ReadStorageConfig(const std::filesystem::path &path) -> StorageConfig;
void InitializeStorage(const StorageConfig &storage, const DistributedNodeConfig &node);
class DeviceLock {
 public:
  explicit DeviceLock(const std::filesystem::path &path);
  ~DeviceLock();
  DeviceLock(const DeviceLock &) = delete;
  auto operator=(const DeviceLock &) -> DeviceLock & = delete;
 private:
  int fd_;
};
}  // namespace bustub::cli
