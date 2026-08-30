//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// stable_store.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <filesystem>
#include <memory>
#include <mutex>  // NOLINT(build/c++11)
#include <optional>
#include <utility>
#include <vector>

#include "raft/raft_types.h"
#include "recovery/durable_storage.h"

namespace bustub {

class HardStateCodec {
 public:
  static constexpr uint32_t FORMAT_VERSION = 1;
  static auto Encode(const HardState &state) -> std::vector<std::byte>;
  static auto Decode(const std::vector<std::byte> &bytes) -> HardState;
};

/** Atomic durable Raft term/vote/commit state. All updates are serialized. */
class StableStore {
 public:
  static auto Open(std::filesystem::path raft_directory, std::shared_ptr<DurableStorage> storage)
      -> std::unique_ptr<StableStore>;

  auto State() const -> HardState;
  /** Returns only after the new HardState file and its parent directory are durable. */
  void Update(uint64_t current_term, std::optional<NodeId> voted_for, uint64_t commit_index);

 private:
  StableStore(std::filesystem::path raft_directory, std::shared_ptr<DurableStorage> storage, HardState state)
      : raft_directory_(std::move(raft_directory)), storage_(std::move(storage)), state_(std::move(state)) {}

  std::filesystem::path raft_directory_;
  std::shared_ptr<DurableStorage> storage_;
  mutable std::mutex mutex_;
  HardState state_;
};

}  // namespace bustub
