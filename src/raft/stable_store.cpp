//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// stable_store.cpp
//
//===----------------------------------------------------------------------===//

#include "raft/stable_store.h"

#include <array>
#include <stdexcept>

#include "common/byte_codec.h"

namespace bustub {
namespace {

constexpr std::array<std::byte, 8> HARD_STATE_MAGIC{std::byte{'B'}, std::byte{'S'}, std::byte{'T'}, std::byte{'H'},
                                                    std::byte{'A'}, std::byte{'R'}, std::byte{'D'}, std::byte{'1'}};

}  // namespace

auto HardStateCodec::Encode(const HardState &state) -> std::vector<std::byte> {
  if (state.format_version_ != FORMAT_VERSION || (state.current_term_ == 0 && state.voted_for_.has_value()) ||
      (state.voted_for_.has_value() && *state.voted_for_ == 0)) {
    throw std::runtime_error("invalid Raft HardState");
  }
  ByteWriter payload;
  payload.PutU64(state.generation_);
  payload.PutU64(state.current_term_);
  payload.PutU8(state.voted_for_.has_value() ? 1 : 0);
  payload.PutU64(state.voted_for_.value_or(0));
  payload.PutU64(state.commit_index_);

  return EncodeVersionedFrame({HARD_STATE_MAGIC.data(), HARD_STATE_MAGIC.size(), FORMAT_VERSION, 1004, "HARD_STATE"},
                              payload.Data());
}

auto HardStateCodec::Decode(const std::vector<std::byte> &bytes) -> HardState {
  const auto payload = DecodeVersionedFrame(
      {HARD_STATE_MAGIC.data(), HARD_STATE_MAGIC.size(), FORMAT_VERSION, 1004, "HARD_STATE"}, bytes);
  ByteReader body(payload);
  HardState state;
  state.format_version_ = FORMAT_VERSION;
  state.generation_ = body.ReadU64();
  state.current_term_ = body.ReadU64();
  const auto has_vote = body.ReadU8();
  const auto vote = body.ReadU64();
  state.commit_index_ = body.ReadU64();
  if (!body.Empty() || has_vote > 1 || (has_vote == 0 && vote != 0)) {
    throw std::runtime_error("invalid HARD_STATE fields");
  }
  if (has_vote == 1) {
    state.voted_for_ = vote;
  }
  static_cast<void>(Encode(state));
  return state;
}

auto StableStore::Open(std::filesystem::path raft_directory, std::shared_ptr<DurableStorage> storage)
    -> std::unique_ptr<StableStore> {
  if (raft_directory.empty() || storage == nullptr) {
    throw std::runtime_error("invalid StableStore configuration");
  }
  storage->CreateDirectories(raft_directory);
  HardState state;
  const auto path = raft_directory / "HARD_STATE";
  if (storage->Exists(path)) {
    state = HardStateCodec::Decode(storage->ReadFile(path, 1024));
    if (state.generation_ == 0) {
      throw std::runtime_error("formal HARD_STATE must have a positive generation");
    }
  }
  // A leftover .tmp was never externally acknowledged and is intentionally ignored.
  return std::unique_ptr<StableStore>(new StableStore(std::move(raft_directory), std::move(storage), state));
}

auto StableStore::State() const -> HardState {
  std::lock_guard lock(mutex_);
  return state_;
}

void StableStore::Update(uint64_t current_term, std::optional<NodeId> voted_for, uint64_t commit_index) {
  std::lock_guard lock(mutex_);
  if (current_term < state_.current_term_ || commit_index < state_.commit_index_ ||
      (current_term == state_.current_term_ && state_.voted_for_.has_value() && voted_for != state_.voted_for_) ||
      (voted_for.has_value() && *voted_for == 0)) {
    throw std::runtime_error("non-monotonic or conflicting HARD_STATE update");
  }
  HardState next{1, state_.generation_ + 1, current_term, voted_for, commit_index};
  const auto temporary = raft_directory_ / "HARD_STATE.tmp";
  const auto formal = raft_directory_ / "HARD_STATE";
  storage_->WriteFile(temporary, HardStateCodec::Encode(next));
  storage_->SyncFile(temporary);
  storage_->Rename(temporary, formal);
  storage_->SyncDirectory(raft_directory_);
  state_ = next;
}

}  // namespace bustub
