//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// single_node_runtime.cpp
//
//===----------------------------------------------------------------------===//

#include "recovery/single_node_runtime.h"

#include <algorithm>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string_view>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "storage/disk/disk_manager.h"

namespace bustub {
namespace {

auto SnapshotDirectoryName(uint64_t generation) -> std::string {
  std::ostringstream output;
  output << "SNAPSHOT-" << std::setw(20) << std::setfill('0') << generation;
  return output.str();
}

auto IsFormalStateName(std::string_view name) -> bool {
  return name == "CURRENT" || name.compare(0, 9, "MANIFEST-") == 0 || name.compare(0, 9, "SNAPSHOT-") == 0;
}

}  // namespace

auto SingleNodeCommandRuntime::Open(const std::filesystem::path &root, std::shared_ptr<DurableStorage> storage,
                                    SingleNodeRuntimeOptions options) -> std::unique_ptr<SingleNodeCommandRuntime> {
  if (options.buffer_pool_size_ == 0 || options.log_options_.segment_max_bytes_ < LogCodec::FRAME_BODY_FIXED_BYTES) {
    throw std::runtime_error("invalid single-node runtime options");
  }
  if (storage == nullptr) {
    storage = std::make_shared<PosixDurableStorage>();
  }
  auto node_directory = NodeDirectory::Open(root, storage);
  auto runtime = std::unique_ptr<SingleNodeCommandRuntime>(
      new SingleNodeCommandRuntime(std::move(storage), std::move(node_directory), options));
  runtime->RecoverOrBootstrap();
  return runtime;
}

SingleNodeCommandRuntime::SingleNodeCommandRuntime(std::shared_ptr<DurableStorage> storage,
                                                   std::unique_ptr<NodeDirectory> node_directory,
                                                   SingleNodeRuntimeOptions options)
    : storage_(std::move(storage)), node_directory_(std::move(node_directory)), options_(options) {
  stable_store_ = StableStore::Open(node_directory_->RaftDirectory(), storage_);
  snapshot_manager_ = std::make_unique<SnapshotManager>(node_directory_.get(), storage_);
}

void SingleNodeCommandRuntime::BootstrapEmptySnapshot() {
  {
    DiskManager disk(node_directory_->WorkingDatabase());
    BufferPoolManager buffer_pool(options_.buffer_pool_size_, &disk);
    Catalog catalog(&buffer_pool, nullptr, nullptr);
    SessionTable sessions;
    snapshot_manager_->CreateSnapshot(catalog, sessions, &visibility_, 1, 0, 0);
    buffer_pool.FlushAllPages();
    disk.ShutDown();
  }
}

auto SingleNodeCommandRuntime::HasFormalRecoveryState() const -> bool {
  const auto hard_state = stable_store_->State();
  if (hard_state.generation_ != 0 || hard_state.commit_index_ != 0 || hard_state.current_term_ != 0 ||
      hard_state.voted_for_.has_value()) {
    return true;
  }
  for (const auto &item : std::filesystem::directory_iterator(node_directory_->StateDirectory())) {
    if (IsFormalStateName(item.path().filename().string())) {
      return true;
    }
  }
  return std::any_of(std::filesystem::directory_iterator(node_directory_->LogDirectory()),
                     std::filesystem::directory_iterator(), [](const auto &item) {
                       return item.is_regular_file() && item.path().filename().string().compare(0, 4, "LOG-") == 0;
                     });
}

auto SingleNodeCommandRuntime::HasBridgeLog(uint64_t snapshot_index) const -> bool {
  try {
    const auto hard_state = stable_store_->State();
    const auto effective_commit = std::max(hard_state.commit_index_, snapshot_index);
    static_cast<void>(CommandLog::Open(node_directory_->LogDirectory(), storage_, effective_commit, snapshot_index, 0,
                                       options_.log_options_));
    return true;
  } catch (const std::exception &) {
    return false;
  }
}

void SingleNodeCommandRuntime::RecoverOrBootstrap() {
  const auto initial_hard_state = stable_store_->State();
  if (initial_hard_state.current_term_ != 0 || initial_hard_state.voted_for_.has_value()) {
    throw std::runtime_error("single-node term-0 runtime cannot open Raft cluster state");
  }

  recovered_ = snapshot_manager_->Recover([this](uint64_t snapshot_index) { return HasBridgeLog(snapshot_index); },
                                          options_.buffer_pool_size_);
  if (recovered_ == nullptr) {
    if (HasFormalRecoveryState()) {
      throw std::runtime_error("no validated snapshot and bridge-log recovery point is available");
    }
    BootstrapEmptySnapshot();
    recovered_ = snapshot_manager_->Recover([this](uint64_t snapshot_index) { return HasBridgeLog(snapshot_index); },
                                            options_.buffer_pool_size_);
    if (recovered_ == nullptr) {
      throw std::runtime_error("failed to recover the initial empty snapshot");
    }
  }
  if (recovered_->manifest_.last_included_term_ != 0) {
    throw std::runtime_error("single-node recovery selected a nonzero-term snapshot");
  }

  const auto snapshot_index = recovered_->manifest_.last_included_index_;
  const auto effective_commit = std::max(stable_store_->State().commit_index_, snapshot_index);
  command_log_ = CommandLog::Open(node_directory_->LogDirectory(), storage_, effective_commit, snapshot_index, 0,
                                  options_.log_options_);
  // A complete entry beyond HARD_STATE was never durably committed. It cannot be reused for a later request.
  command_log_->TruncateSuffix(effective_commit);
  if (stable_store_->State().commit_index_ < effective_commit) {
    stable_store_->Update(0, std::nullopt, effective_commit);
  }

  state_machine_ = std::make_unique<BusTubStateMachine>(recovered_->catalog_.get(), recovered_->sessions_.get(),
                                                        &visibility_, snapshot_index);
  if (effective_commit > snapshot_index) {
    for (const auto &entry : command_log_->Entries(snapshot_index + 1, effective_commit)) {
      state_machine_->Apply(entry);
    }
  }
  if (state_machine_->LastApplied() != effective_commit ||
      state_machine_->PublishedAppliedIndex() != effective_commit || command_log_->LastLogIndex() != effective_commit) {
    throw std::runtime_error("single-node recovery did not converge at the durable commit index");
  }
  recovered_->last_applied_ = effective_commit;
  recovered_->published_applied_index_ = effective_commit;
}

auto SingleNodeCommandRuntime::Commit(const TransactionCommandBatch &batch) -> std::vector<std::byte> {
  std::lock_guard write_lock(write_mutex_);
  return CommitLocked(batch);
}

auto SingleNodeCommandRuntime::CommitLocked(const TransactionCommandBatch &batch) -> std::vector<std::byte> {
  {
    auto visible = visibility_.LockShared();
    const auto disposition =
        recovered_->sessions_->Classify(batch.client_id_, batch.request_id_, batch.request_fingerprint_);
    if (disposition == RequestDisposition::RETRY_LAST) {
      const auto response = recovered_->sessions_->GetLastResponse(batch.client_id_);
      if (!response.has_value()) {
        throw std::runtime_error("retry session has no committed response");
      }
      return *response;
    }
    if (disposition == RequestDisposition::PAYLOAD_MISMATCH) {
      throw std::runtime_error("request payload does not match request identity");
    }
    if (disposition == RequestDisposition::TOO_OLD) {
      throw std::runtime_error("request id is older than the retained session response");
    }
    if (disposition == RequestDisposition::GAP) {
      throw std::runtime_error("request id contains a session sequence gap");
    }
  }

  state_machine_->ValidateProposal(batch);
  const auto hard_state = stable_store_->State();
  if (hard_state.current_term_ != 0 || hard_state.voted_for_.has_value() ||
      command_log_->LastLogIndex() != hard_state.commit_index_ ||
      state_machine_->LastApplied() != hard_state.commit_index_ ||
      state_machine_->PublishedAppliedIndex() != hard_state.commit_index_) {
    throw std::runtime_error("single-node writer state is not aligned at the commit boundary");
  }
  const auto index = hard_state.commit_index_ + 1;
  ReplicatedLogEntry entry{1, index, 0, EntryType::COMMAND_BATCH, CommandBatchCodec::Encode(batch)};
  // M2 durability order: log bytes -> durable commit point -> one atomic-visible FSM Apply -> client response.
  command_log_->Append({entry});
  stable_store_->Update(0, std::nullopt, index);
  state_machine_->Apply(entry);
  recovered_->last_applied_ = index;
  recovered_->published_applied_index_ = index;
  const auto response = state_machine_->GetLastResponse(batch.client_id_);
  if (!response.has_value()) {
    throw std::runtime_error("committed command did not publish a session response");
  }
  return *response;
}

auto SingleNodeCommandRuntime::NextSnapshotGeneration() const -> uint64_t {
  auto generation = recovered_->manifest_.generation_ + 1;
  while (storage_->Exists(node_directory_->StateDirectory() / StateManifestStore::ManifestFileName(generation)) ||
         storage_->Exists(node_directory_->StateDirectory() / SnapshotDirectoryName(generation)) ||
         storage_->Exists(node_directory_->StateDirectory() / (SnapshotDirectoryName(generation) + ".tmp"))) {
    generation++;
  }
  return generation;
}

auto SingleNodeCommandRuntime::CreateSnapshot() -> StateManifest {
  std::lock_guard write_lock(write_mutex_);
  const auto commit_index = stable_store_->State().commit_index_;
  if (state_machine_->IsStopped() || state_machine_->LastApplied() != commit_index ||
      state_machine_->PublishedAppliedIndex() != commit_index || command_log_->LastLogIndex() != commit_index ||
      command_log_->TermAt(commit_index) != std::optional<uint64_t>{0}) {
    throw std::runtime_error("single-node snapshot boundary is not fully published");
  }
  auto manifest = snapshot_manager_->CreateSnapshot(*recovered_->catalog_, *recovered_->sessions_, &visibility_,
                                                    NextSnapshotGeneration(), commit_index, 0);
  command_log_ =
      CommandLog::Open(node_directory_->LogDirectory(), storage_, commit_index, commit_index, 0, options_.log_options_);
  recovered_->manifest_ = manifest;
  recovered_->last_applied_ = commit_index;
  recovered_->published_applied_index_ = commit_index;
  snapshot_manager_->PruneToTwo([this](uint64_t snapshot_index) { return HasBridgeLog(snapshot_index); },
                                [this](uint64_t oldest_boundary) { command_log_->CompactPrefix(oldest_boundary); });
  return manifest;
}

auto SingleNodeCommandRuntime::GetRow(table_oid_t table_oid, const EncodedPrimaryKeyV1 &primary_key) const
    -> std::optional<std::pair<TupleMeta, Tuple>> {
  return state_machine_->GetRow(table_oid, primary_key);
}

auto SingleNodeCommandRuntime::GetLastResponse(uint64_t client_id) const -> std::optional<std::vector<std::byte>> {
  return state_machine_->GetLastResponse(client_id);
}

}  // namespace bustub
