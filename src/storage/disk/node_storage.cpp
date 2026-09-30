//===----------------------------------------------------------------------===//
// BusTub: F34 lifecycle ownership; F33 short, serialized state decisions.
//===----------------------------------------------------------------------===//
#include "storage/disk/node_storage.h"

#include <condition_variable>  // NOLINT(build/c++11)
#include <mutex>               // NOLINT(build/c++11)
#include <utility>

namespace bustub {
namespace {

// Only the lifecycle owner calls this controller, under its short state lock.
// No IO, callbacks, background thread or per-IO event queue is owned here.
class NodeStateController {
 public:
  auto Begin() -> uint64_t {
    const auto next = view_.generation_ + 1;
    view_ = {};
    view_.generation_ = next;
    view_.phase_ = NodeStoragePhase::Starting;
    return next;
  }
  auto Phase(uint64_t generation, NodeStoragePhase phase) -> bool {
    if (generation != view_.generation_) {
      return false;
    }
    view_.phase_ = phase;
    return true;
  }
  void Drain() {
    ++view_.generation_;
    view_.phase_ = NodeStoragePhase::Draining;
  }
  void Stop(std::exception_ptr error) {
    view_.phase_ = NodeStoragePhase::Stopped;
    if (error) {
      Set(NodeStorageCondition::IOFault, true);
      if (!view_.error_) {
        view_.error_ = error;
      }
    }
  }
  void Fail(uint64_t generation, NodeStorageCondition condition, std::exception_ptr error) {
    const bool draining = view_.phase_ == NodeStoragePhase::Draining && generation + 1 == view_.generation_;
    if (generation != view_.generation_ && !draining) {
      return;
    }
    Set(condition, true);
    if (!view_.error_) {
      view_.error_ = error;
    }
    if (!draining) {
      view_.phase_ = NodeStoragePhase::Failed;
    }
  }
  void Pressure(uint64_t generation, bool pressure) {
    if (generation == view_.generation_ && view_.phase_ == NodeStoragePhase::Serving) {
      Set(NodeStorageCondition::ResourcePressure, pressure);
    }
  }
  void Repair(const BootstrapStatus &status) {
    Set(NodeStorageCondition::BootstrapRedundancyLost, status.redundancy_lost_);
    Set(NodeStorageCondition::BootstrapRepairFailed, status.repair_state_ == BootstrapRepairState::Failed);
    view_.repair_error_ = status.last_error_;
  }
  auto View() const -> NodeStorageView {
    auto result = view_;
    result.metadata_read_ = result.metadata_write_ = result.metadata_maintenance_ =
        result.phase_ == NodeStoragePhase::Serving;
    for (auto condition : {NodeStorageCondition::RecoveryFailed, NodeStorageCondition::IOFault,
                           NodeStorageCondition::BootstrapRepairFailed, NodeStorageCondition::ResourcePressure,
                           NodeStorageCondition::BootstrapRedundancyLost}) {
      if ((result.conditions_ & static_cast<uint32_t>(condition)) != 0) {
        result.primary_condition_ = condition;
        break;
      }
    }
    return result;
  }

 private:
  void Set(NodeStorageCondition condition, bool present) {
    const auto bit = static_cast<uint32_t>(condition);
    view_.conditions_ = present ? view_.conditions_ | bit : view_.conditions_ & ~bit;
  }
  NodeStorageView view_;
};

struct StorageContext {
  // Declared in dependency order; member destruction reverses it.
  std::unique_ptr<BlockDevice> device_;
  std::unique_ptr<IOExecutor> executor_;
  std::unique_ptr<BootstrapStore> bootstrap_;
  std::unique_ptr<MetadataEngine> metadata_;
  std::exception_ptr repair_start_error_;

  ~StorageContext() {
    try {
      Close();
    } catch (...) {
      // Explicit Close reports errors; destructors cannot propagate them.
    }
  }
  auto RepairStatus() const -> BootstrapStatus {
    auto result = bootstrap_->Status();
    if (repair_start_error_) {
      result.repair_state_ = BootstrapRepairState::Failed;
      result.last_error_ = repair_start_error_;
    }
    return result;
  }
  void Close() {
    std::exception_ptr first;
    auto close = [&first](auto action) {
      try {
        action();
      } catch (...) {
        if (!first) {
          first = std::current_exception();
        }
      }
    };
    if (metadata_) {
      close([&] { metadata_->Close(); });
    }
    if (bootstrap_) {
      close([&] { bootstrap_->Close(); });
    }
    if (executor_) {
      close([&] { executor_->Shutdown(); });
    }
    if (device_) {
      close([&] { device_->Close(); });
    }
    if (first) {
      std::rethrow_exception(first);
    }
  }
};

}  // namespace

struct NodeStorage::Impl {
  explicit Impl(NodeStorageOptions options) : options_(std::move(options)) {
    if (options_.repair_.max_attempts_ == 0 || options_.repair_.retry_delay_.count() <= 0 ||
        options_.repair_.admission_timeout_.count() <= 0) {
      throw std::invalid_argument("node storage repair requires positive attempt and wait limits");
    }
  }

  // Owns admission/lifetime, not another copy of B's transaction protocol.
  struct Call {
    Impl &owner_;
    std::shared_ptr<StorageContext> context_;
    uint64_t generation_;
    ~Call() { owner_.Finish(); }
  };
  auto Acquire() -> Call {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto state = state_.View();
    if (state.phase_ != NodeStoragePhase::Serving) {
      throw MetadataError(MetadataErrorCode::NotReady, "node storage is not serving metadata");
    }
    ++active_;
    return {*this, context_, state.generation_};
  }
  void Finish() {
    std::lock_guard<std::mutex> lock(mutex_);
    --active_;
    idle_.notify_all();
  }
  void Failure(uint64_t generation, std::exception_ptr error) {
    std::lock_guard<std::mutex> lock(mutex_);
    state_.Fail(generation, NodeStorageCondition::IOFault, error);
  }
  void Pressure(uint64_t generation, bool pressure) {
    std::lock_guard<std::mutex> lock(mutex_);
    state_.Pressure(generation, pressure);
  }
  void Begin(const BootstrapLayout *layout) {
    uint64_t generation;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (state_.View().phase_ != NodeStoragePhase::Stopped || closing_) {
        throw std::logic_error("node storage startup requires a closed instance");
      }
      generation = state_.Begin();
      ++active_;
    }
    Call startup{*this, nullptr, generation};
    std::shared_ptr<StorageContext> context;
    bool cancelled = false;
    try {
      context = std::make_shared<StorageContext>();
      context->device_ = std::make_unique<BlockDevice>(options_.device_path_, options_.device_);
      context->executor_ = std::make_unique<IOExecutor>(*context->device_, options_.io_);
      context->bootstrap_ = std::make_unique<BootstrapStore>(*context->executor_);
      if (layout != nullptr) {
        if (layout->identity_.storage_ != options_.identity_.storage_ ||
            layout->identity_.device_ != options_.identity_.device_) {
          throw std::invalid_argument("Create layout and node storage identity differ");
        }
        context->bootstrap_->Create(*layout);
      }
      context->bootstrap_->Open(options_.identity_);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!state_.Phase(generation, NodeStoragePhase::Recovering)) {
          cancelled = true;
          throw MetadataError(MetadataErrorCode::NotReady, "node storage startup was closed");
        }
        state_.Repair(context->RepairStatus());
      }
      context->metadata_ = std::make_unique<MetadataEngine>(*context->bootstrap_, options_.journal_identity_,
                                                            options_.journal_, options_.metadata_);
      if (layout != nullptr) {
        context->metadata_->Create();
      } else {
        context->metadata_->Open();
      }
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_.View().generation_ != generation) {
          cancelled = true;
          throw MetadataError(MetadataErrorCode::NotReady, "node storage startup was closed");
        }
        // Fresh F03 context: StartRepair only schedules its first bounded worker;
        // it cannot join an old worker or wait for IO here. Serialize that start
        // with Close so a cancelled recovery never launches new background work.
        try {
          context->bootstrap_->StartRepair(options_.repair_);
        } catch (...) {
          context->repair_start_error_ = std::current_exception();
        }
        state_.Phase(generation, NodeStoragePhase::Serving);
        state_.Repair(context->RepairStatus());
        context_ = std::move(context);
      }
    } catch (...) {
      const auto error = std::current_exception();
      // Partially opened contexts also close in dependency order, before the
      // startup call releases its admission slot and wakes Close.
      std::exception_ptr cleanup_error;
      if (context) {
        try {
          context->Close();
        } catch (...) {
          cleanup_error = std::current_exception();
        }
      }
      context.reset();
      std::lock_guard<std::mutex> lock(mutex_);
      if (!cancelled) {
        state_.Fail(generation, NodeStorageCondition::RecoveryFailed, error);
      }
      if (cleanup_error) {
        state_.Fail(generation, NodeStorageCondition::IOFault, cleanup_error);
        if (closing_) {
          close_error_ = cleanup_error;
        }
      }
      throw;
    }
  }
  auto State() -> NodeStorageView {
    std::lock_guard<std::mutex> lock(mutex_);
    if (context_) {
      // F03 Status only takes its short status lock, never waits for disk IO.
      state_.Repair(context_->RepairStatus());
    }
    return state_.View();
  }
  void Close() {
    std::shared_ptr<StorageContext> context;
    std::exception_ptr error;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      if (closing_) {
        idle_.wait(lock, [&] { return !closing_; });
        if (close_error_) {
          std::rethrow_exception(close_error_);
        }
        return;
      }
      if (state_.View().phase_ == NodeStoragePhase::Stopped) {
        return;
      }
      closing_ = true;
      close_error_ = nullptr;
      state_.Drain();
      idle_.wait(lock, [&] { return active_ == 0; });
      context = context_;
      error = close_error_;
    }
    if (context) {
      try {
        context->Close();
      } catch (...) {
        if (!error) {
          error = std::current_exception();
        }
      }
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      // Repair may finish or fail while Close is draining it. Preserve that
      // final status before releasing its only source; no IO runs under this lock.
      if (context) {
        state_.Repair(context->RepairStatus());
      }
      context_.reset();
      state_.Stop(error);
      close_error_ = error;
      closing_ = false;
      idle_.notify_all();
    }
    if (error) {
      std::rethrow_exception(error);
    }
  }

  const NodeStorageOptions options_;
  std::mutex mutex_;
  std::condition_variable idle_;
  NodeStateController state_;
  std::shared_ptr<StorageContext> context_;
  size_t active_{0};
  bool closing_{false};
  std::exception_ptr close_error_;
};

NodeStorage::NodeStorage(NodeStorageOptions options) : impl_(std::make_unique<Impl>(std::move(options))) {}
NodeStorage::~NodeStorage() {
  try {
    Close();
  } catch (...) {
    // Explicit Close is the error-reporting boundary.
  }
}
void NodeStorage::Create(const BootstrapLayout &layout) { impl_->Begin(&layout); }
void NodeStorage::Open() { impl_->Begin(nullptr); }
auto NodeStorage::State() const -> NodeStorageView { return impl_->State(); }
auto NodeStorage::Read() -> MetadataSnapshot {
  auto call = impl_->Acquire();
  return call.context_->metadata_->Read();
}
auto NodeStorage::Commit(const MetadataSnapshot &base, const std::vector<MetadataMutation> &mutations)
    -> JournalResult {
  auto call = impl_->Acquire();
  try {
    auto result = call.context_->metadata_->Commit(base, mutations);
    if (result.outcome_ != JournalOutcome::Durable) {
      impl_->Failure(call.generation_, result.error_);
    } else {
      impl_->Pressure(call.generation_, false);
    }
    return result;
  } catch (const MetadataError &error) {
    if (error.Code() == MetadataErrorCode::ResourceUnavailable) {
      impl_->Pressure(call.generation_, true);
    }
    throw;
  }
}
auto NodeStorage::Writeback(size_t max_pages) -> MetadataWritebackResult {
  auto call = impl_->Acquire();
  try {
    auto result = call.context_->metadata_->Writeback(max_pages);
    if (result.outcome_ == MetadataWritebackOutcome::Failed) {
      impl_->Failure(call.generation_, result.error_);
    }
    return result;
  } catch (const MetadataError &error) {
    if (error.Code() == MetadataErrorCode::ResourceUnavailable) {
      impl_->Pressure(call.generation_, true);
    }
    throw;
  }
}
auto NodeStorage::Checkpoint(size_t max_pages) -> MetadataCheckpointResult {
  auto call = impl_->Acquire();
  try {
    auto result = call.context_->metadata_->Checkpoint(max_pages);
    if (result.outcome_ != MetadataCheckpointOutcome::Durable) {
      impl_->Failure(call.generation_, result.error_);
    } else {
      impl_->Pressure(call.generation_, false);
    }
    return result;
  } catch (const MetadataError &error) {
    if (error.Code() == MetadataErrorCode::ResourceUnavailable) {
      impl_->Pressure(call.generation_, true);
    }
    throw;
  }
}
void NodeStorage::Close() { impl_->Close(); }

}  // namespace bustub
