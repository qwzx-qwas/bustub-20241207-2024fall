//===----------------------------------------------------------------------===//
// BusTub: F34 lifecycle ownership; F33 short, serialized state decisions.
//===----------------------------------------------------------------------===//
#include "storage/disk/node_storage.h"
#include "common/config.h"
#include "storage/byte_range.h"

#include <condition_variable>  // NOLINT(build/c++11)
#include <mutex>               // NOLINT(build/c++11)
#include <thread>
#include <utility>

#include "object_io_internal.h"           // NOLINT(build/include_subdir): private sibling component.
#include "object_transaction_internal.h"  // NOLINT(build/include_subdir): private sibling component.

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
  void Stop(const std::exception_ptr &error) {
    view_.phase_ = NodeStoragePhase::Stopped;
    if (error) {
      Set(NodeStorageCondition::IOFault, true);
      if (!view_.error_) {
        view_.error_ = error;
      }
    }
  }
  void Fail(uint64_t generation, NodeStorageCondition condition, const std::exception_ptr &error) {
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
  void Objects(bool ready, bool transactions, const std::exception_ptr &error) {
    view_.object_read_ = view_.object_data_write_ = ready;
    view_.object_transaction_ = transactions;
    view_.object_error_ = error;
  }
  auto View() const -> NodeStorageView {
    auto result = view_;
    result.metadata_read_ = result.metadata_write_ = result.metadata_maintenance_ =
        result.phase_ == NodeStoragePhase::Serving;
    result.object_read_ = result.object_read_ && result.metadata_read_;
    result.object_data_write_ = result.object_data_write_ && result.metadata_write_;
    result.object_transaction_ = result.object_transaction_ && result.metadata_write_;
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

struct ObjectContext {
  std::unique_ptr<RegionManager> regions_;
  std::unique_ptr<DataAllocator> allocator_;
  std::unique_ptr<ObjectMappingStore> mapping_;
  std::unique_ptr<ObjectReferenceManager> references_;
  std::unique_ptr<ObjectIO> io_;
  std::unique_ptr<ObjectTransactionPipeline> transactions_;
  void Close() {
    if (transactions_) {
      transactions_->Close();
    }
    if (io_) {
      io_->Close();
    }
    if (references_) {
      references_->Close();
    }
    if (allocator_) {
      allocator_->Close();
    }
  }
};
void RequireDurable(const JournalResult &result) {
  if (result.outcome_ != JournalOutcome::Durable) {
    if (result.error_) {
      std::rethrow_exception(result.error_);
    }
    throw std::runtime_error("object initialization was not durably committed");
  }
}
// Called only by startup or explicit InitializeObjects under its initialization
// mutex. A failed Open instance is discarded before explicit Create.
auto OpenObjects(BootstrapStore &bootstrap, IOExecutor &executor, MetadataEngine &metadata,
                 const ObjectStorageOptions &options, const std::optional<ObjectTransactionOptions> &transactions,
                 const IOExecutorOptions &io_limits, size_t external_bytes, bool initialize)
    -> std::unique_ptr<ObjectContext> {
  auto context = std::make_unique<ObjectContext>();
  context->regions_ = std::make_unique<RegionManager>(bootstrap);
  context->allocator_ = std::make_unique<DataAllocator>(metadata, options.allocator_);
  try {
    context->allocator_->Open();
  } catch (const AllocationError &error) {
    if (!initialize || error.Code() != AllocationErrorCode::NotInitialized) {
      throw;
    }
    context->allocator_ = std::make_unique<DataAllocator>(metadata, options.allocator_);
    RequireDurable(context->allocator_->Create());
  }
  context->mapping_ = std::make_unique<ObjectMappingStore>(*context->allocator_, options.mapping_);
  try {
    context->mapping_->Open();
  } catch (const ObjectMappingError &error) {
    if (!initialize || error.Code() != ObjectMappingErrorCode::NotInitialized) {
      throw;
    }
    context->mapping_ = std::make_unique<ObjectMappingStore>(*context->allocator_, options.mapping_);
    RequireDurable(context->mapping_->Create());
  }
  context->references_ = std::make_unique<ObjectReferenceManager>(*context->mapping_, options.references_);
  context->io_ = std::make_unique<ObjectIO>(*context->regions_, executor, *context->allocator_, *context->mapping_,
                                            *context->references_, options.max_read_bytes_, options.max_write_bytes_,
                                            io_limits, external_bytes);
  if (transactions) {
    context->transactions_ = std::make_unique<ObjectTransactionPipeline>(
        *context->io_, *context->mapping_, *context->references_, *transactions, io_limits.memory_budget_);
  }
  return context;
}

struct StorageContext {
  // Declared in dependency order; member destruction reverses it.
  std::unique_ptr<BlockDevice> device_;
  std::unique_ptr<IOExecutor> executor_;
  std::unique_ptr<BootstrapStore> bootstrap_;
  std::unique_ptr<MetadataEngine> metadata_;
  std::exception_ptr repair_start_error_;
  std::mutex object_initialization_;
  std::unique_ptr<ObjectContext> objects_;
  std::exception_ptr object_error_;

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
    if (objects_) {
      close([&] { objects_->Close(); });
    }
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
    if (options_.memory_budget_) {
      options_.io_.memory_budget_ = std::make_shared<ResourceBudget>(*options_.memory_budget_);
      options_.metadata_.memory_budget_ = options_.io_.memory_budget_;
    }
    if (options_.transactions_ && !options_.objects_) {
      throw std::invalid_argument("object transactions require ordinary object storage");
    }
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
  auto Acquire(bool objects = false) -> Call {
    std::lock_guard<std::mutex> lock(mutex_);
    ObserveObjectError();
    const auto state = state_.View();
    if (objects && !state.object_read_) {
      throw MetadataError(MetadataErrorCode::NotReady, "ordinary object storage is not ready");
    }
    if (state.phase_ != NodeStoragePhase::Serving) {
      throw MetadataError(MetadataErrorCode::NotReady, "node storage is not serving metadata");
    }
    ++active_;
    return {*this, context_, state.generation_};
  }
  // mutex_ held; object IO only takes its short state lock here.
  void ObserveObjectError() {
    if (context_ && context_->objects_) {
      auto error = context_->objects_->io_->Error();
      if (!error && context_->objects_->transactions_) {
        error = context_->objects_->transactions_->Error();
      }
      if (error) {
        auto view = state_.View();
        const auto generation = view.phase_ == NodeStoragePhase::Draining ? view.generation_ - 1 : view.generation_;
        state_.Fail(generation, NodeStorageCondition::IOFault, error);
      }
    }
  }
  void Outcome(uint64_t generation, const JournalResult &result) {
    if (result.outcome_ != JournalOutcome::Durable) {
      Failure(generation, result.error_);
    } else {
      Pressure(generation, false);
    }
  }
  template <class F>
  auto ObjectCall(F action) {
    auto call = Acquire(true);
    try {
      return action(*call.context_->objects_, call.generation_);
    } catch (const MetadataError &error) {
      if (error.Code() == MetadataErrorCode::ResourceUnavailable) {
        Pressure(call.generation_, true);
      }
      throw;
    }
  }
  void InitializeObjects() {
    auto call = Acquire();
    if (!options_.objects_) {
      throw std::logic_error("ordinary object storage requires explicit options");
    }
    std::lock_guard<std::mutex> initialize(call.context_->object_initialization_);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (call.context_->objects_) {
        return;
      }
    }
    try {
      auto objects =
          OpenObjects(*call.context_->bootstrap_, *call.context_->executor_, *call.context_->metadata_,
                      *options_.objects_, options_.transactions_, options_.io_, options_.external_buffer_bytes_, true);
      std::lock_guard<std::mutex> lock(mutex_);
      // Close may have begun: retain the completed stack for its ordered drain,
      // but never reopen admission or change the phase here.
      call.context_->objects_ = std::move(objects);
      call.context_->object_error_ = nullptr;
      state_.Objects(true, options_.transactions_.has_value(), nullptr);
    } catch (...) {
      const auto error = std::current_exception();
      std::lock_guard<std::mutex> lock(mutex_);
      call.context_->object_error_ = error;
      state_.Objects(false, false, error);
      // Initialization failure is explicit and may include an uncertain commit.
      state_.Fail(call.generation_, NodeStorageCondition::RecoveryFailed, error);
      throw;
    }
  }
  void Finish() {
    std::lock_guard<std::mutex> lock(mutex_);
    --active_;
    idle_.notify_all();
  }
  void Failure(uint64_t generation, const std::exception_ptr &error) {
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
      context->executor_ =
          std::make_unique<IOExecutor>(*context->device_, options_.io_, options_.external_buffer_bytes_);
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
      if (options_.objects_) {
        try {
          context->objects_ =
              OpenObjects(*context->bootstrap_, *context->executor_, *context->metadata_, *options_.objects_,
                          options_.transactions_, options_.io_, options_.external_buffer_bytes_, layout != nullptr);
        } catch (const AllocationError &error) {
          if (layout != nullptr || error.Code() != AllocationErrorCode::NotInitialized) {
            throw;
          }
          context->object_error_ = std::current_exception();
        } catch (const ObjectMappingError &error) {
          if (layout != nullptr || error.Code() != ObjectMappingErrorCode::NotInitialized) {
            throw;
          }
          context->object_error_ = std::current_exception();
        } catch (const ObjectReferenceError &error) {
          if (layout != nullptr || error.Code() != ObjectReferenceErrorCode::UnsupportedFormat) {
            throw;
          }
          context->object_error_ = std::current_exception();
        }
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
        state_.Objects(context->objects_ != nullptr, context->objects_ && context->objects_->transactions_,
                       context->object_error_);
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
    ObserveObjectError();
    if (context_) {
      // F03 Status only takes its short status lock, never waits for disk IO.
      state_.Repair(context_->RepairStatus());
    }
    auto view = state_.View();
    if (view.phase_ == NodeStoragePhase::Serving && options_.io_.memory_budget_ &&
        options_.io_.memory_budget_->Pressure()) {
      view.conditions_ |= static_cast<uint32_t>(NodeStorageCondition::ResourcePressure);
      if (!view.primary_condition_ || view.primary_condition_ == NodeStorageCondition::BootstrapRedundancyLost)
        view.primary_condition_ = NodeStorageCondition::ResourcePressure;
    }
    return view;
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
      context = context_;
      lock.unlock();
      // A Store step re-enters the ordinary NodeStorage API. Let its accepted
      // work finish before changing readiness, otherwise Close could manufacture
      // a NotReady error in an otherwise healthy maintenance operation.
      if (context && context->objects_ && context->objects_->transactions_)
        context->objects_->transactions_->StopStoreMaintenance();
      lock.lock();
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
      ObserveObjectError();
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

  NodeStorageOptions options_;
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
void NodeStorage::InitializeObjects() { impl_->InitializeObjects(); }
auto NodeStorage::Objects() -> ObjectMappingSnapshot {
  return impl_->ObjectCall([](ObjectContext &c, uint64_t) { return c.mapping_->Read(); });
}
auto NodeStorage::CreateObjectSpace(const ObjectMappingSnapshot &base) -> ObjectSpaceCreation {
  return impl_->ObjectCall([&](ObjectContext &c, uint64_t generation) {
    auto result = c.mapping_->CreateSpace(base);
    impl_->Outcome(generation, result.result_);
    return result;
  });
}
auto NodeStorage::CreateObject(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t length, ObjectSizeMode mode)
    -> JournalResult {
  return impl_->ObjectCall([&](ObjectContext &c, uint64_t generation) {
    auto result = c.mapping_->CreateObject(base, key, length, mode);
    impl_->Outcome(generation, result);
    return result;
  });
}
auto NodeStorage::ResizeObject(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t length) -> JournalResult {
  return impl_->ObjectCall([&](ObjectContext &c, uint64_t generation) {
    auto result = c.mapping_->Resize(base, key, length);
    impl_->Outcome(generation, result);
    return result;
  });
}
auto NodeStorage::RemoveObject(const ObjectMappingSnapshot &base, ObjectKey key) -> JournalResult {
  return impl_->ObjectCall([&](ObjectContext &c, uint64_t generation) {
    auto result = c.mapping_->Remove(base, key);
    impl_->Outcome(generation, result);
    return result;
  });
}
auto NodeStorage::ReclaimObject(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t allocation)
    -> ObjectReclaimResult {
  return impl_->ObjectCall([&](ObjectContext &c, uint64_t generation) {
    auto result = c.references_->Reclaim(base, key, allocation);
    if (result.commit_) {
      impl_->Outcome(generation, *result.commit_);
    }
    return result;
  });
}
auto NodeStorage::SupportsObjectSharing() -> bool {
  return impl_->ObjectCall([&](ObjectContext &c, uint64_t) { return c.mapping_->SupportsSharing(); });
}
auto NodeStorage::ProtectObject(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t offset, uint64_t length)
    -> ObjectReadLease {
  return impl_->ObjectCall(
      [&](ObjectContext &c, uint64_t) { return c.references_->ProtectRead(base, key, offset, length); });
}
auto NodeStorage::ShareObjectRange(const ObjectMappingSnapshot &source, ObjectKey key, ObjectKey destination,
                                   uint64_t offset, uint64_t length, uint64_t destination_offset) -> JournalResult {
  if (!StorageByteRange::Create(destination_offset, length)) {
    throw std::invalid_argument("shared destination range overflows");
  }
  return impl_->ObjectCall([&](ObjectContext &c, uint64_t generation) {
    auto lease = c.references_->ProtectRead(source, key, offset, length);
    auto spans = lease.Spans();
    for (auto &span : spans) {
      if (span.data_ && !span.data_->owner_) span.data_->owner_ = key;
      span.offset_ = destination_offset + (span.offset_ - offset);
    }
    auto result = c.mapping_->Share(c.mapping_->Read(), destination, spans);
    impl_->Outcome(generation, result);
    return result;
  });
}
void NodeStorage::ShareObjectRangeBatched(const ObjectMappingSnapshot &source, ObjectKey key, ObjectKey destination,
                                          uint64_t offset, uint64_t length, uint64_t destination_offset) {
  uint64_t done = 0, bound = 64U * 1024U;
  while (done < length) {
    const auto take = std::min(bound, length - done);
    try {
      const auto result = ShareObjectRange(source, key, destination, offset + done, take, destination_offset + done);
      if (result.outcome_ != JournalOutcome::Durable) {
        if (result.error_) std::rethrow_exception(result.error_);
        throw std::runtime_error("shared range was not durably committed");
      }
      done += take;
    } catch (const MetadataViewConflict &) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } catch (const MetadataCommitBusy &) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } catch (const ObjectMappingError &e) {
      // Mapping planning rejects oversized changes before any commit. Reduce
      // the batch only for this capacity rejection, never retry uncertain IO.
      if (e.Code() != ObjectMappingErrorCode::ResourceUnavailable || take <= BUSTUB_PAGE_SIZE) throw;
      bound = std::max<uint64_t>(BUSTUB_PAGE_SIZE, (take / 2 / BUSTUB_PAGE_SIZE) * BUSTUB_PAGE_SIZE);
    }
  }
}
auto NodeStorage::ReadObject(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t offset, uint64_t length)
    -> ObjectRead {
  return impl_->ObjectCall([&](ObjectContext &c, uint64_t) { return c.io_->Read(base, key, offset, length); });
}
auto NodeStorage::ReadObjectInto(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t offset, uint64_t length,
                                 ObjectReadTarget target) -> ObjectRead {
  return impl_->ObjectCall(
      [&](ObjectContext &c, uint64_t) { return c.io_->ReadInto(base, key, offset, length, std::move(target)); });
}
auto NodeStorage::PrefetchObjectInto(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t offset, uint64_t length,
                                     ObjectReadTarget target, std::function<void(std::exception_ptr)> complete)
    -> bool {
  return impl_->ObjectCall([&](ObjectContext &c, uint64_t) {
    return c.io_->PrefetchInto(base, key, offset, length, std::move(target), std::move(complete));
  });
}
auto NodeStorage::PageIO() const -> PageIOCapabilities {
  const auto &o = impl_->options_;
  return impl_->ObjectCall([&](ObjectContext &c, uint64_t) {
    if (!o.transactions_ || o.external_buffer_bytes_ < BUSTUB_PAGE_SIZE) {
      throw std::invalid_argument("page backend requires Common and external frame budget");
    }
    const auto info = c.regions_->Describe(c.regions_->Region(RegionKind::Data));
    const auto unit = o.objects_->allocator_.allocation_bytes_;
    const auto rounded = (BUSTUB_PAGE_SIZE + unit - 1) / unit * unit;
    const auto stride =
        (BUSTUB_PAGE_SIZE + info.memory_alignment_ - 1) / info.memory_alignment_ * info.memory_alignment_;
    const auto charge = stride + BUSTUB_PAGE_SIZE + 2 * unit;
    const auto limit = std::min({o.io_.max_operations_ - o.io_.progress_operations_, o.transactions_->max_operations_,
                                 static_cast<size_t>(o.external_buffer_bytes_ / stride),
                                 static_cast<size_t>(o.transactions_->max_request_bytes_ / BUSTUB_PAGE_SIZE),
                                 static_cast<size_t>(o.transactions_->max_pending_bytes_ / charge),
                                 static_cast<size_t>(o.objects_->max_write_bytes_ / rounded)});
    return PageIOCapabilities{info.memory_alignment_, limit, o.objects_->max_read_bytes_, o.objects_->max_write_bytes_};
  });
}
auto NodeStorage::WriteObjectData(const void *source, size_t size) -> ObjectWrite {
  return impl_->ObjectCall([&](ObjectContext &c, uint64_t) { return c.io_->Write(source, size); });
}
auto NodeStorage::PublishObjectData(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t offset,
                                    ObjectWrite &write) -> JournalResult {
  return impl_->ObjectCall([&](ObjectContext &c, uint64_t generation) {
    auto result = c.io_->Publish(base, key, offset, write);
    impl_->Outcome(generation, result);
    return result;
  });
}
void NodeStorage::CheckControlBatch(const std::vector<ObjectControlMutation> &controls) const {
  const auto &o = impl_->options_;
  if (!o.transactions_ || !o.objects_ || controls.empty() || controls.size() > o.transactions_->max_operations_ ||
      controls.size() > o.objects_->mapping_.max_update_entries_) {
    throw std::invalid_argument("control publication exceeds operation budget");
  }
  uint64_t bytes = 0;
  uint64_t encoded = 0;
  for (const auto &c : controls) {
    const auto size = c.value_ ? c.value_->size() : 0;
    if (size > o.metadata_.max_value_bytes_ || size > o.transactions_->max_request_bytes_ - bytes) {
      throw std::invalid_argument("control publication exceeds value/request budget");
    }
    bytes += size;
    encoded += size + 32;
  }
  if (encoded > o.metadata_.max_batch_bytes_ || encoded > o.objects_->mapping_.max_update_bytes_ ||
      bytes > o.transactions_->max_pending_bytes_) {
    throw std::invalid_argument("control publication exceeds metadata batch budget");
  }
}
auto NodeStorage::SubmitObjects(ObjectTransaction &transaction) -> ObjectTransactionSubmission {
  return impl_->ObjectCall([&](ObjectContext &c, uint64_t generation) {
    if (!c.transactions_) {
      throw MetadataError(MetadataErrorCode::NotReady, "object transactions are not configured");
    }
    auto result = c.transactions_->Submit(transaction);
    impl_->Pressure(generation, result.admission_ == IOAdmission::Full);
    return result;
  });
}
auto NodeStorage::MemoryBudget() const -> std::shared_ptr<ResourceBudget> {
  return impl_->options_.io_.memory_budget_;
}
void NodeStorage::SetStoreMaintenance(std::function<void()> step) {
  std::shared_ptr<StorageContext> context;
  {
    std::lock_guard lock(impl_->mutex_);
    context = impl_->context_;
    if (step && (impl_->closing_ || !context || !context->objects_ || !context->objects_->transactions_))
      throw MetadataError(MetadataErrorCode::NotReady, "Store maintenance is unavailable");
  }
  // Detachment remains valid after a storage failure and during shutdown.
  // Shared context keeps the existing role alive while it drains this source.
  if (context && context->objects_ && context->objects_->transactions_)
    context->objects_->transactions_->SetStoreMaintenance(std::move(step));
}
void NodeStorage::Close() { impl_->Close(); }

}  // namespace bustub
