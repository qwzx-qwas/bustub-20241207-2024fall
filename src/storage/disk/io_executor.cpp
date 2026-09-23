//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// io_executor.cpp
//
//===----------------------------------------------------------------------===//

#include "storage/disk/io_executor.h"

#include <sys/types.h>

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <cstdlib>
#include <iterator>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <stdexcept>
#include <thread>  // NOLINT
#include <utility>

namespace bustub {
namespace {

auto Terminal(IOBatchPhase phase) -> bool { return phase == IOBatchPhase::Succeeded || phase == IOBatchPhase::Failed; }

// Separate from the scheduling mutex: the last owner can free buffers and
// return its reservation while a worker finishes under the scheduling lock.
struct IOBudget {
  IOBudget(const IOExecutorOptions &options, size_t external_limit)
      : options_(options), external_limit_(external_limit) {}

  auto Reserve(size_t operations, size_t bytes, size_t external_bytes) -> bool {
    std::lock_guard<std::mutex> lock(mutex_);
    if (operations > options_.max_operations_ - operations_ || bytes > options_.max_buffer_bytes_ - bytes_ ||
        external_bytes > external_limit_ - external_bytes_) {
      return false;
    }
    operations_ += operations;
    bytes_ += bytes;
    external_bytes_ += external_bytes;
    return true;
  }

  void Release(size_t operations, size_t bytes, size_t external_bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    operations_ -= operations;
    bytes_ -= bytes;
    external_bytes_ -= external_bytes;
  }

  IOExecutorOptions options_;
  std::mutex mutex_;
  size_t operations_{0};
  size_t bytes_{0};
  size_t external_limit_;
  size_t external_bytes_{0};
};

struct FreeBuffer {
  void operator()(char *data) const { std::free(data); }
};

struct IOAllocation {
  IOAllocation(size_t size, size_t alignment) {
    base_.reset(static_cast<char *>(std::malloc(size + alignment - 1)));
    if (!base_) {
      throw std::bad_alloc();
    }
    // Supports the actual queried alignment, without assuming a power of two
    // or substituting posix_memalign's own minimum alignment for the device's.
    const auto remainder = reinterpret_cast<uintptr_t>(base_.get()) % alignment;
    data_ = base_.get() + (remainder == 0 ? 0 : alignment - remainder);
  }

  std::unique_ptr<char, FreeBuffer> base_;
  char *data_{nullptr};
};

struct IOCore {
  IOCore(BlockDevice &device, const IOExecutorOptions &options, size_t external_limit)
      : device_(device), info_(device.Info()), budget_(std::make_shared<IOBudget>(options, external_limit)) {}
  BlockDevice &device_;
  BlockDeviceInfo info_;
  std::shared_ptr<IOBudget> budget_;
  std::mutex mutex_;
  std::condition_variable work_ready_;
  bool accepting_{true};
  std::list<std::shared_ptr<IOBatchData>> active_;
};

}  // namespace

struct IOBatchData {
  IOBatchData(std::shared_ptr<IOCore> core, std::vector<IORequest> requests, bool flush, size_t bytes,
              size_t external_bytes)
      : core_(std::move(core)),
        requests_(std::move(requests)),
        flush_(flush),
        bytes_(bytes),
        external_bytes_(external_bytes),
        external_(external_bytes != 0) {
    result_.operations_.resize(requests_.size());
    if (!external_) {
      buffers_.reserve(requests_.size());
      for (const auto &request : requests_) {
        buffers_.emplace_back(static_cast<size_t>(request.range_.Size()), core_->info_.memory_alignment_);
      }
    }
  }

  ~IOBatchData() {
    ReturnExternal();
    buffers_.clear();
    core_->budget_->Release(requests_.size(), bytes_, 0);
  }

  auto Address(size_t member) const -> const void * {
    return external_ ? leases_[member].buffer_ : buffers_[member].data_;
  }

  // No scheduling lock and no remaining IO users. Keep member/result slots until
  // the last batch owner goes away, but do not pin external storage with results.
  void ReturnExternal() {
    if (external_bytes_ == 0) {
      return;
    }
    leases_.clear();
    core_->budget_->Release(0, 0, std::exchange(external_bytes_, 0));
  }

  std::shared_ptr<IOCore> core_;
  std::vector<IORequest> requests_;
  std::vector<IOAllocation> buffers_;
  std::vector<IOBufferLease> leases_;
  bool flush_;
  size_t bytes_;
  size_t external_bytes_;
  bool external_;
  IOBatchResult result_;
  std::condition_variable done_;
  IOBatchPhase phase_{IOBatchPhase::Prepared};
  size_t next_{0};
  size_t running_{0};
  bool failed_{false};
  bool flush_started_{false};
  std::list<std::shared_ptr<IOBatchData>>::iterator position_;
};

namespace {

auto RequiredBytes(const std::vector<IORequest> &requests, bool flush, const BlockDeviceInfo &info, bool owned)
    -> size_t {
  if (requests.empty()) {
    throw std::invalid_argument("empty IO batch");
  }
  size_t bytes = 0;
  bool has_write = false;
  std::vector<const IORequest *> sorted;
  sorted.reserve(requests.size());
  for (const auto &request : requests) {
    const auto range = request.range_;
    if ((request.operation_ != IOOperation::Read && request.operation_ != IOOperation::Write) || range.Size() == 0 ||
        range.Offset() + range.Size() > info.capacity_ ||
        range.Offset() + range.Size() > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) ||
        range.Offset() % info.offset_alignment_ != 0 || range.Size() % info.offset_alignment_ != 0) {
      throw std::invalid_argument("invalid device range in IO batch");
    }
    const auto padding = owned ? static_cast<size_t>(info.memory_alignment_) - 1 : 0;
    if (range.Size() > std::numeric_limits<size_t>::max() - padding ||
        range.Size() + padding > std::numeric_limits<size_t>::max() - bytes) {
      throw std::invalid_argument("IO batch allocation size overflow");
    }
    bytes += static_cast<size_t>(range.Size()) + padding;
    has_write = has_write || request.operation_ == IOOperation::Write;
    sorted.push_back(&request);
  }
  if (flush && !has_write) {
    throw std::invalid_argument("flush batch requires a member write");
  }
  std::sort(sorted.begin(), sorted.end(),
            [](const IORequest *a, const IORequest *b) { return a->range_.Offset() < b->range_.Offset(); });
  uint64_t previous_end = 0;
  uint64_t previous_write_end = 0;
  for (const auto *request : sorted) {
    const auto range = request->range_;
    if (range.Offset() < previous_write_end ||
        (request->operation_ == IOOperation::Write && range.Offset() < previous_end)) {
      throw std::invalid_argument("conflicting members require separate ordered IO batches");
    }
    previous_end = std::max(previous_end, range.Offset() + range.Size());
    if (request->operation_ == IOOperation::Write) {
      previous_write_end = previous_end;
    }
  }
  return bytes;
}

void Finish(const std::shared_ptr<IOBatchData> &batch, bool success, std::unique_lock<std::mutex> &lock) {
  // All members/Flush have stopped. Leave the batch on active_ while returning
  // leases so Shutdown cannot finish early. No remaining member is dispatchable.
  if (batch->external_) {
    lock.unlock();
    batch->ReturnExternal();
    lock.lock();
  }
  batch->phase_ = success ? IOBatchPhase::Succeeded : IOBatchPhase::Failed;
  batch->core_->active_.erase(batch->position_);
  batch->done_.notify_all();
}

void RunWorkers(const std::shared_ptr<IOCore> &core) {
  std::unique_lock<std::mutex> lock(core->mutex_);
  while (true) {
    std::shared_ptr<IOBatchData> batch;
    size_t member = 0;
    bool flush = false;
    for (auto it = core->active_.begin(); it != core->active_.end(); ++it) {
      auto &candidate = *it;
      if (!candidate->failed_ && candidate->next_ < candidate->requests_.size()) {
        batch = candidate;
        member = batch->next_++;
        ++batch->running_;
        batch->phase_ = IOBatchPhase::Executing;
      } else if (candidate->phase_ == IOBatchPhase::Flushing && !candidate->flush_started_) {
        batch = candidate;
        batch->flush_started_ = true;
        flush = true;
      }
      if (batch) {
        // One member per visit; splice allocates nothing and preserves iterators.
        core->active_.splice(core->active_.end(), core->active_, it);
        break;
      }
    }
    if (!batch) {
      if (!core->accepting_ && core->active_.empty()) {
        return;
      }
      core->work_ready_.wait(lock);
      continue;
    }
    lock.unlock();
    std::exception_ptr error;
    uint64_t completed = 0;
    try {
      if (flush) {
        core->device_.Flush();
      } else {
        const auto &request = batch->requests_[member];
        const auto *buffer = batch->Address(member);
        const auto size = static_cast<size_t>(request.range_.Size());
        if (request.operation_ == IOOperation::Read) {
          core->device_.ReadAt(request.range_, const_cast<void *>(buffer), size);
        } else {
          core->device_.WriteAt(request.range_, buffer, size);
        }
        completed = request.range_.Size();
      }
    } catch (const BlockDeviceIOError &failure) {
      completed = failure.CompletedBytes();
      error = std::current_exception();
    } catch (...) {
      error = std::current_exception();
    }
    lock.lock();
    if (flush) {
      batch->result_.flush_error_ = error;
      batch->result_.writes_durable_ = !error;
      Finish(batch, !error, lock);
    } else {
      auto &result = batch->result_.operations_[member];
      result.outcome_ = error ? IOOutcome::Failed : IOOutcome::Succeeded;
      result.completed_bytes_ = completed;
      result.error_ = error;
      --batch->running_;
      batch->failed_ = batch->failed_ || static_cast<bool>(error);
      if (batch->failed_) {
        // Undispatched members stay NotStarted; already running IO must drain.
        batch->phase_ = IOBatchPhase::Draining;
        if (batch->running_ == 0) {
          Finish(batch, false, lock);
        }
      } else if (batch->next_ == batch->requests_.size() && batch->running_ == 0) {
        if (batch->flush_) {
          batch->phase_ = IOBatchPhase::Flushing;
        } else {
          Finish(batch, true, lock);
        }
      }
    }
    batch.reset();
    core->work_ready_.notify_all();
  }
}

}  // namespace

IOBufferLease::IOBufferLease(IOOperation operation, const void *buffer, size_t capacity, std::function<void()> release)
    : operation_(operation), buffer_(buffer), capacity_(capacity), release_(std::move(release)) {
  if (buffer == nullptr || capacity == 0 || !release_) {
    throw std::invalid_argument("external buffer requires storage and a release permission");
  }
}

auto IOBufferLease::ForRead(void *buffer, size_t capacity, std::function<void()> release) -> IOBufferLease {
  return {IOOperation::Read, buffer, capacity, std::move(release)};
}

auto IOBufferLease::ForWrite(const void *buffer, size_t capacity, std::function<void()> release) -> IOBufferLease {
  return {IOOperation::Write, buffer, capacity, std::move(release)};
}

IOBufferLease::~IOBufferLease() { Release(); }

IOBufferLease::IOBufferLease(IOBufferLease &&other) noexcept
    : operation_(other.operation_),
      buffer_(std::exchange(other.buffer_, nullptr)),
      capacity_(std::exchange(other.capacity_, 0)),
      release_(std::move(other.release_)) {}

auto IOBufferLease::operator=(IOBufferLease &&other) noexcept -> IOBufferLease & {
  if (this != &other) {
    Release();
    operation_ = other.operation_;
    buffer_ = std::exchange(other.buffer_, nullptr);
    capacity_ = std::exchange(other.capacity_, 0);
    release_ = std::move(other.release_);
  }
  return *this;
}

void IOBufferLease::Release() noexcept {
  if (buffer_ != nullptr) {
    buffer_ = nullptr;
    capacity_ = 0;
    auto release = std::move(release_);
    release();
  }
}

IOBatch::IOBatch(std::shared_ptr<IOBatchData> data) : data_(std::move(data)) {}
IOBatch::~IOBatch() = default;
IOBatch::IOBatch(IOBatch &&) noexcept = default;
auto IOBatch::operator=(IOBatch &&) noexcept -> IOBatch & = default;

auto IOBatch::Data() const -> IOBatchData & {
  if (!data_) {
    throw std::logic_error("moved-from IO batch");
  }
  return *data_;
}

auto IOBatch::Buffer(size_t member) -> char * {
  auto &data = Data();
  std::lock_guard<std::mutex> lock(data.core_->mutex_);
  if (data.external_) {
    throw std::logic_error("external buffer access belongs to its permission owner");
  }
  if (data.phase_ != IOBatchPhase::Prepared && !Terminal(data.phase_)) {
    throw std::logic_error("IO batch still owns buffer access");
  }
  return data.buffers_.at(member).data_;
}

auto IOBatch::Phase() const -> IOBatchPhase {
  auto &data = Data();
  std::lock_guard<std::mutex> lock(data.core_->mutex_);
  return data.phase_;
}

void IOBatch::Wait() const {
  auto &data = Data();
  std::unique_lock<std::mutex> lock(data.core_->mutex_);
  if (data.phase_ == IOBatchPhase::Prepared) {
    throw std::logic_error("IO batch has not been submitted");
  }
  data.done_.wait(lock, [&data] { return Terminal(data.phase_); });
}

auto IOBatch::WaitFor(std::chrono::milliseconds timeout) const -> bool {
  auto &data = Data();
  std::unique_lock<std::mutex> lock(data.core_->mutex_);
  if (data.phase_ == IOBatchPhase::Prepared) {
    throw std::logic_error("IO batch has not been submitted");
  }
  return data.done_.wait_for(lock, timeout, [&data] { return Terminal(data.phase_); });
}

auto IOBatch::Result() const -> const IOBatchResult & {
  auto &data = Data();
  std::lock_guard<std::mutex> lock(data.core_->mutex_);
  if (!Terminal(data.phase_)) {
    throw std::logic_error("IO batch has no terminal result");
  }
  return data.result_;
}

struct IOExecutor::Impl {
  Impl(BlockDevice &device, const IOExecutorOptions &options, size_t external_limit) {
    if (options.worker_count_ == 0 || options.max_operations_ == 0 ||
        (options.max_buffer_bytes_ == 0 && external_limit == 0) || options.worker_count_ > options.max_operations_) {
      throw std::invalid_argument("invalid IO executor limits");
    }
    core_ = std::make_shared<IOCore>(device, options, external_limit);
    workers_.reserve(options.worker_count_);
    try {
      for (size_t i = 0; i < options.worker_count_; ++i) {
        workers_.emplace_back(RunWorkers, core_);
      }
    } catch (...) {
      Shutdown();
      throw;
    }
  }

  void Shutdown() {
    std::call_once(shutdown_, [this] {
      {
        std::lock_guard<std::mutex> lock(core_->mutex_);
        core_->accepting_ = false;
      }
      core_->work_ready_.notify_all();
      for (auto &worker : workers_) {
        worker.join();
      }
    });
  }

  std::shared_ptr<IOCore> core_;
  std::vector<std::thread> workers_;
  std::once_flag shutdown_;
};

IOExecutor::IOExecutor(BlockDevice &device, const IOExecutorOptions &options) : IOExecutor(device, options, 0) {}

IOExecutor::IOExecutor(BlockDevice &device, const IOExecutorOptions &options, size_t max_external_buffer_bytes)
    : impl_(std::make_unique<Impl>(device, options, max_external_buffer_bytes)) {}

IOExecutor::~IOExecutor() { Shutdown(); }

auto IOExecutor::TryPrepare(std::vector<IORequest> requests, bool flush_after_writes) -> IOPreparation {
  const auto &core = impl_->core_;
  if (requests.size() > core->budget_->options_.max_operations_) {
    return {IOAdmission::Full, std::nullopt};
  }
  const auto bytes = RequiredBytes(requests, flush_after_writes, core->info_, true);
  const auto count = requests.size();
  {
    std::lock_guard<std::mutex> lock(core->mutex_);
    if (!core->accepting_) {
      return {IOAdmission::Stopped, std::nullopt};
    }
    if (!core->budget_->Reserve(count, bytes, 0)) {
      return {IOAdmission::Full, std::nullopt};
    }
  }
  std::shared_ptr<IOBatchData> data;
  try {
    data = std::make_shared<IOBatchData>(core, std::move(requests), flush_after_writes, bytes, 0);
  } catch (...) {
    core->budget_->Release(count, bytes, 0);
    throw;
  }
  return {IOAdmission::Accepted, IOBatch(std::move(data))};
}

auto IOExecutor::TryPrepareExternal(std::vector<IORequest> requests, std::vector<IOBufferLease> &leases,
                                    bool flush_after_writes) -> IOPreparation {
  const auto &core = impl_->core_;
  if (requests.size() > core->budget_->options_.max_operations_) {
    return {IOAdmission::Full, std::nullopt};
  }
  RequiredBytes(requests, flush_after_writes, core->info_, false);
  if (leases.size() != requests.size()) {
    throw std::invalid_argument("one external permission is required per IO member");
  }
  std::vector<size_t> sorted;
  sorted.reserve(leases.size());
  size_t bytes = 0;
  for (size_t i = 0; i < leases.size(); ++i) {
    const auto &lease = leases[i];
    const auto address = reinterpret_cast<uintptr_t>(lease.buffer_);
    if (lease.buffer_ == nullptr || !lease.release_ || lease.operation_ != requests[i].operation_ ||
        lease.capacity_ < requests[i].range_.Size() || address % core->info_.memory_alignment_ != 0 ||
        lease.capacity_ > std::numeric_limits<uintptr_t>::max() - address ||
        lease.capacity_ > std::numeric_limits<size_t>::max() - bytes) {
      throw std::invalid_argument("invalid external buffer permission");
    }
    bytes += lease.capacity_;
    sorted.push_back(i);
  }
  std::sort(sorted.begin(), sorted.end(), [&leases](size_t a, size_t b) {
    return reinterpret_cast<uintptr_t>(leases[a].buffer_) < reinterpret_cast<uintptr_t>(leases[b].buffer_);
  });
  uintptr_t previous_end = 0;
  uintptr_t previous_fill_end = 0;
  for (const auto i : sorted) {
    const auto &lease = leases[i];
    const auto begin = reinterpret_cast<uintptr_t>(lease.buffer_);
    const bool fills_ram = lease.operation_ == IOOperation::Read;
    if (begin < previous_fill_end || (fills_ram && begin < previous_end)) {
      throw std::invalid_argument("conflicting external buffer permissions");
    }
    previous_end = std::max(previous_end, begin + lease.capacity_);
    if (fills_ram) {
      previous_fill_end = previous_end;
    }
  }
  const auto count = requests.size();
  {
    std::lock_guard<std::mutex> lock(core->mutex_);
    if (!core->accepting_) {
      return {IOAdmission::Stopped, std::nullopt};
    }
    if (!core->budget_->Reserve(count, 0, bytes)) {
      return {IOAdmission::Full, std::nullopt};
    }
  }
  std::shared_ptr<IOBatchData> data;
  try {
    data = std::make_shared<IOBatchData>(core, std::move(requests), flush_after_writes, 0, bytes);
  } catch (...) {
    core->budget_->Release(count, 0, bytes);
    throw;
  }
  // No throwing work after transfer: rejected/failed preparation keeps leases.
  data->leases_.swap(leases);
  return {IOAdmission::Accepted, IOBatch(std::move(data))};
}

auto IOExecutor::TrySubmit(IOBatch &batch) -> IOAdmission {
  const auto &core = impl_->core_;
  auto &data = batch.Data();
  if (data.core_ != core) {
    throw std::invalid_argument("IO batch belongs to another executor");
  }
  {
    std::lock_guard<std::mutex> lock(core->mutex_);
    if (data.phase_ != IOBatchPhase::Prepared) {
      throw std::logic_error("IO batch has already been submitted");
    }
    if (!core->accepting_) {
      return IOAdmission::Stopped;
    }
    core->active_.push_back(batch.data_);
    data.position_ = std::prev(core->active_.end());
    data.phase_ = IOBatchPhase::Queued;
  }
  core->work_ready_.notify_all();
  return IOAdmission::Accepted;
}

void IOExecutor::Shutdown() { impl_->Shutdown(); }

}  // namespace bustub
