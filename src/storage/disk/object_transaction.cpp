//===----------------------------------------------------------------------===//
// BusTub: event-driven Common pipeline on the existing IO and Journal workers.
//===----------------------------------------------------------------------===//
#include "storage/disk/object_transaction.h"

#include <algorithm>
#include <array>
#include <condition_variable>  // NOLINT(build/c++11)
#include <cstring>
#include <list>
#include <mutex>  // NOLINT(build/c++11)
#include <set>
#include <thread>  // NOLINT(build/c++11)
#include <tuple>
#include <utility>

#include "object_transaction_internal.h"  // NOLINT(build/include_subdir): private sibling component.

namespace bustub {
namespace {
struct RequestBudget {
  std::mutex mutex_;
  size_t requests_[2]{0, 0};
  uint64_t bytes_[2]{0, 0};
};
enum class Step { Pending, Reading, Writing, Ready, Committing, Done };
auto Same(ObjectKey a, ObjectKey b) -> bool { return a.space_ == b.space_ && a.number_ == b.number_; }
auto End(uint64_t offset, uint64_t size) -> uint64_t {
  if (!StorageByteRange::Create(offset, size)) {
    throw std::invalid_argument("object transaction range overflows");
  }
  return offset + size;
}
auto Transient(const std::exception_ptr &error) -> bool {
  try {
    std::rethrow_exception(error);
  } catch (const ObjectIOBusy &) {
    return true;
  } catch (const AllocationError &e) {
    return e.Code() == AllocationErrorCode::Busy;
  } catch (const ObjectReferenceError &e) {
    return e.Code() == ObjectReferenceErrorCode::Busy;
  } catch (...) {
    return false;
  }
}
}  // namespace
struct ObjectTransactionData {
  ~ObjectTransactionData() {
    if (budget_) {
      std::lock_guard<std::mutex> lock(budget_->mutex_);
      --budget_->requests_[progress_];
      budget_->bytes_[progress_] -= charge_;
    }
  }
  void Finish(JournalResult result) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto &op : input_.objects_) {
      op.source_.reset();  // Release frame permission before publishing completion.
      std::vector<std::byte>().swap(op.bytes_);
    }
    bytes_.clear();
    payloads_.clear();
    input_.controls_.clear();
    for (auto &change : changes_) std::vector<uint32_t>().swap(change.checksums_);
    memory_.Reset();
    result_ = std::move(result);
    done_.notify_all();
  }
  auto Finished() const -> bool {
    std::lock_guard<std::mutex> lock(mutex_);
    return result_.has_value();
  }
  mutable std::mutex mutex_;
  std::condition_variable done_;
  std::optional<JournalResult> result_;
  std::shared_ptr<RequestBudget> budget_;
  uint64_t charge_{0};
  ResourceCharge memory_;
  bool progress_{false};
  ObjectTransaction input_;
  Step step_{Step::Pending};  // Written only by the coordinator.
  std::optional<ObjectMappingSnapshot> view_;
  std::vector<ObjectChange> changes_;
  std::vector<CommonInput> bytes_;
  std::vector<std::array<std::optional<ObjectRead>, 2>> reads_;
  std::vector<std::array<bool, 2>> read_done_;
  std::vector<bool> assembled_;
  CommonDataWrite write_;
  std::vector<std::vector<std::byte>> payloads_;
};
ObjectTransactionTicket::ObjectTransactionTicket(std::shared_ptr<ObjectTransactionData> data)
    : data_(std::move(data)) {}
void ObjectTransactionTicket::Wait() const {
  std::unique_lock<std::mutex> lock(data_->mutex_);
  data_->done_.wait(lock, [&] { return data_->result_.has_value(); });
}
auto ObjectTransactionTicket::WaitFor(std::chrono::milliseconds timeout) const -> bool {
  std::unique_lock<std::mutex> lock(data_->mutex_);
  return data_->done_.wait_for(lock, timeout, [&] { return data_->result_.has_value(); });
}
auto ObjectTransactionTicket::Result() const -> JournalResult {
  std::lock_guard<std::mutex> lock(data_->mutex_);
  if (!data_->result_) {
    throw std::logic_error("object transaction is not complete");
  }
  return *data_->result_;
}

struct ObjectTransactionPipeline::Impl {
  struct State {
    std::mutex mutex_;
    std::condition_variable changed_;
    std::list<std::shared_ptr<ObjectTransactionData>> pending_;
    std::shared_ptr<ObjectTransactionData> commit_;
    bool deferred_notified_{true};
    bool accepting_{true}, coordinator_done_{false}, notified_{false};
    std::exception_ptr error_;
    void Notify() {
      std::lock_guard<std::mutex> lock(mutex_);
      notified_ = true;
      changed_.notify_all();
    }
  };
  Impl(ObjectIO &io, ObjectMappingStore &mapping, ObjectReferenceManager &references, ObjectTransactionOptions options,
       std::shared_ptr<ResourceBudget> memory)
      : io_(io),
        mapping_(mapping),
        references_(references),
        options_(options),
        unit_(ObjectMappingAccess::Unit(mapping)),
        memory_(ResourceAccount::Create(std::move(memory))) {
    if (options.max_requests_ == 0 || options.max_operations_ == 0 || options.max_request_bytes_ == 0 ||
        options.max_pending_bytes_ == 0 || options.retry_interval_.count() <= 0 || options.gc_interval_.count() <= 0) {
      throw std::invalid_argument("object transactions require positive explicit budgets");
    }
    if (options.deferred_max_bytes_ != 0 && !ObjectMappingAccess::SupportsDeferred(mapping_))
      throw std::invalid_argument(
          "Deferred requires a v3 object store; existing v2 stores remain Common without migration");
    if (options.deferred_max_bytes_ != 0 &&
        (options.deferred_pending_bytes_ < options.deferred_max_bytes_ || options.deferred_pending_tasks_ == 0))
      throw std::invalid_argument("Deferred requires explicit backlog budgets");
    metadata_ = std::thread([this] { CommitLoop(); });
    try {
      coordinator_ = std::thread([this] { Drive(); });
      deferred_ = std::thread([this] { DeferredLoop(); });
      garbage_ = std::thread([this] { GarbageLoop(); });
    } catch (...) {
      {
        std::lock_guard<std::mutex> lock(state_->mutex_);
        state_->accepting_ = false;
        state_->coordinator_done_ = true;
      }
      state_->changed_.notify_all();
      state_->Notify();
      if (coordinator_.joinable()) coordinator_.join();
      if (deferred_.joinable()) deferred_.join();
      metadata_.join();
      throw;
    }
  }
  ~Impl() { Close(); }
  auto Notify() -> std::function<void()> {
    std::weak_ptr<State> weak = state_;
    return [weak] {
      if (auto state = weak.lock()) {
        state->Notify();
      }
    };
  }
  auto Submit(ObjectTransaction &input) -> ObjectTransactionSubmission {
    const auto count = input.objects_.size() + input.controls_.size();
    if (count == 0 || count > options_.max_operations_) {
      throw std::invalid_argument("object transaction operation count is outside its budget");
    }
    uint64_t bytes = 0;
    uint64_t charge = 0;
    uint64_t ram = 0;
    std::set<std::pair<uint64_t, uint64_t>> objects;
    std::set<std::tuple<uint64_t, uint64_t, uint64_t>> controls;
    auto account = [&](uint64_t size) {
      if (size > options_.max_request_bytes_ - bytes) {
        throw std::invalid_argument("object transaction input exceeds request budget");
      }
      bytes += size;
    };
    for (const auto &op : input.objects_) {
      if (!objects.emplace(op.object_.space_, op.object_.number_).second || op.object_.space_ == 0 ||
          op.object_.space_ >= (uint64_t{1} << 56) ||
          (op.mode_ != ObjectSizeMode::Variable && op.mode_ != ObjectSizeMode::Fixed)) {
        throw std::invalid_argument("invalid or repeated object in transaction");
      }
      const bool write = op.operation_ == ObjectOperation::Write || op.operation_ == ObjectOperation::Append;
      if ((write == (op.Size() == 0)) || (op.operation_ == ObjectOperation::Append && op.offset_ != 0) ||
          (op.operation_ == ObjectOperation::Remove && op.offset_ != 0)) {
        throw std::invalid_argument("object operation has incompatible input");
      }
      if ((op.operation_ == ObjectOperation::Unmap) != (op.length_ != 0)) {
        throw std::invalid_argument("only unmap specifies a nonempty range");
      }
      if (op.operation_ < ObjectOperation::Create || op.operation_ > ObjectOperation::Unmap) {
        throw std::invalid_argument("unknown object operation");
      }
      if (op.source_ && (!op.bytes_.empty() || !op.source_->data_ || !op.source_->owner_ ||
                         op.source_->size_ > op.source_->capacity_)) {
        throw std::invalid_argument("invalid borrowed object source");
      }
      account(op.Size());
      // Borrowed frame capacity is retention pressure, not another allocation.
      ram = End(ram, End(op.source_ ? 0 : op.bytes_.capacity(), write ? 2 * End(op.Size(), End(unit_, unit_)) : 0));
      if (write) ram = End(ram, ((op.Size() + 3 * unit_ - 1) / unit_) * sizeof(uint32_t));
      // Charge the moved input's retained capacity, plus worst-case assembly.
      // F02 owns its separate IO-buffer budget; operation/result slots are bounded
      // independently by max_requests/max_operations.
      charge = End(charge, End((op.source_ ? op.source_->capacity_ : op.bytes_.capacity()),
                               write ? End(op.Size(), End(unit_, unit_)) : 0));
    }
    for (const auto &control : input.controls_) {
      if (control.owner_.space_ == 0 || control.owner_.space_ >= (uint64_t{1} << 56) ||
          !controls.emplace(control.owner_.space_, control.owner_.number_, control.item_).second) {
        throw std::invalid_argument("invalid or repeated control key");
      }
      const auto size = control.value_ ? control.value_->size() : 0;
      account(size);
      charge = End(charge, control.value_ ? control.value_->capacity() : 0);
      ram = End(ram, control.value_ ? control.value_->capacity() : 0);
    }
    if (charge > options_.max_pending_bytes_) {
      throw std::invalid_argument("one object transaction exceeds pending byte budget");
    }
    const bool progress = ProgressWork::Active();
    if (!memory_->Reserve(ram, progress)) return {IOAdmission::Full, std::nullopt};
    ResourceCharge memory(memory_, ram, progress);
    auto task = std::make_shared<ObjectTransactionData>();
    task->memory_ = std::move(memory);
    task->charge_ = charge;
    task->progress_ = progress;
    std::lock_guard<std::mutex> lock(state_->mutex_);
    if (!state_->accepting_ || state_->error_) {
      return {IOAdmission::Stopped, std::nullopt};
    }
    {
      std::lock_guard<std::mutex> budget_lock(budget_->mutex_);
      // One serialized maintenance source can need one submission while all
      // ordinary result tickets are retained. Both lanes retain the existing
      // per-request byte/operation limits and share actual node memory.
      const auto requests = progress ? size_t{1} : options_.max_requests_;
      if (budget_->requests_[progress] == requests ||
          charge > options_.max_pending_bytes_ - budget_->bytes_[progress]) {
        return {IOAdmission::Full, std::nullopt};
      }
      ++budget_->requests_[progress];
      budget_->bytes_[progress] += charge;
      task->budget_ = budget_;
    }
    // Queue storage and result ownership exist BEFORE consuming the caller's input.
    state_->pending_.push_back(task);
    task->input_ = std::move(input);
    state_->notified_ = true;
    state_->changed_.notify_all();
    return {IOAdmission::Accepted, ObjectTransactionTicket(task)};
  }
  static auto Conflicts(const ObjectTransactionData &a, const ObjectTransactionData &b) -> bool {
    auto touches = [&](ObjectKey key) {
      return std::any_of(b.input_.objects_.begin(), b.input_.objects_.end(),
                         [&](const auto &op) { return Same(key, op.object_); }) ||
             std::any_of(b.input_.controls_.begin(), b.input_.controls_.end(),
                         [&](const auto &op) { return Same(key, op.owner_); });
    };
    return std::any_of(a.input_.objects_.begin(), a.input_.objects_.end(),
                       [&](const auto &op) { return touches(op.object_); }) ||
           std::any_of(a.input_.controls_.begin(), a.input_.controls_.end(),
                       [&](const auto &op) { return touches(op.owner_); });
  }
  void Plan(ObjectTransactionData &task) {
    task.view_.emplace(mapping_.Read());
    task.changes_.clear();
    task.bytes_.clear();
    for (const auto &op : task.input_.objects_) {
      ObjectChange change{op.operation_, op.object_, 0, op.offset_, op.offset_, op.mode_, {}};
      CommonInput bytes;
      if (op.operation_ != ObjectOperation::Create) {
        const auto info = task.view_->Describe(op.object_);
        change.version_ = info.version_;
        if (op.operation_ == ObjectOperation::Unmap) {
          if (End(op.offset_, op.length_) > info.size_) {
            throw std::invalid_argument("unmap exceeds object length");
          }
          change.length_ = op.length_;
        }
        if (op.operation_ == ObjectOperation::Write || op.operation_ == ObjectOperation::Append) {
          change.offset_ = op.operation_ == ObjectOperation::Append ? info.size_ : op.offset_;
          const auto end = End(change.offset_, op.Size());
          if (info.mode_ == ObjectSizeMode::Fixed && end > info.size_) {
            throw std::invalid_argument("write exceeds fixed object size");
          }
          // F01 reports direct-IO alignment, not a power-failure isolation
          // unit. An allocation's small tail therefore stays COW until a real
          // device contract can authorize independent writes within that unit.
          change.offset_ -= change.offset_ % unit_;
          const auto stop = std::max(end, std::min(End(end, unit_ - 1) / unit_ * unit_, info.size_));
          change.length_ = stop - change.offset_;
          if (change.length_ > options_.max_request_bytes_ + 2 * unit_) {
            throw std::invalid_argument("object assembly exceeds request budget");
          }
          if (op.source_ && change.offset_ == (op.operation_ == ObjectOperation::Append ? info.size_ : op.offset_) &&
              change.length_ == op.Size() && change.length_ % unit_ == 0) {
            bytes.source_ = op.source_;
          } else {
            bytes.owned_.resize(change.length_);  // Explicit preservation/padding buffer.
          }
        }
      }
      task.changes_.push_back(std::move(change));
      task.bytes_.push_back(std::move(bytes));
    }
    task.payloads_.resize(task.changes_.size());
    // Select using assembled byte ranges, never SQL's changed-field count.
    uint64_t pending_bytes = 0;
    size_t pending_tasks = 0;
    if (options_.deferred_max_bytes_ != 0) {
      const auto pending = ObjectMappingAccess::Pending(*task.view_, options_.deferred_pending_tasks_ + 1);
      pending_tasks = pending.size();
      for (const auto &p : pending) pending_bytes += p.payload_.ref_.bytes_ - 4;
    }
    for (size_t i = 0; i < task.changes_.size(); ++i) {
      auto &change = task.changes_[i];
      const auto &op = task.input_.objects_[i];
      if (!op.common_only_ && (op.operation_ == ObjectOperation::Write || op.operation_ == ObjectOperation::Append) &&
          change.length_ <= options_.deferred_max_bytes_ && pending_tasks < options_.deferred_pending_tasks_ &&
          pending_bytes <= options_.deferred_pending_bytes_ &&
          change.length_ <= options_.deferred_pending_bytes_ - pending_bytes) {
        change.deferred_ = true;
        pending_bytes += change.length_;
        ++pending_tasks;
      }
    }
    task.reads_.resize(task.changes_.size());
    task.read_done_.assign(task.changes_.size(), {false, false});
    task.assembled_.assign(task.changes_.size(), false);
    task.step_ = Step::Reading;
  }
  auto Assemble(ObjectTransactionData &task) -> bool {
    bool complete = true;
    for (size_t i = 0; i < task.changes_.size(); ++i) {
      if (task.assembled_[i]) {
        continue;
      }
      auto &bytes = task.bytes_[i];
      const auto &op = task.input_.objects_[i];
      const auto &change = task.changes_[i];
      if (bytes.Size() == 0 || bytes.source_) {
        task.assembled_[i] = true;
        continue;
      }
      const auto info = task.view_->Describe(op.object_);
      const auto target = op.operation_ == ObjectOperation::Append ? info.size_ : op.offset_;
      std::array<std::pair<uint64_t, uint64_t>, 2> edges{};
      {
        const auto prefix_end = std::min(target, info.size_);
        const auto written_end = target + op.Size();
        const auto preserved_end = std::min(change.offset_ + change.length_, info.size_);
        if (prefix_end > change.offset_) {
          edges[0] = {change.offset_, prefix_end - change.offset_};
        }
        if (preserved_end > written_end) {
          edges[1] = {written_end, preserved_end - written_end};
        }
        // Small partial COW reads its unit once. A large COW reads only the two
        // surviving edges, never the overwritten middle of the old object.
        if (change.length_ <= unit_ && (edges[0].second != 0 || edges[1].second != 0)) {
          edges[0] = {change.offset_, preserved_end - change.offset_};
          edges[1] = {0, 0};
        }
      }
      for (size_t part = 0; part < edges.size(); ++part) {
        if (edges[part].second == 0) {
          task.read_done_[i][part] = true;
        }
        if (task.read_done_[i][part]) {
          continue;
        }
        auto &read = task.reads_[i][part];
        if (!read) {
          read.emplace(io_.Read(*task.view_, op.object_, edges[part].first, edges[part].second, Notify()));
        }
        if (read->WaitFor(std::chrono::milliseconds(0))) {
          const auto destination = edges[part].first - change.offset_;
          read->CopyTo(bytes.owned_.data() + destination, bytes.owned_.size() - destination);
          read.reset();
          task.read_done_[i][part] = true;
        }
      }
      if (!task.read_done_[i][0] || !task.read_done_[i][1]) {
        complete = false;
        continue;
      }
      std::memcpy(bytes.owned_.data() + (target - change.offset_), op.Data(), op.Size());
      task.assembled_[i] = true;
    }
    return complete;
  }
  void Fail(ObjectTransactionData &task, std::exception_ptr error) {
    // Read owners keep their own physical protection through actual completion;
    // a failed preparation has not submitted a data write or metadata commit.
    task.reads_.clear();
    task.write_ = {};
    task.view_.reset();
    task.bytes_.clear();
    task.Finish({JournalOutcome::NotCommitted, 0, 0, std::move(error)});
    task.step_ = Step::Done;
  }
  void Drive() {
    std::list<std::shared_ptr<ObjectTransactionData>> live;
    for (;;) {
      bool closing;
      bool retry_admission = false;
      {
        std::lock_guard<std::mutex> lock(state_->mutex_);
        live.splice(live.end(), state_->pending_);
        closing = !state_->accepting_;
        state_->notified_ = false;
      }
      for (auto it = live.begin(); it != live.end(); ++it) {
        auto &task = **it;
        ProgressWork progress(task.progress_);
        try {
          if (task.step_ == Step::Committing) {
            if (task.Finished()) {
              task.step_ = Step::Done;
            }
            continue;
          }
          if (task.step_ == Step::Pending) {
            if (closing) {
              throw MetadataError(MetadataErrorCode::NotReady, "closed before object execution");
            }
            bool blocked = false;
            for (auto prior = live.begin(); prior != it; ++prior) {
              if ((**prior).step_ != Step::Done && Conflicts(task, **prior)) {
                blocked = true;
                break;
              }
            }
            if (blocked) {
              continue;
            }
            Plan(task);
          }
          if (task.step_ == Step::Reading && Assemble(task)) {
            for (size_t i = 0; i < task.changes_.size(); ++i) {
              if (task.changes_[i].deferred_ && task.bytes_[i].source_) {
                const auto *bytes = static_cast<const std::byte *>(task.bytes_[i].Data());
                task.payloads_[i].assign(bytes, bytes + task.bytes_[i].Size());
              }
            }
            task.write_ = io_.WriteCommon(*task.view_, &task.changes_, task.bytes_, task.input_.controls_, Notify());
            // Transfer assembled ownership; do not keep a second uncharged body.
            // Borrowed frames were copied before any Common IO was accepted.
            for (size_t i = 0; i < task.changes_.size(); ++i) {
              if (task.changes_[i].deferred_ && !task.bytes_[i].source_)
                task.payloads_[i] = std::move(task.bytes_[i].owned_);
            }
            task.bytes_.clear();
            task.reads_.clear();
            task.step_ = Step::Writing;
          }
          if (task.step_ == Step::Writing &&
              (!task.write_.batch_ || task.write_.batch_->WaitFor(std::chrono::milliseconds(0)))) {
            io_.FinishCommon(&task.write_);
            task.view_.reset();
            task.step_ = Step::Ready;
          }
          if (task.step_ == Step::Ready) {
            std::lock_guard<std::mutex> lock(state_->mutex_);
            if (!state_->commit_) {
              state_->commit_ = *it;  // Metadata role has accepted ownership.
              task.step_ = Step::Committing;
              state_->changed_.notify_all();
            }
          }
        } catch (...) {
          const auto error = std::current_exception();
          if (Transient(error) && !closing && task.step_ != Step::Writing) {
            retry_admission = true;
            continue;  // No failed submitted IO is retried; only pre-IO admission.
          }
          Fail(task, error);
        }
      }
      live.remove_if([](const auto &task) { return task->step_ == Step::Done; });
      std::unique_lock<std::mutex> lock(state_->mutex_);
      if (closing && live.empty() && state_->pending_.empty()) {
        state_->coordinator_done_ = true;
        state_->changed_.notify_all();
        return;
      }
      if (retry_admission) {
        // External callers may return F02 capacity without a pipeline event.
        state_->changed_.wait_for(lock, options_.retry_interval_, [&] { return state_->notified_; });
      } else {
        state_->changed_.wait(lock, [&] { return state_->notified_; });
      }
    }
  }
  void CommitLoop() {
    for (;;) {
      std::shared_ptr<ObjectTransactionData> task;
      {
        std::unique_lock<std::mutex> lock(state_->mutex_);
        state_->changed_.wait(lock, [&] { return state_->commit_ || state_->coordinator_done_; });
        if (state_->coordinator_done_) {
          return;
        }
        task = state_->commit_;
      }
      if (task) {
        ProgressWork progress(task->progress_);
        JournalResult result{JournalOutcome::NotCommitted, 0, 0, nullptr};
        try {
          if (!task->write_.durable_) {
            throw std::logic_error("metadata commit requires completed data dependencies");
          }
          for (;;) {
            try {
              const auto base = mapping_.Read();
              if (std::any_of(task->changes_.begin(), task->changes_.end(),
                              [](const auto &c) { return c.deferred_; })) {
                auto pending = ObjectMappingAccess::Pending(base, options_.deferred_pending_tasks_ + 1);
                uint64_t bytes = 0;
                size_t count = pending.size();
                for (const auto &p : pending) bytes += p.payload_.ref_.bytes_ - 4;
                for (const auto &c : task->changes_)
                  if (c.deferred_) {
                    count += c.extents_.size();
                    bytes += c.length_;
                  }
                if (count > options_.deferred_pending_tasks_ || bytes > options_.deferred_pending_bytes_)
                  throw MetadataError(MetadataErrorCode::ResourceUnavailable, "Deferred backlog admission exceeded");
              }
              result = ObjectMappingAccess::Apply(mapping_, base, task->changes_,
                                                  task->write_.reservation_ ? &*task->write_.reservation_ : nullptr,
                                                  task->input_.controls_, task->payloads_);
              break;
            } catch (const MetadataViewConflict &) {
              // Rebuild mapping/allocation mutations, rechecking every object
              // version. Never repeat data IO or retry an uncertain commit.
              std::this_thread::sleep_for(options_.retry_interval_);
            }
          }
          if (result.outcome_ != JournalOutcome::Durable) {
            RecordError(result.error_);
          }
        } catch (...) {
          result.error_ = std::current_exception();
        }
        // The allocator retains uncertainty internally. A successful reservation
        // no longer needs its temporary ticket; result observers hold no IO slots.
        task->write_ = {};
        task->Finish(std::move(result));
        {
          std::lock_guard<std::mutex> lock(state_->mutex_);
          state_->deferred_notified_ = true;
          state_->commit_.reset();
          state_->notified_ = true;
          state_->changed_.notify_all();
        }
      }
    }
  }
  void SetStoreMaintenance(std::function<void()> step, bool stop = false) {
    std::unique_lock lock(store_mutex_);
    if (step && store_stopped_) throw std::logic_error("storage maintenance is closed");
    if (step && store_step_) throw std::logic_error("storage already has a Store maintenance owner");
    store_step_ = {};  // No new invocation can acquire the old owner.
    store_idle_.wait(lock, [&] { return !store_running_; });
    if (stop) store_stopped_ = true;
    store_step_ = std::move(step);
  }
  void StoreRound() {
    std::function<void()> step;
    {
      std::lock_guard lock(store_mutex_);
      step = store_step_;
      if (!step) return;
      store_running_ = true;
    }
    try {
      step();  // One bounded Store visit. Never hold registration/state locks across IO.
    } catch (const MetadataCommitBusy &) {
      // Not admitted / explicitly not committed: durable owner remains discoverable.
    } catch (...) {
      RecordError(std::current_exception());
    }
    {
      std::lock_guard lock(store_mutex_);
      store_running_ = false;
      store_idle_.notify_all();
    }
  }
  // F22 execution role. The persisted retirement directory remains the work
  // authority. One candidate per round bounds competition for B; cursor progress
  // includes pinned/blocked candidates, so one old reader cannot starve others.
  void ScrubRound() {
    if (!options_.integrity_scan_) return;
    try {
      if (scrub_metadata_) {
        if (ObjectMappingAccess::ScrubMetadata(mapping_)) scrub_metadata_ = false;
        return;
      }
      if (scrub_) {
        if (!scrub_->WaitFor(std::chrono::milliseconds(0))) return;
        io_.FinishScrub(*scrub_);
        {
          std::lock_guard lock(state_->mutex_);
          if (scrub_journal_)
            integrity_.journal_bytes_ += scrub_->Size();
          else
            integrity_.data_bytes_ += scrub_->Size();
        }
        scrub_.reset();
        scrub_metadata_ = true;
        return;
      }
      const auto view = mapping_.Read();
      const auto candidate = ObjectMappingAccess::ScrubCandidate(view, scrub_cursor_);
      if (candidate.end_ || (!candidate.covered_ && !candidate.span_->journal_)) {
        scrub_cursor_ = candidate.next_;
        std::lock_guard lock(state_->mutex_);
        scrub_metadata_ = true;
        if (candidate.end_)
          ++integrity_.passes_;
        else
          integrity_.uncovered_bytes_ += unit_;
        return;
      }
      try {
        scrub_.emplace(io_.ScrubRead(view, candidate));
      } catch (const ObjectReferenceError &e) {
        if (e.Code() != ObjectReferenceErrorCode::Stale) throw;
        // GC won before this scanner pinned the original allocation.
      }
      scrub_cursor_ = candidate.next_;
      scrub_journal_ = candidate.span_->journal_.has_value();
    } catch (const ObjectIOBusy &) {
      ScrubYield();
    } catch (const MetadataCommitBusy &) {
      ScrubYield();
    } catch (const ObjectReferenceError &e) {
      if (e.Code() == ObjectReferenceErrorCode::Busy)
        ScrubYield();
      else
        RecordError(std::current_exception());
    } catch (...) {
      RecordError(std::current_exception());
    }
  }
  void ScrubYield() {
    std::lock_guard lock(state_->mutex_);
    ++integrity_.yielded_;
  }
  void GarbageLoop() {
    MetadataKey cursor{0, 0, 0};
    ObjectGCPage page{cursor, {}};
    size_t next = 0;
    for (;;) {
      {
        std::unique_lock<std::mutex> lock(state_->mutex_);
        if (state_->changed_.wait_for(lock, options_.gc_interval_,
                                      [&] { return !state_->accepting_ || state_->error_; }))
          return;
      }
      ScrubRound();  // Optional work uses ordinary credits, never ProgressWork reserves.
      StoreRound();
      {
        std::lock_guard lock(state_->mutex_);
        if (!state_->accepting_ || state_->error_) return;
      }
      try {
        ProgressWork progress(true);
        if (next == page.candidates_.size()) {
          page = ObjectMappingAccess::Garbage(mapping_.Read(), cursor);
          cursor = page.next_;
          next = 0;
        }
        if (next == page.candidates_.size()) continue;
        const auto candidate = page.candidates_[next++];
        const auto result = references_.Reclaim(mapping_.Read(), candidate.first, candidate.second);
        if (result.commit_ && result.commit_->outcome_ != JournalOutcome::Durable)
          std::rethrow_exception(result.commit_->error_);
      } catch (const MetadataCommitBusy &) {
        // The durable candidate remains discoverable on the next sweep.
      } catch (const MetadataError &e) {
        if (e.Code() != MetadataErrorCode::Conflict) RecordError(std::current_exception());
      } catch (const ObjectReferenceError &e) {
        if (e.Code() != ObjectReferenceErrorCode::Busy) RecordError(std::current_exception());
      } catch (...) {
        RecordError(std::current_exception());
      }
    }
  }
  void DeferredLoop() {
    ProgressWork progress(true);
    for (;;) {
      {
        std::unique_lock<std::mutex> lock(state_->mutex_);
        state_->changed_.wait(lock, [&] { return state_->deferred_notified_ || !state_->accepting_; });
        if (!state_->accepting_ || state_->error_) return;
        state_->deferred_notified_ = false;
      }
      try {
        for (;;) {
          {
            std::lock_guard<std::mutex> lock(state_->mutex_);
            if (!state_->accepting_) return;
          }
          const auto tasks = ObjectMappingAccess::Pending(mapping_.Read(), 1);
          if (tasks.empty()) break;
          const auto &task = tasks.front();
          if (!memory_->Reserve(task.payload_.ref_.bytes_, true)) {
            RetryDeferred();
            break;
          }
          {
            ResourceCharge memory(memory_, task.payload_.ref_.bytes_, true);
            std::vector<std::byte> body;
            {
              IOReadBudget budget;
              auto read = ObjectMappingAccess::ReadPayload(mapping_, task.payload_, budget, {}, {});
              read.Wait();
              body = DecodeMetadataPayload(read);
            }
            // Release Journal read buffers before acquiring the Data batch.
            auto batch = io_.WriteDeferred(task, body);
            batch.Wait();
            CommonDataWrite done;
            done.batch_.emplace(std::move(batch));
            io_.FinishCommon(&done);
          }
          // Data is durable. Completion metadata needs neither the decoded
          // body nor its credits; retaining them here can block its own retry.
          for (;;) {
            try {
              const auto result = ObjectMappingAccess::Complete(mapping_, mapping_.Read(), task);
              if (result.outcome_ != JournalOutcome::Durable) std::rethrow_exception(result.error_);
              break;
            } catch (const MetadataViewConflict &) {
              continue;  // Rebuild only completion metadata; never repeat accepted device IO.
            } catch (const MetadataCommitBusy &) {
              // Wait below, without repeating accepted device IO.
            } catch (const MetadataError &e) {
              if (e.Code() != MetadataErrorCode::ResourceUnavailable) throw;
              // The Data write is already durable. Wait only for completion
              // metadata; pressure is not corruption or permission to drop it.
            }
            RetryDeferred();
            std::lock_guard<std::mutex> lock(state_->mutex_);
            if (!state_->accepting_) return;
          }
        }
      } catch (const JournalError &e) {
        if (e.Code() != JournalErrorCode::ResourceUnavailable) {
          RecordError(std::current_exception());
          return;
        }
        RetryDeferred();
      } catch (const ObjectIOBusy &) {
        RetryDeferred();
      } catch (const MetadataCommitBusy &) {
        RetryDeferred();
      } catch (const MetadataError &e) {
        if (e.Code() != MetadataErrorCode::ResourceUnavailable) {
          RecordError(std::current_exception());
          return;
        }
        RetryDeferred();
      } catch (...) {
        RecordError(std::current_exception());
        return;
      }
    }
  }
  void RetryDeferred() {
    std::unique_lock<std::mutex> lock(state_->mutex_);
    state_->changed_.wait_for(lock, options_.retry_interval_, [&] { return !state_->accepting_; });
    state_->deferred_notified_ = true;
  }
  void RecordError(std::exception_ptr error) {
    std::lock_guard<std::mutex> lock(state_->mutex_);
    if (!state_->error_) {
      state_->error_ = std::move(error);
    }
    state_->notified_ = true;
    state_->changed_.notify_all();
  }
  void Close() {
    std::call_once(close_, [&] {
      SetStoreMaintenance({}, true);
      {
        std::lock_guard<std::mutex> lock(state_->mutex_);
        state_->accepting_ = false;
        state_->notified_ = true;
      }
      state_->changed_.notify_all();
      coordinator_.join();
      metadata_.join();
      deferred_.join();
      garbage_.join();
    });
  }
  MetadataKey scrub_cursor_{0, 0, 0};
  std::optional<ObjectRead> scrub_;
  bool scrub_journal_{false};
  bool scrub_metadata_{false};
  IntegrityScanStatus integrity_;
  ObjectIO &io_;
  ObjectMappingStore &mapping_;
  ObjectReferenceManager &references_;
  ObjectTransactionOptions options_;
  uint64_t unit_;
  std::shared_ptr<ResourceAccount> memory_;
  std::shared_ptr<State> state_{std::make_shared<State>()};
  std::shared_ptr<RequestBudget> budget_{std::make_shared<RequestBudget>()};
  std::mutex store_mutex_;
  std::condition_variable store_idle_;
  std::function<void()> store_step_;
  bool store_running_{false}, store_stopped_{false};
  std::thread metadata_, coordinator_, deferred_, garbage_;
  std::once_flag close_;
};
ObjectTransactionPipeline::ObjectTransactionPipeline(ObjectIO &io, ObjectMappingStore &mapping,
                                                     ObjectReferenceManager &references,
                                                     ObjectTransactionOptions options,
                                                     std::shared_ptr<ResourceBudget> memory)
    : impl_(std::make_unique<Impl>(io, mapping, references, options, std::move(memory))) {}
ObjectTransactionPipeline::~ObjectTransactionPipeline() = default;
auto ObjectTransactionPipeline::Submit(ObjectTransaction &transaction) -> ObjectTransactionSubmission {
  return impl_->Submit(transaction);
}
auto ObjectTransactionPipeline::IntegrityStatus() const -> IntegrityScanStatus {
  std::lock_guard lock(impl_->state_->mutex_);
  return impl_->integrity_;
}
auto ObjectTransactionPipeline::Error() const -> std::exception_ptr {
  std::lock_guard<std::mutex> lock(impl_->state_->mutex_);
  return impl_->state_->error_;
}
void ObjectTransactionPipeline::SetStoreMaintenance(std::function<void()> step) {
  impl_->SetStoreMaintenance(std::move(step));
}
void ObjectTransactionPipeline::StopStoreMaintenance() { impl_->SetStoreMaintenance({}, true); }
void ObjectTransactionPipeline::Close() { impl_->Close(); }
}  // namespace bustub
