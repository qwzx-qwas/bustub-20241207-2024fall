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
  size_t requests_{0};
  uint64_t bytes_{0};
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
      --budget_->requests_;
      budget_->bytes_ -= charge_;
    }
  }
  void Finish(JournalResult result) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto &op : input_.objects_) {
      op.source_.reset();  // Release frame permission before publishing completion.
      std::vector<std::byte>().swap(op.bytes_);
    }
    bytes_.clear();
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
  ObjectTransaction input_;
  Step step_{Step::Pending};  // Written only by the coordinator.
  std::optional<ObjectMappingSnapshot> view_;
  std::vector<ObjectChange> changes_;
  std::vector<CommonInput> bytes_;
  std::vector<std::array<std::optional<ObjectRead>, 2>> reads_;
  std::vector<std::array<bool, 2>> read_done_;
  std::vector<bool> assembled_;
  CommonDataWrite write_;
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
    bool accepting_{true}, coordinator_done_{false}, notified_{false};
    std::exception_ptr error_;
    void Notify() {
      std::lock_guard<std::mutex> lock(mutex_);
      notified_ = true;
      changed_.notify_all();
    }
  };
  Impl(ObjectIO &io, ObjectMappingStore &mapping, ObjectReferenceManager &references, ObjectTransactionOptions options)
      : io_(io),
        mapping_(mapping),
        references_(references),
        options_(options),
        unit_(ObjectMappingAccess::Unit(mapping)) {
    if (options.max_requests_ == 0 || options.max_operations_ == 0 || options.max_request_bytes_ == 0 ||
        options.max_pending_bytes_ == 0 || options.retry_interval_.count() <= 0 || options.gc_interval_.count() <= 0) {
      throw std::invalid_argument("object transactions require positive explicit budgets");
    }
    metadata_ = std::thread([this] { CommitLoop(); });
    try {
      coordinator_ = std::thread([this] { Drive(); });
    } catch (...) {
      {
        std::lock_guard<std::mutex> lock(state_->mutex_);
        state_->accepting_ = false;
        state_->coordinator_done_ = true;
      }
      state_->changed_.notify_all();
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
      if (op.operation_ < ObjectOperation::Create || op.operation_ > ObjectOperation::Remove) {
        throw std::invalid_argument("unknown object operation");
      }
      if (op.source_ && (!op.bytes_.empty() || !op.source_->data_ || !op.source_->owner_ ||
                         op.source_->size_ > op.source_->capacity_)) {
        throw std::invalid_argument("invalid borrowed object source");
      }
      account(op.Size());
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
    }
    if (charge > options_.max_pending_bytes_) {
      throw std::invalid_argument("one object transaction exceeds pending byte budget");
    }
    auto task = std::make_shared<ObjectTransactionData>();
    task->charge_ = charge;
    std::lock_guard<std::mutex> lock(state_->mutex_);
    if (!state_->accepting_ || state_->error_) {
      return {IOAdmission::Stopped, std::nullopt};
    }
    {
      std::lock_guard<std::mutex> budget_lock(budget_->mutex_);
      if (budget_->requests_ == options_.max_requests_ || charge > options_.max_pending_bytes_ - budget_->bytes_) {
        return {IOAdmission::Full, std::nullopt};
      }
      ++budget_->requests_;
      budget_->bytes_ += charge;
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
        // One small RMW reads its unit once. A large COW reads only the two
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
            task.write_ = io_.WriteCommon(&task.changes_, task.bytes_, Notify());
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
    MetadataKey cursor{0, 0, 0};
    auto next_gc = std::chrono::steady_clock::now() + options_.gc_interval_;
    for (;;) {
      std::shared_ptr<ObjectTransactionData> task;
      bool gc = false;
      {
        std::unique_lock<std::mutex> lock(state_->mutex_);
        state_->changed_.wait_until(lock, next_gc, [&] { return state_->commit_ || state_->coordinator_done_; });
        if (state_->coordinator_done_) {
          return;
        }
        task = state_->commit_;
        gc = state_->accepting_ && std::chrono::steady_clock::now() >= next_gc;
      }
      if (task) {
        JournalResult result{JournalOutcome::NotCommitted, 0, 0, nullptr};
        try {
          if (!task->write_.durable_) {
            throw std::logic_error("metadata commit requires completed data dependencies");
          }
          for (;;) {
            try {
              const auto base = mapping_.Read();
              result = ObjectMappingAccess::Apply(mapping_, base, task->changes_,
                                                  task->write_.reservation_ ? &*task->write_.reservation_ : nullptr,
                                                  task->input_.controls_);
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
          state_->commit_.reset();
          state_->notified_ = true;
          state_->changed_.notify_all();
        }
      }
      if (gc) {
        try {
          auto page = ObjectMappingAccess::Garbage(mapping_.Read(), cursor);
          cursor = page.next_;
          for (const auto &[object, allocation] : page.candidates_) {
            const auto result = references_.Reclaim(mapping_.Read(), object, allocation);
            if (result.commit_ && result.commit_->outcome_ != JournalOutcome::Durable) {
              std::rethrow_exception(result.commit_->error_);
            }
          }
        } catch (const MetadataError &e) {
          if (e.Code() != MetadataErrorCode::Conflict && e.Code() != MetadataErrorCode::ResourceUnavailable) {
            RecordError(std::current_exception());
          }
        } catch (const ObjectReferenceError &e) {
          if (e.Code() != ObjectReferenceErrorCode::Busy) {
            RecordError(std::current_exception());
          }
        } catch (...) {
          RecordError(std::current_exception());
        }
      }
      if (gc || std::chrono::steady_clock::now() >= next_gc) {
        next_gc = std::chrono::steady_clock::now() + options_.gc_interval_;
      }
    }
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
      {
        std::lock_guard<std::mutex> lock(state_->mutex_);
        state_->accepting_ = false;
        state_->notified_ = true;
      }
      state_->changed_.notify_all();
      coordinator_.join();
      metadata_.join();
    });
  }
  ObjectIO &io_;
  ObjectMappingStore &mapping_;
  ObjectReferenceManager &references_;
  ObjectTransactionOptions options_;
  uint64_t unit_;
  std::shared_ptr<State> state_{std::make_shared<State>()};
  std::shared_ptr<RequestBudget> budget_{std::make_shared<RequestBudget>()};
  std::thread metadata_, coordinator_;
  std::once_flag close_;
};
ObjectTransactionPipeline::ObjectTransactionPipeline(ObjectIO &io, ObjectMappingStore &mapping,
                                                     ObjectReferenceManager &references,
                                                     ObjectTransactionOptions options)
    : impl_(std::make_unique<Impl>(io, mapping, references, options)) {}
ObjectTransactionPipeline::~ObjectTransactionPipeline() = default;
auto ObjectTransactionPipeline::Submit(ObjectTransaction &transaction) -> ObjectTransactionSubmission {
  return impl_->Submit(transaction);
}
auto ObjectTransactionPipeline::Error() const -> std::exception_ptr {
  std::lock_guard<std::mutex> lock(impl_->state_->mutex_);
  return impl_->state_->error_;
}
void ObjectTransactionPipeline::Close() { impl_->Close(); }
}  // namespace bustub
