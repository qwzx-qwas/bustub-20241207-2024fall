#pragma once

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "storage/disk/resource_budget.h"

namespace bustub {

// A permit follows the result, including completed results not yet consumed.
// Executor destruction drains accepted work; discarded callers cannot free a
// task's inputs while it is still using them.
class TaskExecutor {
 private:
  struct Permit {
    ResourceCharge charge_;
    std::shared_ptr<std::atomic<size_t>> outstanding_;
    ~Permit() { outstanding_->fetch_sub(1); }
  };

 public:
  template <typename T>
  struct Result {
    auto Ready() const -> bool { return ready_.load(std::memory_order_acquire); }
    auto Take() -> T {
      if (error_) std::rethrow_exception(error_);
      return std::move(*value_);
    }

   private:
    friend class TaskExecutor;
    std::shared_ptr<Permit> permit_;
    std::optional<T> value_;
    std::exception_ptr error_;
    std::atomic<bool> ready_{false};
  };

  TaskExecutor(size_t threads, size_t limit, std::shared_ptr<ResourceBudget> memory, std::function<void()> wake,
               size_t progress_slots = 0)
      : limit_(limit),
        progress_slots_(progress_slots),
        wake_(std::move(wake)),
        memory_(ResourceAccount::Create(std::move(memory))) {
    try {
      for (size_t i = 0; i < threads; ++i) threads_.emplace_back([this] { Run(); });
    } catch (...) {
      Close();
      throw;
    }
  }
  ~TaskExecutor() { Close(); }
  void Drain() {
    std::unique_lock lock(mutex_);
    idle_.wait(lock, [&] { return queue_.empty() && executing_ == 0; });
  }
  TaskExecutor(const TaskExecutor &) = delete;
  auto operator=(const TaskExecutor &) -> TaskExecutor & = delete;

  // work is moved only after admission. The caller can retain it on Full.
  template <typename F>
  auto Submit(size_t bytes, bool progress, F &&work) -> std::shared_ptr<Result<std::invoke_result_t<F>>> {
    using T = std::invoke_result_t<F>;
    static_assert(!std::is_void_v<T>);
    std::lock_guard lock(mutex_);
    if (closing_ || outstanding_->load() >= (progress ? limit_ : limit_ - progress_slots_)) return {};
    if (!memory_->Reserve(bytes, progress)) return {};
    ResourceCharge charge(memory_, bytes, progress);
    auto permit = std::make_shared<Permit>();
    permit->outstanding_ = outstanding_;
    outstanding_->fetch_add(1);
    permit->charge_ = std::move(charge);
    auto result = std::make_shared<Result<T>>();
    result->permit_ = std::move(permit);
    auto execute = [result, progress, work = std::forward<F>(work)]() mutable {
      ProgressWork progress_work(progress);
      try {
        result->value_.emplace(work());
      } catch (...) {
        result->error_ = std::current_exception();
      }
      result->ready_.store(true, std::memory_order_release);
    };
    auto position = progress ? std::find_if(queue_.begin(), queue_.end(), [](const auto &item) { return !item.first; })
                             : queue_.end();
    queue_.emplace(position, progress, std::move(execute));
    ready_.notify_one();
    return result;
  }

 private:
  void Close() {
    {
      std::lock_guard lock(mutex_);
      closing_ = true;
    }
    ready_.notify_all();
    for (auto &thread : threads_)
      if (thread.joinable()) thread.join();
  }
  void Run() {
    for (;;) {
      std::function<void()> work;
      {
        std::unique_lock lock(mutex_);
        ready_.wait(lock, [&] { return closing_ || !queue_.empty(); });
        if (queue_.empty()) return;
        work = std::move(queue_.front().second);
        queue_.pop_front();
        ++executing_;
      }
      work();
      work = {};
      if (wake_) wake_();
      {
        std::lock_guard lock(mutex_);
        --executing_;
      }
      idle_.notify_all();
    }
  }
  const size_t limit_, progress_slots_;
  std::function<void()> wake_;
  std::shared_ptr<ResourceAccount> memory_;
  std::shared_ptr<std::atomic<size_t>> outstanding_{std::make_shared<std::atomic<size_t>>(0)};
  std::mutex mutex_;
  std::condition_variable ready_, idle_;
  size_t executing_{0};
  std::deque<std::pair<bool, std::function<void()>>> queue_;
  bool closing_{false};
  std::vector<std::thread> threads_;
};
}  // namespace bustub
