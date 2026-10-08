// Node-owned byte credits. Owners keep local limits and actual resource lifetimes.
#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

namespace bustub {

struct ResourceBudgetOptions {
  size_t bytes_;
  size_t grant_bytes_;
  size_t progress_bytes_;
};

// Only synchronous preparation inherits this class. Accepted IO captures it;
// completion never depends on the thread on which it eventually runs.
class ProgressWork {
 public:
  explicit ProgressWork(bool enable) : previous_(active_) { active_ = active_ || enable; }
  ~ProgressWork() { active_ = previous_; }
  ProgressWork(const ProgressWork &) = delete;
  auto operator=(const ProgressWork &) -> ProgressWork & = delete;
  static auto Active() -> bool { return active_; }

 private:
  bool previous_;
  inline static thread_local bool active_{false};
};

class ResourceAccount;
class ResourceBudget {
 public:
  explicit ResourceBudget(ResourceBudgetOptions options) : options_(options) {
    if (options.bytes_ == 0 || options.grant_bytes_ == 0 || options.progress_bytes_ >= options.bytes_) {
      throw std::invalid_argument("invalid shared memory budget");
    }
  }
  // Node status hint, not the cause/result of any particular admission attempt.
  auto Pressure() const -> bool { return pressure_.load(std::memory_order_relaxed); }

 private:
  friend class ResourceAccount;
  auto Grant(size_t minimum, bool progress) -> size_t {
    std::lock_guard<std::mutex> lock(mutex_);
    auto free = options_.bytes_ - grants_[0] - grants_[1];
    if (!progress) free = std::min(free, options_.bytes_ - options_.progress_bytes_ - grants_[0]);
    if (minimum > free) {
      pressure_.store(true, std::memory_order_relaxed);
      return 0;
    }
    const auto result = std::min(free, std::max(minimum, options_.grant_bytes_));
    grants_[progress] += result;
    return result;
  }
  void Return(size_t bytes, bool progress) {
    std::lock_guard<std::mutex> lock(mutex_);
    grants_[progress] -= bytes;
    if (bytes != 0) pressure_.store(false, std::memory_order_relaxed);
  }
  void ReclaimIdle();
  ResourceBudgetOptions options_;
  std::mutex mutex_;
  std::vector<std::weak_ptr<ResourceAccount>> accounts_;
  size_t grants_[2]{0, 0};
  std::atomic<bool> pressure_{false};
};

// Lock order is account -> node; the node never calls an account while locked.
// Releases keep a small local cache of credits. Pressure reclaims unused grants
// from other accounts, without ever freeing their live memory.
class ResourceAccount {
 public:
  static auto Create(std::shared_ptr<ResourceBudget> budget) -> std::shared_ptr<ResourceAccount> {
    auto result = std::shared_ptr<ResourceAccount>(new ResourceAccount(budget));
    if (budget) {
      std::lock_guard<std::mutex> lock(budget->mutex_);
      auto &accounts = budget->accounts_;
      accounts.erase(std::remove_if(accounts.begin(), accounts.end(), [](const auto &p) { return p.expired(); }),
                     accounts.end());
      accounts.push_back(result);
    }
    return result;
  }
  ~ResourceAccount() {
    if (used_[0] != 0 || used_[1] != 0) std::terminate();
    if (budget_) {
      budget_->Return(grants_[0], false);
      budget_->Return(grants_[1], true);
    }
  }
  auto Reserve(size_t bytes, bool progress) -> bool {
    if (!budget_ || bytes == 0) return true;
    const auto capacity = budget_->options_.bytes_ - (progress ? 0 : budget_->options_.progress_bytes_);
    if (bytes > capacity) {
      throw std::invalid_argument("request exceeds shared memory capacity for its work class");
    }
    for (int attempt = 0; attempt != 2; ++attempt) {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto free = grants_[progress] - used_[progress];
        if (bytes > free) {
          const auto extra = budget_->Grant(bytes - free, progress);
          grants_[progress] += extra;
        }
        if (bytes <= grants_[progress] - used_[progress]) {
          used_[progress] += bytes;
          budget_->pressure_.store(false, std::memory_order_relaxed);
          return true;
        }
      }
      if (attempt == 0) budget_->ReclaimIdle();
    }
    return false;
  }
  void Release(size_t bytes, bool progress) {
    if (!budget_ || bytes == 0) return;
    std::lock_guard<std::mutex> lock(mutex_);
    if (bytes > used_[progress]) std::terminate();
    used_[progress] -= bytes;
    const auto free = grants_[progress] - used_[progress];
    const auto quantum = budget_->options_.grant_bytes_;
    const auto release = free > quantum ? (free - quantum) / quantum * quantum : 0;
    grants_[progress] -= release;
    if (release != 0) budget_->Return(release, progress);
  }

 private:
  friend class ResourceBudget;
  explicit ResourceAccount(std::shared_ptr<ResourceBudget> budget) : budget_(std::move(budget)) {}
  void TrimIdle() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (size_t lane = 0; lane < 2; ++lane) {
      const auto free = grants_[lane] - used_[lane];
      grants_[lane] -= free;
      budget_->Return(free, lane != 0);
    }
  }
  std::shared_ptr<ResourceBudget> budget_;
  std::mutex mutex_;
  size_t grants_[2]{0, 0};
  size_t used_[2]{0, 0};
};

inline void ResourceBudget::ReclaimIdle() {
  std::vector<std::weak_ptr<ResourceAccount>> accounts;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    accounts = accounts_;
  }
  for (const auto &weak : accounts) {
    if (auto account = weak.lock()) account->TrimIdle();
  }
}

// Follows the actual memory owner, including retained results and old B views.
class ResourceCharge {
 public:
  ResourceCharge() = default;
  ResourceCharge(std::shared_ptr<ResourceAccount> account, size_t bytes, bool progress)
      : account_(std::move(account)), bytes_(bytes), progress_(progress) {}
  ~ResourceCharge() { Reset(); }
  ResourceCharge(const ResourceCharge &) = delete;
  auto operator=(const ResourceCharge &) -> ResourceCharge & = delete;
  ResourceCharge(ResourceCharge &&other) noexcept
      : account_(std::move(other.account_)), bytes_(std::exchange(other.bytes_, 0)), progress_(other.progress_) {}
  auto operator=(ResourceCharge &&other) noexcept -> ResourceCharge & {
    if (this != &other) {
      Reset();
      account_ = std::move(other.account_);
      bytes_ = std::exchange(other.bytes_, 0);
      progress_ = other.progress_;
    }
    return *this;
  }
  void Reset() {
    if (account_) account_->Release(bytes_, progress_);
    account_.reset();
    bytes_ = 0;
  }

 private:
  std::shared_ptr<ResourceAccount> account_;
  size_t bytes_{0};
  bool progress_{false};
};
}  // namespace bustub
