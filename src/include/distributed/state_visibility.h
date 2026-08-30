//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// state_visibility.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>  // NOLINT(build/c++11)
#include <shared_mutex>

namespace bustub {

/** Global V1 publication barrier shared by SQL reads, FSM Apply, snapshots, and MVCC GC. */
class StateVisibilityLatch {
 public:
  auto LockShared() -> std::shared_lock<std::shared_mutex> { return std::shared_lock(mutex_); }
  auto LockExclusive() -> std::unique_lock<std::shared_mutex> { return std::unique_lock(mutex_); }

 private:
  std::shared_mutex mutex_;
};

}  // namespace bustub
