#pragma once

#include <map>

#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * @brief tracks all the read timestamps.
 *
 */
class Watermark {
 public:
  explicit Watermark(timestamp_t commit_ts) : commit_ts_(commit_ts), watermark_(commit_ts) {}

  auto AddTxn(timestamp_t read_ts) -> void;

  auto RemoveTxn(timestamp_t read_ts) -> void;

  /** The caller should update commit ts before removing the txn from the watermark so that we can track watermark
   * correctly. */
  auto UpdateCommitTs(timestamp_t commit_ts) -> void;

  auto GetWatermark() -> timestamp_t {
    if (current_reads_.empty()) {
      return commit_ts_;
    }
    return watermark_;
  }

  // 当前最新的提交时间戳
  timestamp_t commit_ts_;

  // 所有活跃事务中最小的read_ts
  timestamp_t watermark_;

  // 记录所有活跃 read_ts；有序计数表使最小值可在 O(1) 读取、增删为 O(log n)。
  std::map<timestamp_t, int> current_reads_;
};

};  // namespace bustub
