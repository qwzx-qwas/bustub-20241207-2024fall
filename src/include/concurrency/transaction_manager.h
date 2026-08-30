//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.h
//
// Identification: src/include/concurrency/transaction_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

#include "catalog/schema.h"
#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/watermark.h"
#include "recovery/log_manager.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * TransactionManager keeps track of all the transactions running in the system.
 * 事务管理器负责跟踪系统中所有正在运行的事务。
 */
class TransactionManager {
 public:
  TransactionManager() = default;
  ~TransactionManager() = default;

  /**
   * Begins a new transaction.
   * 开始一个新事务。
   * @param isolation_level an optional isolation level of the transaction.
   *                        事务的可选隔离级别。
   * @return an initialized transaction
   *         一个初始化的事务。
   */
  auto Begin(IsolationLevel isolation_level = IsolationLevel::SNAPSHOT_ISOLATION) -> Transaction *;

  /**
   * Begins a read transaction at an externally selected committed timestamp. Distributed callers pass a Raft-derived
   * published applied index instead of allocating a node-local timestamp.
   */
  auto BeginReadAt(timestamp_t read_ts, IsolationLevel isolation_level = IsolationLevel::SNAPSHOT_ISOLATION)
      -> Transaction *;

  /** Finish a read-only transaction without allocating or advancing a node-local commit timestamp. */
  void EndRead(Transaction *txn);

  /**
   * Commits a transaction.
   * 提交一个事务。
   * @param txn the transaction to commit, the txn will be managed by the txn manager so no need to delete it by
   * yourself
   */
  auto Commit(Transaction *txn) -> bool;

  /**
   * Aborts a transaction
   * 中止一个事务。
   * @param txn the transaction to abort, the txn will be managed by the txn manager so no need to delete it by yourself
   *            要中止的事务，该事务将由事务管理器管理，因此无需手动删除。
   */
  void Abort(Transaction *txn);

  /**
   * @brief Update an undo link that links table heap tuple to the first undo log.
   *        更新一个撤销链接，该链接将表堆元组链接到第一个撤销日志。
   * Before updating, `check` function will be called to ensure validity.
   * 更新之前，将调用 `check` 函数以确保有效性。
   */
  auto UpdateUndoLink(RID rid, std::optional<UndoLink> prev_link,
                      std::function<bool(std::optional<UndoLink>)> &&check = nullptr) -> bool;

  /** @brief Get the first undo log of a table heap tuple. */
  /** @brief 获取表堆元组的第一个撤销日志。 */
  auto GetUndoLink(RID rid) -> std::optional<UndoLink>;

  /** @brief Access the transaction undo log buffer and get the undo log. Return nullopt if the txn does not exist. Will
   *         访问事务撤销日志缓冲区并获取撤销日志。如果事务不存在，则返回 nullopt。
   * still throw an exception if the index is out of range.
   * 如果索引超出范围，仍将抛出异常。
   */
  auto GetUndoLogOptional(UndoLink link) -> std::optional<UndoLog>;

  /** @brief Access the transaction undo log buffer and get the undo log. Except when accessing the current txn buffer,
   *         访问事务撤销日志缓冲区并获取撤销日志。除非访问当前事务缓冲区，
   * you should always call this function to get the undo log instead of manually retrieve the txn shared_ptr and access
   * 否则应始终调用此函数以获取撤销日志，而不是手动检索事务 shared_ptr 并访问
   * the buffer.
   * 缓冲区。
   */
  auto GetUndoLog(UndoLink link) -> UndoLog;

  /** @brief Get the lowest read timestamp in the system. */
  /** @brief 获取系统中的最低读取时间戳。 */
  auto GetWatermark() -> timestamp_t {
    std::shared_lock lock(txn_map_mutex_);
    return running_txns_.GetWatermark();
  }

  /** @brief Stop-the-world garbage collection. Will be called only when all transactions are not accessing the table
   *         停止世界垃圾回收。仅当所有事务都未访问表堆时才会调用。
   * heap. */
  /** 表堆。 */
  void GarbageCollection();

  /** protects txn map */
  /** 保护事务映射。 */
  std::shared_mutex txn_map_mutex_;
  /** All transactions, running or committed */
  /** 所有事务，包括运行中或已提交的事务。 */
  std::unordered_map<txn_id_t, std::shared_ptr<Transaction>> txn_map_;

  struct PageVersionInfo {
    /** protects the map */
    /** 保护映射。 */
    std::shared_mutex mutex_;
    /** Stores previous version info for all slots. Note: DO NOT use `[x]` to access it because
     *  存储所有槽的先前版本信息。注意：不要使用 `[x]` 访问它，因为
     * it will create new elements even if it does not exist. Use `find` instead.
     * 即使它不存在，也会创建新元素。请改用 `find`。
     */
    std::unordered_map<slot_offset_t, UndoLink> prev_link_;
  };

  /** protects version info */
  /** 保护版本信息。 */
  std::shared_mutex version_info_mutex_;
  /** Stores the previous version of each tuple in the table heap. Do not directly access this field. Use the helper
   *  存储表堆中每个元组的先前版本。不要直接访问此字段。请使用
   * functions in `transaction_manager_impl.cpp`.
   * `transaction_manager_impl.cpp` 中的辅助函数。
   */
  std::unordered_map<page_id_t, std::shared_ptr<PageVersionInfo>> version_info_;

  /** Stores all the read_ts of running txns so as to facilitate garbage collection. */
  /** 存储所有运行中事务的 read_ts，以便于垃圾回收。 */
  Watermark running_txns_{0};

  /** Only one txn is allowed to commit at a time */
  /** 一次只允许一个事务提交。 */
  std::mutex commit_mutex_;
  /** The last committed timestamp. */
  /** 最后提交的时间戳。 */
  std::atomic<timestamp_t> last_commit_ts_{0};

  /** Catalog */
  /** 目录。 */
  Catalog *catalog_;

  std::atomic<txn_id_t> next_txn_id_{TXN_START_ID};

 private:
  /** @brief Verify if a txn satisfies serializability. We will not test this function and you can change / remove it as
   *         验证事务是否满足可串行化。我们不会测试此功能，您可以根据需要更改/删除它。
   * you want. */
  auto VerifyTxn(Transaction *txn) -> bool;
};

/**
 * @brief Update the tuple and its undo link in the table heap atomically.
 *        在表堆中以原子方式更新元组及其撤销链接。
 */
auto UpdateTupleAndUndoLink(
    TransactionManager *txn_mgr, RID rid, std::optional<UndoLink> undo_link, TableHeap *table_heap, Transaction *txn,
    const TupleMeta &meta, const Tuple &tuple,
    std::function<bool(const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink>)> &&check = nullptr)
    -> bool;

/**
 * @brief Get the tuple and its undo link in the table heap atomically.
 *        在表堆中以原子方式获取元组及其撤销链接。
 */
auto GetTupleAndUndoLink(TransactionManager *txn_mgr, TableHeap *table_heap, RID rid)
    -> std::tuple<TupleMeta, Tuple, std::optional<UndoLink>>;

}  // namespace bustub
