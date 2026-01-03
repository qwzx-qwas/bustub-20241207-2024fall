//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// executor_context.h
//
// Identification: src/include/execution/executor_context.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <deque>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "execution/check_options.h"
#include "execution/executors/abstract_executor.h"
#include "storage/page/tmp_tuple_page.h"

namespace bustub {
class AbstractExecutor;
/**
 * ExecutorContext stores all the context necessary to run an executor.
 * ExecutorContext 存储运行执行器所需的所有上下文。
 */
class ExecutorContext {
 public:
  /**
   * Creates an ExecutorContext for the transaction that is executing the query.
   * 为执行查询的事务创建一个 ExecutorContext。
   * @param transaction The transaction executing the query
   * @param catalog The catalog that the executor uses
   * @param bpm The buffer pool manager that the executor uses
   * @param txn_mgr The transaction manager that the executor uses
   * @param lock_mgr The lock manager that the executor uses
   */
  ExecutorContext(Transaction *transaction, Catalog *catalog, BufferPoolManager *bpm, TransactionManager *txn_mgr,
                  LockManager *lock_mgr, bool is_delete)
      : transaction_(transaction),
        catalog_{catalog},
        bpm_{bpm},
        txn_mgr_(txn_mgr),
        lock_mgr_(lock_mgr),
        is_delete_(is_delete) {
    nlj_check_exec_set_ = std::deque<std::pair<AbstractExecutor *, AbstractExecutor *>>(
        std::deque<std::pair<AbstractExecutor *, AbstractExecutor *>>{});
    check_options_ = std::make_shared<CheckOptions>();
  }

  ~ExecutorContext() = default;

  DISALLOW_COPY_AND_MOVE(ExecutorContext);

  /** @return the running transaction */
  /** @return 正在运行的事务 */
  auto GetTransaction() const -> Transaction * { return transaction_; }

  /** @return the catalog */
  /** @return 目录 */
  auto GetCatalog() -> Catalog * { return catalog_; }

  /** @return the buffer pool manager */
  /** @return 缓冲池管理器 */
  auto GetBufferPoolManager() -> BufferPoolManager * { return bpm_; }

  /** @return the log manager - don't worry about it for now */
  /** @return 日志管理器 - 暂时不用担心 */
  auto GetLogManager() -> LogManager * { return nullptr; }

  /** @return the lock manager */
  /** @return 锁管理器 */
  auto GetLockManager() -> LockManager * { return lock_mgr_; }

  /** @return the transaction manager */
  /** @return 事务管理器 */
  auto GetTransactionManager() -> TransactionManager * { return txn_mgr_; }

  /** @return the set of nlj check executors */
  /** @return NLJ 检查执行器集合 */
  auto GetNLJCheckExecutorSet() -> std::deque<std::pair<AbstractExecutor *, AbstractExecutor *>> & {
    return nlj_check_exec_set_;
  }

  /** @return the check options */
  /** @return 检查选项 */
  auto GetCheckOptions() -> std::shared_ptr<CheckOptions> { return check_options_; }

  void AddCheckExecutor(AbstractExecutor *left_exec, AbstractExecutor *right_exec) {
    nlj_check_exec_set_.emplace_back(left_exec, right_exec);
  }

  void InitCheckOptions(std::shared_ptr<CheckOptions> &&check_options) {
    BUSTUB_ASSERT(check_options, "nullptr");
    check_options_ = std::move(check_options);
  }

  /** As of Fall 2023, this function should not be used. */
  /** 截至 2023 年秋季，不应使用此函数。 */
  auto IsDelete() const -> bool { return is_delete_; }

 private:
  /** The transaction context associated with this executor context */
  /** 与此执行器上下文关联的事务上下文 */
  Transaction *transaction_;
  /** The database catalog associated with this executor context */
  /** 与此执行器上下文关联的数据库目录 */
  Catalog *catalog_;
  /** The buffer pool manager associated with this executor context */
  /** 与此执行器上下文关联的缓冲池管理器 */
  BufferPoolManager *bpm_;
  /** The transaction manager associated with this executor context */
  /** 与此执行器上下文关联的事务管理器 */
  TransactionManager *txn_mgr_;
  /** The lock manager associated with this executor context */
  /** 与此执行器上下文关联的锁管理器 */
  LockManager *lock_mgr_;
  /** The set of NLJ check executors associated with this executor context */
  /** 与此执行器上下文关联的 NLJ 检查执行器集合 */
  std::deque<std::pair<AbstractExecutor *, AbstractExecutor *>> nlj_check_exec_set_;
  /** The set of check options associated with this executor context */
  /** 与此执行器上下文关联的检查选项集合 */
  std::shared_ptr<CheckOptions> check_options_;
  bool is_delete_;
};

}  // namespace bustub
