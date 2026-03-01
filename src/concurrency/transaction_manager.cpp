//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  // 因为last_commit_ts_实际上记录的是已经发生过的、最新一次提交的时间戳。
  // 就是每进行一次提交，吐出的下一个时间戳。
  txn_ref->read_ts_ = last_commit_ts_.load();
  txn_ref->commit_ts_ = 0;

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

//OCC的向后验证
auto TransactionManager::VerifyTxn(Transaction *txn) -> bool {
  if (txn->GetIsolationLevel() != IsolationLevel::SERIALIZABLE) {
    return true;
  }

  // 1. 检查是否为读写事务。如果写集合为空，视为只读且无需验证
  if (txn->GetWriteSets().empty()) {
    return true;
  }

  auto read_ts = txn->GetReadTs();
  const auto &scan_predicates = txn->GetScanPredicates();
  if (scan_predicates.empty()) {
    return true;
  }

  // 2. 找到该事务开始后提交的所有事务
  std::vector<Transaction *> committed_txns;
  {
    std::shared_lock<std::shared_mutex> lck(txn_map_mutex_);
    for (const auto &pair : txn_map_) {
      auto *t = pair.second.get();
      // 如果 t->GetCommitTs() > read_ts，
      // 说明 t 所做的修改在当前事务的快照之后。
      // 这些修改对当前事务来说是“未来的改动”，是潜在的冲突源。
      if (t->GetTransactionState() == TransactionState::COMMITTED && t->GetCommitTs() > read_ts &&
          t->GetTransactionId() != txn->GetTransactionId()) {
        committed_txns.push_back(t);
      }
    }
  }

  if (committed_txns.empty()) {
    return true;
  }

  // 3. 将这些冲突事务的write_set（这些竞争事务修改过的所有行）汇总到一个conflict_rids集合中去
  std::unordered_map<table_oid_t, std::unordered_set<RID>> conflict_rids;
  for (auto *t : committed_txns) {
    for (const auto &[table_oid, rids] : t->GetWriteSets()) {
      conflict_rids[table_oid].insert(rids.begin(), rids.end());
    }
  }

  // 4. 对其中的每一个记录沿着版本链去判断
  for (const auto &[table_oid, rids] : conflict_rids) {
    if (scan_predicates.find(table_oid) == scan_predicates.end()) {
      continue;
    }

    auto table_info = catalog_->GetTable(table_oid).get();
    const auto &schema = table_info->schema_;
    const auto &preds = scan_predicates.at(table_oid);

    for (const auto &rid : rids) {
      auto [base_meta, base_tuple] = table_info->table_->GetTuple(rid);
      auto undo_link = GetUndoLink(rid);

      auto logs_opt = CollectUndoLogs(rid, base_meta, base_tuple, undo_link, txn, this);
      
      //检查最新版本
      // 检查该RID在tableHeap中的最新版本是否是非删除的
      if (!base_meta.is_deleted_) {
        for (const auto &pred : preds) {
          // 如果扫描时没有where条件，则pred为nullptr
          // 将谓词逻辑应用于base_tuple上，判断是否满足条件
          if (!pred || pred->Evaluate(&base_tuple, schema).GetAs<bool>()) {
            return false;
          }
        }
      }

      // 检查历史版本
      if (logs_opt.has_value()) {
        auto logs = std::move(*logs_opt);
        for (size_t i = 1; i <= logs.size(); ++i) {
          std::vector<UndoLog> sub_logs;
          for (size_t j = 0; j < i; ++j) {
            sub_logs.push_back(logs[j]);
          }
          auto reconstructed = ReconstructTuple(&schema, base_tuple, base_meta, sub_logs);
          if (reconstructed.has_value()) {
            for (const auto &pred : preds) {
              if (!pred || pred->Evaluate(&*reconstructed, schema).GetAs<bool>()) {
                return false;
              }
            }
          }
        }
      }
    }
  }

  return true;
}
/*auto TransactionManager::Commit(Transaction *txn) -> bool {
  //确保提交过程是线程安全的
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  //获取提交时间戳，并更新下一个可用的提交时间戳
  timestamp_t commit_ts = last_commit_ts_.fetch_add(1) + 1;
  txn->commit_ts_ = commit_ts;

  // last_commit_ts_ = txn->commit_ts_.load();

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  // 更新基础元组
  // 遍历写集合
  for (const auto &write_entry : txn->GetWriteSets()) {
    auto table_oid = write_entry.first;
    auto rid_set = write_entry.second;
    // 获取表信息
    auto table_info = catalog_->GetTable(table_oid).get();
    auto table_heap = table_info->table_.get();

    for (const auto &rid : rid_set) {

      // 获取当前元组及其元数据
      auto [meta, tuple] = table_heap->GetTuple(rid);
      // 更新元数据的提交时间戳
      meta.ts_ = commit_ts;
      // 原子地更新元组元数据
      table_heap->UpdateTupleMeta(meta, rid);



      // 原子地读取当前元组及其 undo link
      auto [base_meta, base_tuple, base_undo_link] = GetTupleAndUndoLink(this, table_heap, rid);

      // 期望 base_meta.ts_ 指示当前事务对该元组的占用（即事务 id）
      txn_id_t expect_txn_id = txn->GetTransactionId();

      // 构造要写回的新 meta（只修改 ts）
      TupleMeta new_meta = base_meta;
      new_meta.ts_ = commit_ts;

      // 检查函数确保在我们准备提交时该元组仍然由本事务占有
      auto check = [expect_txn_id](const TupleMeta &meta, const Tuple &tuple, RID rid,
                                   std::optional<UndoLink> undo_link) { return meta.ts_ == expect_txn_id; };

      // 使用原子 helper 来更新元组（并保留 undo link）
      bool updated = UpdateTupleAndUndoLink(this, rid, base_undo_link, table_heap, txn, new_meta, base_tuple, check);
      if (!updated) {
        // 若更新失败，按要求中止事务并返回 false
        Abort(txn);
        return false;
      }
    }
  }

  // 更新事务状态
  txn->state_ = TransactionState::COMMITTED;
  // 更新全局活跃事务的最新提交时间戳
  running_txns_.UpdateCommitTs(commit_ts);
  // 将该事务从活跃事务列表中移除
  running_txns_.RemoveTxn(txn->read_ts_);

  // last_commit_ts_.fetch_add(1);

  return true;
}*/
auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // 1. 预取 commit_ts，但不立即增加全局 last_commit_ts_
  timestamp_t commit_ts = last_commit_ts_.load() + 1;

  // 2. 物理更新：将所有修改过的 tuple 时间戳从 txn_id 改为真正的 commit_ts
  for (const auto &[table_oid, rids] : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(table_oid).get();
    for (const auto &rid : rids) {
      TupleMeta meta = table_info->table_->GetTupleMeta(rid);
      // 检查：确保当前还是该事务持有（防止意外覆盖）
      if (meta.ts_ == txn->GetTransactionId()) {
        meta.ts_ = commit_ts;
        table_info->table_->UpdateTupleMeta(meta, rid);
      }
    }
  }

  // 3. 状态变更：必须在更新全局时间戳之前修改状态
  txn->commit_ts_ = commit_ts;
  txn->state_ = TransactionState::COMMITTED;

  // 4. 关键：更新全局 commit 时间戳，新开始的事务现在能看到 commit_ts 了
  last_commit_ts_.store(commit_ts);

  // 5. 清理活跃事务列表
  {
    std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
    running_txns_.UpdateCommitTs(commit_ts);
    running_txns_.RemoveTxn(txn->read_ts_);
  }

  return true;
}

/*
We have already provided the starter code for TransactionManager::Abort,
and you do not need to change anything in Abort in order to get
full points for Task #1.
*/
/*void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    return;  // 或者抛出异常
  }

  // 1. 设置状态为 ABORTED
  txn->state_ = TransactionState::ABORTED;

  // 2. 回滚所有修改
  auto write_sets = txn->GetWriteSets();
  for (auto &entry : write_sets) {
    auto table_oid = entry.first;
    auto &rids = entry.second;
    auto table_info = catalog_->GetTable(table_oid).get();

    for (auto &rid : rids) {
      // 获取当前表中的元数据和 UndoLink
      auto meta = table_info->table_->GetTupleMeta(rid);
      auto undo_link = GetUndoLink(rid);

      // 关键判断：只有当这行数据当前确实是由本事务持有（ts_ == txn_id）时才需要回滚
      if (meta.ts_ == txn->GetTransactionId()) {
        if (undo_link.has_value() && undo_link->IsValid()) {
          // 情况 A: 存在旧版本日志，说明是 Update 或 Delete。
          // 需要取出最近的一条 UndoLog，将其内容写回 TableHeap，并恢复旧的 ts_。
          auto undo_log = GetUndoLog(*undo_link);

          // 使用已有的工具函数（如在 execution_common.h 中定义的）重建原始元组
          // 注意：这里需要你根据 UndoLog 的 modified_fields 恢复原始数据
          auto original_tuple =
              ReconstructTuple(&table_info->schema_, table_info->table_->GetTuple(rid).second, meta, {undo_log});

          // 更新回 TableHeap
          if (original_tuple.has_value()) {
            table_info->table_->UpdateTupleInPlace({undo_log.ts_, undo_log.is_deleted_}, original_tuple.value(), rid);
          }

          // 恢复 UndoLink 为该日志指向的上一条记录（即把当前事务产生的日志从链表中剔除）
          UpdateUndoLink(rid, undo_log.prev_version_);
        } else {
          // 情况 B: 不存在旧版本，说明是此事务新插入（Insert）的行。
          // 直接将 ts 置为 0 并标记为已删除，使其对其他事务不可见。
          table_info->table_->UpdateTupleMeta({0, true}, rid);
        }
      }
    }
  }

  // 3. 从活跃事务记录中移除
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    return;
  }

  // 1. 先将状态修改为 TAINTED。
  // GC 线程只会回收 COMMITTED 或 ABORTED 的事务，不会回收 TAINTED。
  // 这样可以防止在回滚过程中 txn 对象被 GC 线程意外销毁。
  txn->state_ = TransactionState::TAINTED;

  // 2. 回滚所有修改 (逻辑保持不变)
  auto write_sets = txn->GetWriteSets();
  for (auto &entry : write_sets) {
    auto table_oid = entry.first;
    auto &rids = entry.second;
    auto table_info = catalog_->GetTable(table_oid).get();

    for (auto &rid : rids) {
      auto meta = table_info->table_->GetTupleMeta(rid);

      if (meta.ts_ == txn->GetTransactionId()) {
        auto undo_link = GetUndoLink(rid);
        if (undo_link.has_value() && undo_link->IsValid()) {
          auto undo_log = GetUndoLog(*undo_link);
          auto original_tuple =
              ReconstructTuple(&table_info->schema_, table_info->table_->GetTuple(rid).second, meta, {undo_log});

          // 如果还原出了 Tuple，使用 UpdateTupleInPlace 恢复内容和 Meta
          if (original_tuple.has_value()) {
            table_info->table_->UpdateTupleInPlace({undo_log.ts_, undo_log.is_deleted_}, original_tuple.value(), rid);
          } else {
            // 如果还原结果为空（说明原版本是物理删除或不存在），我们至少要恢复 Meta 信息（ts 和 is_deleted）
            // 这里直接更新 Meta，内容保持原样即可（因为标记为 deleted 后内容不重要）
            table_info->table_->UpdateTupleMeta({undo_log.ts_, true}, rid);
          }

          UpdateUndoLink(rid, undo_log.prev_version_);
        } else {
          // 是新插入的数据，直接标记删除，ts 置 0
          table_info->table_->UpdateTupleMeta({0, true}, rid);
        }
      }
    }
  }

  // 3. 关键修复：加锁后更新状态为 ABORTED 并移除水位线
  // 必须在锁内完成状态切换和 RemoveTxn，防止 GC 在两者之间插入
  {
    std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
    txn->state_ = TransactionState::ABORTED;
    running_txns_.RemoveTxn(txn->read_ts_);
  }
}
  */
void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    return;
  }

  // 1. 先将状态标记为 TAINTED，防止 GC 干扰
  txn->state_ = TransactionState::TAINTED;

  // 这个事务改过哪些行
  auto write_sets = txn->GetWriteSets();
  for (auto &entry : write_sets) {
    auto table_oid = entry.first;
    auto &rids = entry.second;
    auto table_info = catalog_->GetTable(table_oid).get();

    for (auto &rid : rids) {
      // tuplemeta 包含了 ts_ 和 is_deleted_，我们需要根据 ts_ 判断当前版本是否由本事务持有
      auto meta = table_info->table_->GetTupleMeta(rid);

      // 只有当前事务拥有的行才需要回滚
      if (meta.ts_ != txn->GetTransactionId()) {
        continue;
      }

      auto undo_link_opt = GetUndoLink(rid);
      TupleMeta restore_meta;
      Tuple restore_tuple;

      // 左边的条件是std::optional的判断，判断有没有给这个rid生成过undo log(即看他是否有修改历史)
      // 右边的条件是判断这个undo log是否合法（即是否真的记录了一次修改）
      if (undo_link_opt.has_value() && undo_link_opt->IsValid()) {
        // --- 情况 A: 更新或删除的回滚 ---
        // 沿着版本链回溯，收集直到第一个已提交的日志（ts_ < TXN_START_ID），同时收集中间日志
        std::vector<UndoLog> collected_logs;
        std::optional<UndoLink> cur = undo_link_opt;
        bool found_committed = false;
        UndoLog committed_log;

        while (cur.has_value() && cur->IsValid()) {
          UndoLog log = GetUndoLog(*cur);
          collected_logs.push_back(log);
          if (log.ts_ < TXN_START_ID) {
            committed_log = log;
            found_committed = true;
            break;
          }
          cur = log.prev_version_;
        }

        if (found_committed) {
          // 使用收集到的日志恢复到该已提交版本
          auto original_tuple_opt =
              ReconstructTuple(&table_info->schema_, table_info->table_->GetTuple(rid).second, meta, collected_logs);
          if (original_tuple_opt.has_value()) {
            restore_meta = {committed_log.ts_, committed_log.is_deleted_};
            restore_tuple = original_tuple_opt.value();
            UpdateTupleAndUndoLink(this, rid, committed_log.prev_version_, table_info->table_.get(), txn, restore_meta,
                                   restore_tuple);
          } else {
            // 如果还原结果为空（说明原版本是物理删除或不存在），我们至少要恢复 Meta 信息（ts 和 is_deleted）
            // 并且必须把 UndoLink 恢复到修改之前的状态。
            table_info->table_->UpdateTupleMeta({committed_log.ts_, true}, rid);
            UpdateUndoLink(rid, committed_log.prev_version_);
          }
        } else {
          // 整个链中都没有已提交版本，说明该行在事务开始前并不存在（本事务插入），将其标记为删除
          table_info->table_->UpdateTupleMeta({0, true}, rid);
          UpdateUndoLink(rid, std::optional<UndoLink>());
        }
      } else {
        // --- 情况 B: 新插入元组的回滚 ---
        // 直接标记为已删除，并将时间戳归零（表示不再被任何事务拥有）
        table_info->table_->UpdateTupleMeta({0, true}, rid);
        UpdateUndoLink(rid, std::optional<UndoLink>());
      }
    }
  }

  // 3. 状态转换与清理：在锁内设置为 ABORTED 并从 running list 中移除（使用 txn id 作为键）
  {
    std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
    txn->state_ = TransactionState::ABORTED;
    running_txns_.RemoveTxn(txn->read_ts_);
  }
}

void TransactionManager::GarbageCollection() {
  std::unordered_set<txn_id_t> alive_txns;
  for (const auto &table_name : catalog_->GetTableNames()) {
    auto table_info = catalog_->GetTable(table_name).get();
    auto table_heap = table_info->table_.get();
    // 获取表的迭代器
    auto iter = table_heap->MakeIterator();
    // 遍历表中的每一行
    while (!iter.IsEnd()) {
      RID rid = iter.GetRID();
      auto [meta, tuple] = iter.GetTuple();
      // 获取该行的undo link
      std::optional<UndoLink> link = GetUndoLink(rid);
      std::optional<UndoLink> prev_link;
      if (link.has_value()) {
        prev_link = link;
      } else {
        ++iter;
        continue;
        ;
      }
      if (meta.ts_ <= running_txns_.GetWatermark()) {
        // 当前版本足够旧，最老的活跃事务也能看见它，不需要回退到更早版本
        ++iter;
        continue;
      }

      // 遍历版本链
      while (prev_link.has_value() && prev_link->IsValid()) {
        auto undo_log_opt = GetUndoLogOptional(*prev_link);
        if (!undo_log_opt.has_value()) {
          break;
        }
        const auto &undo_log = *undo_log_opt;
        alive_txns.insert(prev_link->prev_txn_);
        // 检查该日志的时间戳
        // 如果该日志的时间戳大于watermark，说明该日志对应的事务还活着
        if (undo_log.ts_ <= running_txns_.GetWatermark()) {
          // 记录该活跃事务id
          break;
        }
        // 继续往前找
        prev_link = undo_log.prev_version_;
      }
      ++iter;
    }
  }
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  // 遍历事务映射表，找出不活跃的事务
  for (auto it = txn_map_.begin(); it != txn_map_.end();) {
    auto txn_id = it->first;
    auto &txn = it->second;
    // 如果这个事务已经commit或abort，并且不在活跃事务列表中
    if ((txn->state_ == TransactionState::COMMITTED || txn->state_ == TransactionState::ABORTED) &&
        alive_txns.find(txn_id) == alive_txns.end()) {
      //该事务不活跃，可以删除
      it = txn_map_.erase(it);
    } else {
      ++it;
    }
  }
}

}  // namespace bustub
