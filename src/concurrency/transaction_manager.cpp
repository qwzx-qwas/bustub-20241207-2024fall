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
  //因为last_commit_ts_实际上记录的是已经发生过的、最新一次提交的时间戳。
  //就是每进行一次提交，吐出的下一个时间戳。
  txn_ref->read_ts_ = last_commit_ts_.load();
  txn_ref->commit_ts_ = 0;  

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  //确保提交过程是线程安全的
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  //获取提交时间戳，并更新下一个可用的提交时间戳
  txn->commit_ts_ = last_commit_ts_.load() + 1;
  //last_commit_ts_ = txn->commit_ts_.load();

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
  //更新基础元组
  //遍历写集合
  for (const auto &write_entry : txn->GetWriteSets()) {
    auto table_oid = write_entry.first;
    auto rid_set = write_entry.second;
    //获取表信息
    auto table_info = catalog_->GetTable(table_oid).get();
    auto table_heap = table_info->table_.get();

    for (const auto &rid : rid_set) {
      //获取当前元组的元数据
      auto [meta, tuple] = table_heap->GetTuple(rid);
      //更新元数据的时间戳为提交时间戳
      meta.ts_ = txn->commit_ts_;
      //写回元数据
      table_heap->UpdateTupleMeta(meta, rid);
    }
  }

  //更新事务状态
  txn->state_ = TransactionState::COMMITTED;
  //更新全局活跃事务的最新提交时间戳
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  //将该事务从活跃事务列表中移除
  running_txns_.RemoveTxn(txn->read_ts_);

  last_commit_ts_.fetch_add(1);

  return true;
}

/*
We have already provided the starter code for TransactionManager::Abort,
and you do not need to change anything in Abort in order to get 
full points for Task #1.
*/
void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }


  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
      std::unordered_set<txn_id_t> alive_txns;
  for(const auto &table_name : catalog_->GetTableNames()) {
    auto table_info = catalog_->GetTable(table_name).get();
    auto table_heap = table_info->table_.get();
    //获取表的迭代器
    auto iter = table_heap->MakeIterator();
    //遍历表中的每一行
    while(!iter.IsEnd()) {
      RID rid = iter.GetRID();
      auto [meta, tuple] = iter.GetTuple();
      //获取该行的undo link
      std::optional<UndoLink> link = GetUndoLink(rid);
      std::optional<UndoLink> prev_link;
      if(link.has_value()) {
        prev_link = link;
      } else {
        ++iter;
        continue;;
      }
      if(meta.ts_ <= running_txns_.GetWatermark()) {
        // 当前版本足够旧，最老的活跃事务也能看见它，不需要回退到更早版本
        ++iter;
        continue;
      }
      
      //遍历版本链
      while(prev_link.has_value() && prev_link->IsValid()) {
        auto undo_log_opt = GetUndoLogOptional(*prev_link);
        if(!undo_log_opt.has_value()) {
          break;
        }
        const auto &undo_log = *undo_log_opt;
          alive_txns.insert(prev_link->prev_txn_);        
        //检查该日志的时间戳
        //如果该日志的时间戳大于watermark，说明该日志对应的事务还活着
        if(undo_log.ts_ <= running_txns_.GetWatermark()) {
          //记录该活跃事务id
          break;
        } 
          //继续往前找
          prev_link = undo_log.prev_version_;
      }
      ++iter;
    }


  }
      std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
      //遍历事务映射表，找出不活跃的事务
    for(auto it = txn_map_.begin(); it != txn_map_.end(); ) {
      auto txn_id = it->first;
      auto &txn = it->second;
      //如果这个事务已经commit或abort，并且不在活跃事务列表中
      if((txn->state_ == TransactionState::COMMITTED || txn->state_ == TransactionState::ABORTED) &&
         alive_txns.find(txn_id) == alive_txns.end()) {
        //该事务不活跃，可以删除
        it = txn_map_.erase(it);
      } else {
        ++it;
      }
    }
}

}  // namespace bustub
