//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto *catalog = exec_ctx_->GetCatalog();
  index_info_ = catalog->GetIndex(plan_->GetIndexOid()).get();
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());

  current_key_idx_ = 0;

  //查看上面优化器传来的pred_keys_（之前收集的常量值），决定是点查找模式还是全表扫描模式
  if (!plan_->pred_keys_.empty()) {
    // 点查找模式
    // 将迭代器初始化到第一个目标键
    std::vector<Value> values;
    //取出第一个key的值
    values.push_back(plan_->pred_keys_[current_key_idx_]->Evaluate(nullptr, plan_->OutputSchema()));
    //构造索引key的tuple
    Tuple key_tuple(values, index_info_->index_->GetKeySchema());
    //将该tuple转换为B+树索引的key类型
    IntegerKeyType_BTree index_key;
    index_key.SetFromKey(key_tuple);
    iter_.emplace(tree_->GetBeginIterator(index_key));
  } else {
    // 全表扫描模式
    iter_.emplace(tree_->GetBeginIterator());
  }
  end_.emplace(tree_->GetEndIterator());
}
/*auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!iter_.has_value()) {
    return false;
  }
  while (true) {
    //检查是否到达当前扫描的末尾
    if (*iter_ == *end_) {
      // 若处于点查找模式且还有更多 key 需要检查
      // 判断是否还有未处理的 key（OR 子句中的下一个 key）
      if (!plan_->pred_keys_.empty() && current_key_idx_ + 1 < plan_->pred_keys_.size()) {
        current_key_idx_++;
        std::vector<Value> values;
        values.push_back(plan_->pred_keys_[current_key_idx_]->Evaluate(nullptr, plan_->OutputSchema()));
        Tuple key_tuple(values, index_info_->index_->GetKeySchema());
        //定位迭代器到下一个目标Key
        IntegerKeyType_BTree index_key;
        index_key.SetFromKey(key_tuple);
        iter_.emplace(tree_->GetBeginIterator(index_key));
        continue;
      }
      iter_ = std::nullopt;
      return false;
    }

    *rid = (**iter_).second;
    auto table_info = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
    auto [meta, real_tuple] = table_info->table_->GetTuple(*rid);

    // 检查点查找的谓词
    if (!plan_->pred_keys_.empty()) {
      // 期望的键值
      auto expected_val = plan_->pred_keys_[current_key_idx_]->Evaluate(nullptr, plan_->OutputSchema());

      // 按索引键的 schema 从元组中获取实际键值
      // 注意：为简化实现，假设索引为单列索引（满足项目要求）
      // auto key_schema = index_info_->index_->GetKeySchema();
      auto key_attrs = index_info_->index_->GetKeyAttrs();
      //实际的值是多少
      auto actual_val = real_tuple.GetValue(&table_info->schema_, key_attrs[0]);

      // 如果当前元组的索引键不等于目标值，说明索引迭代器已经越过目标键
      // 防止 ++iter_ 后读到的 tuple 不是目标键对应的元组
      if (actual_val.CompareEquals(expected_val) != CmpBool::CmpTrue) {
        // 如果还有下一个目标 key，则切换到下一个 key 的起始迭代器
        if (current_key_idx_ + 1 < plan_->pred_keys_.size()) {
          current_key_idx_++;
          std::vector<Value> values;
          values.push_back(plan_->pred_keys_[current_key_idx_]->Evaluate(nullptr, plan_->OutputSchema()));
          Tuple key_tuple(values, index_info_->index_->GetKeySchema());
          IntegerKeyType_BTree index_key;
          index_key.SetFromKey(key_tuple);
          iter_.emplace(tree_->GetBeginIterator(index_key));
          continue;
        }
        iter_ = std::nullopt;
        return false;
      }
    }

    ++(*iter_);

    if (!meta.is_deleted_) {
      *tuple = real_tuple;
      return true;
    }
  }
}*/
// MVCC版本
auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!iter_.has_value()) {
    return false;
  }

  auto txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  while (true) {
    //检查是否到达当前扫描的末尾
    if (*iter_ == *end_) {
      // 若处于点查找模式且还有更多 key 需要检查
      // 判断是否还有未处理的 key（OR 子句中的下一个 key）
      if (!plan_->pred_keys_.empty() && current_key_idx_ + 1 < plan_->pred_keys_.size()) {
        current_key_idx_++;
        std::vector<Value> values;
        values.push_back(plan_->pred_keys_[current_key_idx_]->Evaluate(nullptr, plan_->OutputSchema()));
        Tuple key_tuple(values, index_info_->index_->GetKeySchema());
        //定位迭代器到下一个目标Key
        IntegerKeyType_BTree index_key;
        index_key.SetFromKey(key_tuple);
        iter_.emplace(tree_->GetBeginIterator(index_key));
        continue;
      }
      iter_ = std::nullopt;
      return false;
    }

    *rid = (**iter_).second;

    auto [base_meta, base_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_info->table_.get(), *rid);

    // 通过 undo log 重建该 rid 在当前事务视角下的可见版本
    auto undo_logs_opt = CollectUndoLogs(*rid, base_meta, base_tuple, undo_link, txn, txn_mgr);
    if (!undo_logs_opt.has_value()) {
      ++(*iter_);
      continue;
    }

    std::optional<Tuple> visible_tuple_opt;
    if (undo_logs_opt->empty()) {
      if (base_meta.is_deleted_) {
        ++(*iter_);
        continue;
      }
      visible_tuple_opt = base_tuple;
    } else {
      visible_tuple_opt = ReconstructTuple(&table_info->schema_, base_tuple, base_meta, *undo_logs_opt);
    }

    if (!visible_tuple_opt.has_value()) {
      ++(*iter_);
      continue;
    }
    const auto &visible_tuple = *visible_tuple_opt;

    // 检查点查找的谓词
    if (!plan_->pred_keys_.empty()) {
      // 期望的键值
      auto expected_val = plan_->pred_keys_[current_key_idx_]->Evaluate(nullptr, plan_->OutputSchema());

      // 按索引键的 schema 从元组中获取实际键值
      // 注意：为简化实现，假设索引为单列索引（满足项目要求）
      // auto key_schema = index_info_->index_->GetKeySchema();
      auto key_attrs = index_info_->index_->GetKeyAttrs();
      //实际的值是多少
      auto actual_val = visible_tuple.GetValue(&table_info->schema_, key_attrs[0]);

      // 如果当前元组的索引键不等于目标值，说明索引迭代器已经越过目标键
      // 防止 ++iter_ 后读到的 tuple 不是目标键对应的元组
      if (actual_val.CompareEquals(expected_val) != CmpBool::CmpTrue) {
        // 如果还有下一个目标 key，则切换到下一个 key 的起始迭代器
        if (current_key_idx_ + 1 < plan_->pred_keys_.size()) {
          current_key_idx_++;
          std::vector<Value> values;
          values.push_back(plan_->pred_keys_[current_key_idx_]->Evaluate(nullptr, plan_->OutputSchema()));
          Tuple key_tuple(values, index_info_->index_->GetKeySchema());
          IntegerKeyType_BTree index_key;
          index_key.SetFromKey(key_tuple);
          iter_.emplace(tree_->GetBeginIterator(index_key));
          continue;
        }
        iter_ = std::nullopt;
        return false;
      }
    }

    ++(*iter_);

    *tuple = visible_tuple;
    return true;
  }
}

}  // namespace bustub
