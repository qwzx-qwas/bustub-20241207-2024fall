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
    // Point Lookup Mode
    // Initialize iterator to the first key
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
    // Full Scan Mode
    iter_.emplace(tree_->GetBeginIterator());
  }
  end_.emplace(tree_->GetEndIterator());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!iter_.has_value()) {
    return false;
  }
  while (true) {
    //检查是否到达当前扫描的末尾
    if (*iter_ == *end_) {
      // If we are in point lookup mode and have more keys to check
      //判断是否还有未处理的key（OR子句中的下一个key）
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

     // Check predicate for point lookup
    if (!plan_->pred_keys_.empty()) {
      // 当前的值是多少
      auto expected_val = plan_->pred_keys_[current_key_idx_]->Evaluate(nullptr, plan_->OutputSchema());
      
      // Get the actual value from the tuple using the index key schema
      // Note: This assumes single column index for simplicity as per project requirements
      //auto key_schema = index_info_->index_->GetKeySchema();
      auto key_attrs = index_info_->index_->GetKeyAttrs();
      //实际的值是多少
      auto actual_val = real_tuple.GetValue(&table_info->schema_, key_attrs[0]);

      // If the current tuple's key doesn't match what we are looking for, 
      // it means we've moved past the target key in the index.
      //防止++iter_后，读到的tuple不是我们想要的key对应的tuple
      if (actual_val.CompareEquals(expected_val) != CmpBool::CmpTrue) {
         // Move to next key if available，跟上面一样的逻辑
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
}

}  // namespace bustub
