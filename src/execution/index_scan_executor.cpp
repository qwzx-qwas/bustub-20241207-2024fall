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
    : AbstractExecutor(exec_ctx) {}

void IndexScanExecutor::Init() {
    auto *catalog = exec_ctx_->GetCatalog();
    index_info_ = catalog->GetIndex(plan_->GetIndexOid());
    /*转换成B+tree对象
    因为IndexInfo中存储的是基类的Index类型指针 
    而如果要实现顺序扫描(order scan)，需要用到B+tree的迭代器，
    所以需要将Index指针转换成BPlusTreeIndexForTwoIntegerColumn指针
    这里索引是建立在两个整数列上的B+树上
    */
    tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
    //检查优化器处理后的pred_keys是否为空
    if(!plan_->pred_keys_.empty()) {
        //点查询模式（Point Lookup）
        std::vector<Value> values;
        for(const auto &expr : plan_->pred_keys_) {
            values.push_back(expr->Evaluate(nullptr, {}));
        }
        //构造查找用的Key Tuple
        Tuple key_tuple(values, index_info_->index_->GetKeySchema());
        //定位迭代器到目标Key
        iter_ = tree_->GetBeginIterator(key_tuple.GetKeyAttrs(), key_tuple);
    } else {
        //全扫描模式
        iter_ = tree_->GetBeginIterator();
    }
    end_ = tree_->GetEndIterator();


}


auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    while (iter_ != end_) {
        //获取当前索引项的RID
        *rid = (*iter_).second;
        //根据RID从表中获取对应的tuple
        auto table_info = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
        auto [meta, real_tuple] = table_info->table_->GetTuple(*rid);

        //检查谓词，如果是点查询，若当前Key不满足谓词条件，则停止扫描
        if (!plan_->pred_keys_.empty()) {
             if (plan_->filter_predicate_ != nullptr) {
                 auto value = plan_->filter_predicate_->Evaluate(&real_tuple, plan_->OutputSchema());
                 if (!value.GetAs<bool>()) {
                    //因为在B+树中，Key是有序的，一旦遇到不满足谓词条件的Key，就可以停止扫描
                     return false;
                 }
             }
        }
        
        ++iter_;  // 移动到下一个索引项

        if(!meta.is_deleted_) {
            *tuple = real_tuple;
            return true;
        }

    }
    return false;
}

}  // namespace bustub
