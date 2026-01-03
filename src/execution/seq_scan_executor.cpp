//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {}

void SeqScanExecutor::Init() {
    //Init的职责是初始化扫描器，设置迭代器到表的起始位置
    //先通过ExecutorContext获取Catalog
    auto catalog = exec_ctx_->GetCatalog();
    //拿到目标id(从plan中获取）的表信息
    auto table_info = catalog->GetTable(plan_->GetTableOid());
    //初始化迭代器到表的起始位置
    iter_ = table_info->table_->MakeIterator();
}

//Next函数会被上层反复调用，直到返回false，表示没有更多元组
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    //循环，直到找到一个未被删除的元组，或者迭代器到达表尾
    while(!iter_.IsEnd()){
        auto [meta, current_tuple] = iter_.GetTuple();
        //检查元组是否被删除
        if(!meta.IsDeleted()){
            //如果没有被删除，输出这个元组和它的RID
            *tuple = current_tuple;
            *rid = current_tuple.GetRid();
            //将迭代器前进到下一个位置
            ++iter_;
            return true;
        }
        ++iter_; //跳过被删除的元组
    }
    return false; //没有更多元组
}

}  // namespace bustub
