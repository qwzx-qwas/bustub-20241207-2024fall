//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 * SeqScanExecutor 执行器执行顺序表扫描。
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * 构造一个新的 SeqScanExecutor 实例。
   * @param exec_ctx The executor context 执行上下文
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  /** 初始化顺序扫描 */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * 从顺序扫描中生成下一个元组。
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */
  /** @return 顺序扫描的输出模式 */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed */
  /** 要执行的顺序扫描计划节点 */
  //SeqScanPlanNode 只是整个大查询计划树（Plan Tree）中的一个“零件”或“节点”
  const SeqScanPlanNode *plan_;
  //迭代器成员，用来指向当前扫描到的位置
  std::optional<TableIterator> iter_;
  //表的信息
  const TableInfo *table_info;
};
}  // namespace bustub
