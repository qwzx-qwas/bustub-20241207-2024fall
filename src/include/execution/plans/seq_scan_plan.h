//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_plan.h
//
// Identification: src/include/execution/plans/seq_scan_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <utility>

#include "binder/table_ref/bound_base_table_ref.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * The SeqScanPlanNode represents a sequential table scan operation.
 * SeqScanPlanNode 表示顺序表扫描操作。
 */
class SeqScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new SeqScanPlanNode instance.
   * 构造一个新的 SeqScanPlanNode 实例。
   * @param output The output schema of this sequential scan plan node
   * @param table_oid The identifier of table to be scanned
   */
  SeqScanPlanNode(SchemaRef output, table_oid_t table_oid, std::string table_name,
                  AbstractExpressionRef filter_predicate = nullptr)
      : AbstractPlanNode(std::move(output), {}),
        table_oid_{table_oid},
        table_name_(std::move(table_name)),
        filter_predicate_(std::move(filter_predicate)) {}

  /** @return The type of the plan node */
  /** @return 计划节点的类型 */
  auto GetType() const -> PlanType override { return PlanType::SeqScan; }

  /** @return The identifier of the table that should be scanned */
  /** @return 应扫描的表的标识符 */
  auto GetTableOid() const -> table_oid_t { return table_oid_; }

  static auto InferScanSchema(const BoundBaseTableRef &table_ref) -> Schema;

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(SeqScanPlanNode);

  /** The table whose tuples should be scanned */
  /** 应扫描其元组的表 */
  table_oid_t table_oid_;

  /** The table name */
  /** 表名 */
  std::string table_name_;

  /** The predicate to filter in seqscan.
   * 顺序扫描中的过滤谓词。
   * For Fall 2023, We'll enable the MergeFilterScan rule, so we can further support index point lookup
   * 对于 2023 年秋季学期，我们将启用 MergeFilterScan 规则，以便我们可以进一步支持索引点查找
   */
  AbstractExpressionRef filter_predicate_;

 protected:
  auto PlanNodeToString() const -> std::string override {
    if (filter_predicate_) {
      return fmt::format("SeqScan {{ table={}, filter={} }}", table_name_, filter_predicate_);
    }
    return fmt::format("SeqScan {{ table={} }}", table_name_);
  }
};

}  // namespace bustub
