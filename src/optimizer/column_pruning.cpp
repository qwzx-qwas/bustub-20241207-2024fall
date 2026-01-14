#include "execution/expressions/column_value_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

namespace {

// 辅助函数：递归提取表达式中所有被引用的列索引（ColIdx）
//
// 该函数像一个“探针”，递归遍历复杂的表达式树，找出该表达式依赖了数据表的哪些列。
//
// 举例：对于 SQL "SELECT ... WHERE age > 20"
// 表达式树：(Comparison: >)
//            /           \
//      (Column: age)   (Constant: 20)
//

void ExtractUsedColumns(const AbstractExpressionRef &expr, std::unordered_set<uint32_t> &used_cols) {
  // 如果当前节点是 ColumnValueExpression（比如 'age'）：
  // - 它直接通过 col_idx 指向物理表的某一列（比如第 2 列）。
  // - 我们将这个 col_idx (2) 存入 used_cols 集合。
  //  注意：仅处理 TupleIdx == 0 的情况（适用于 Filter, Projection 等单子节点算子）。
  //  对于 Join 算子，左表通常对应 Tuple 0，右表对应 Tuple 1。
  //  当前实现只会提取左表的列引用，忽略右表引用。要支持 Join 裁剪，需扩展逻辑以处理 Tuple 1。

  if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(expr.get()); col_expr != nullptr) {
    if (col_expr->GetTupleIdx() == 0) {
      used_cols.insert(col_expr->GetColIdx());
    }
  } else {
    // 如果当前节点不是列表达式（比如 '>' 符号或常量）：
    // 递归检查它的所有子节点（继续往下挖）。
    for (const auto &child : expr->GetChildren()) {
      ExtractUsedColumns(child, used_cols);
    }
  }
}

// 辅助函数：重写表达式中的列索引（ColIdx）
//
// 当我们对底层算子（如 SeqScan）进行列裁剪后，其输出 Schema 会发生变化。
// 例如：原表有 [A, B, C, D] 四列，上层只用到了 D。
// 裁剪后，SeqScan 只输出 [D]。此时 D 的索引从原来的 3 变成了 0。
//
// 因此，上层算子中引用 D 的表达式必须更新，将其中的 ColIdx 从 3 改为 0。
// mapping 参数提供了这种 {旧索引 -> 新索引} 的映射关系。

//其实也就是重新写一个schema来表示列裁剪后的schema
auto RewriteExpression(const AbstractExpressionRef &expr, const std::unordered_map<uint32_t, uint32_t> &mapping)
    -> AbstractExpressionRef {
  if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(expr.get()); col_expr != nullptr) {
    if (col_expr->GetTupleIdx() == 0) {
      if (mapping.count(col_expr->GetColIdx()) > 0) {
        return std::make_shared<ColumnValueExpression>(0, mapping.at(col_expr->GetColIdx()), col_expr->GetReturnType());
      }
      return expr;
    }
  }
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.push_back(RewriteExpression(child, mapping));
  }
  return expr->CloneWithChildren(children);
}

}  // namespace

/**
 * @note You may use this function to implement column pruning optimization.
 */
auto Optimizer::OptimizeColumnPruning(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // 目前该函数为空实现
  // 完整的列裁剪优化（Column Pruning）通常采用自顶向下的方式：
  // 1. 父节点通知子节点它需要的列集合。
  // 2. 子节点（如 SeqScan）根据需求只读取必要的列，并修改其输出 Schema。
  // 3. 子节点由于输出列变少，列的索引（col_idx）会发生变化，需要生成一个映射关系。
  // 4. 父节点根据子节点返回的映射关系，重写自己的表达式。
  //
  // 仅处理 Projection -> SeqScan 和 Projection -> Filter -> SeqScan 模式

  if (plan->GetType() == PlanType::Projection) {
    const auto &proj_plan = dynamic_cast<const ProjectionPlanNode &>(*plan);
    auto child = proj_plan.GetChildAt(0);

    // 1. 递归优化子节点（自底向上，标准 Optimizer 流程）
    // 但列裁剪比较特殊，它既可以 Top-Down 也可以 Bottom-Up 重写。
    // 为了简单，我们这里在 Top 层直接分析，并假设我们要处理的是紧随其后的 SeqScan。

    //收集 Projection 需要的所有列
    std::unordered_set<uint32_t> used_cols;
    for (const auto &expr : proj_plan.GetExpressions()) {
      ExtractUsedColumns(expr, used_cols);
    }

    // 处理 Projection -> Filter -> SeqScan 的情况
    // 如果中间夹着 Filter，我们也得收集 Filter 用到的列
    const FilterPlanNode *filter_plan = nullptr;
    if (child->GetType() == PlanType::Filter) {
      filter_plan = dynamic_cast<const FilterPlanNode *>(child.get());
      ExtractUsedColumns(filter_plan->GetPredicate(), used_cols);
      child = filter_plan->GetChildAt(0);
    }

    //  找到底层的 SeqScan
    if (child->GetType() == PlanType::SeqScan) {
      const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(*child);
      const auto &table_info = catalog_.GetTable(seq_scan.GetTableOid());
      const auto &table_schema = table_info->schema_;

      // 如果需要所有列，则无需裁剪
      if (used_cols.size() == table_schema.GetColumnCount()) {
        return plan;
      }
      if (used_cols.empty()) {
        // Edge case: used_cols 为空（如 select count(*) 或 select 1），通常不直接裁剪为空 Scan，
        // 而是至少保留一列，或者走专门的 Count(*) 优化。暂且直接返回。
        return plan;
      }

      //构建剪枝后的 Schema 和列映射 (OldIdx -> NewIdx)
      std::vector<uint32_t> sorted_used_cols(used_cols.begin(), used_cols.end());
      std::sort(sorted_used_cols.begin(), sorted_used_cols.end());

      std::vector<Column> new_columns;
      std::unordered_map<uint32_t, uint32_t> column_mapping;  // Old -> New;

      for (uint32_t i = 0; i < sorted_used_cols.size(); i++) {
        uint32_t old_idx = sorted_used_cols[i];
        new_columns.push_back(table_schema.GetColumn(old_idx));
        column_mapping[old_idx] = i;
      }

      // 创建新的 SeqScan
      Schema new_schema(new_columns);
      auto new_seq_scan = std::make_shared<SeqScanPlanNode>(std::make_shared<Schema>(new_schema), seq_scan.table_oid_,
                                                            seq_scan.table_name_);

      //  重写上层算子（Filter 和 Projection）的表达式
      AbstractPlanNodeRef new_child = new_seq_scan;

      // 重写 Filter
      if (filter_plan != nullptr) {
        auto new_predicate = RewriteExpression(filter_plan->GetPredicate(), column_mapping);
        new_child = std::make_shared<FilterPlanNode>(std::make_shared<Schema>(new_schema), new_predicate, new_seq_scan);
      }

      // 重写 Projection
      std::vector<AbstractExpressionRef> new_expressions;
      for (const auto &expr : proj_plan.GetExpressions()) {
        new_expressions.push_back(RewriteExpression(expr, column_mapping));
      }

      return std::make_shared<ProjectionPlanNode>(proj_plan.output_schema_, new_expressions, new_child);
    }
  }

  // 默认递归处理其他情况
  //即如果不是Projection->（Filter）->SeqScan模式，就直接递归优化子节点
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeColumnPruning(child));
  }
  return plan->CloneWithChildren(std::move(children));
}

}  // namespace bustub
