#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {
// 不同于被注释的版本，这个版本需要额外处理其他两种情况
// 处理等值条件
// 处理本地谓词，即只涉及左表或右表的谓词，将谓词下推
// 复杂连接（涉及两个表但不是等值条件的连接）不处理，直接返回原计划节点
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // 必须先递归优化子节点，因为优化是从底部往上进行的
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // 如果 new_plan 的类型不是 NestedLoopJoin就直接返回原plan
  if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
    return optimized_plan;
  }

  const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

  // 这个plan有没有谓词,即有没有连接条件
  if (nlj_plan.Predicate() == nullptr) {
    return optimized_plan;
  }

  // 辅助函数：检查表达式涉及的 tuple_idx
  // 返回值:
  // 0: 只涉及左表
  // 1: 只涉及右表
  // -1: 混合引用（涉及左右表）或无效
  // 2: 不涉及任何 tuple (常量)
  std::function<int(const AbstractExpressionRef &)> get_expr_tuple_idx;
  get_expr_tuple_idx = [&](const AbstractExpressionRef &expr) -> int {
    if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(expr.get()); col_expr != nullptr) {
      return col_expr->GetTupleIdx();
    }
    int status = 2;  // 初始状态：未发现列引用
    for (const auto &child : expr->GetChildren()) {
      int child_status = get_expr_tuple_idx(child);
      if (child_status == -1) {
        return -1;
      }
      if (status == 2) {
        status = child_status;
      } else if (child_status != 2 && child_status != status) {
        return -1;
      }
    }
    return status;
  };

  // 辅助函数：将表达式中引用右表(tuple_idx=1)的部分重写为 tuple_idx=0
  // 用于将右表的过滤条件推到 HashJoin 下方时，子节点输出的 tuple_idx 变为 0
  std::function<AbstractExpressionRef(const AbstractExpressionRef &)> rewrite_expr;
  rewrite_expr = [&](const AbstractExpressionRef &expr) -> AbstractExpressionRef {
    if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(expr.get()); col_expr != nullptr) {
      if (col_expr->GetTupleIdx() == 1) {
        return std::make_shared<ColumnValueExpression>(0, col_expr->GetColIdx(), col_expr->GetReturnType());
      }
    }
    std::vector<AbstractExpressionRef> new_children;
    for (const auto &child : expr->GetChildren()) {
      new_children.push_back(rewrite_expr(child));
    }
    return expr->CloneWithChildren(new_children);
  };

  // 提取所有逻辑与（AND）连接的谓词
  std::vector<AbstractExpressionRef> predicates;
  std::function<void(const AbstractExpressionRef &)> collect_predicates;
  collect_predicates = [&](const AbstractExpressionRef &expr) {
    if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get()); logic_expr != nullptr) {
      if (logic_expr->logic_type_ == LogicType::And) {
        collect_predicates(logic_expr->GetChildAt(0));
        collect_predicates(logic_expr->GetChildAt(1));
        return;
      }
    }
    predicates.push_back(expr);
  };
  collect_predicates(nlj_plan.Predicate());

  // 准备容器：分别存储 左/右 连接键，和 左/右 过滤条件
  std::vector<AbstractExpressionRef> left_key_expressions;
  std::vector<AbstractExpressionRef> right_key_expressions;
  std::vector<AbstractExpressionRef> left_filters;
  std::vector<AbstractExpressionRef> right_filters;

  for (const auto &expr : predicates) {
    // 检查是否是等值连接条件
    if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get()); cmp_expr != nullptr) {
      if (cmp_expr->comp_type_ == ComparisonType::Equal) {
        auto left_status = get_expr_tuple_idx(cmp_expr->GetChildAt(0));
        auto right_status = get_expr_tuple_idx(cmp_expr->GetChildAt(1));

        // Case 1: Left=Right
        if (left_status == 0 && right_status == 1) {
          left_key_expressions.push_back(cmp_expr->GetChildAt(0));
          right_key_expressions.push_back(rewrite_expr(cmp_expr->GetChildAt(1)));
          continue;
        }
        // Case 2: Right=Left
        if (left_status == 1 && right_status == 0) {
          left_key_expressions.push_back(cmp_expr->GetChildAt(1));
          right_key_expressions.push_back(rewrite_expr(cmp_expr->GetChildAt(0)));
          continue;
        }
      }
    }

    // 如果不是连接键，检查是否是本地过滤条件
    int status = get_expr_tuple_idx(expr);
    if (status == 0 || status == 2) {
      // 只涉及左表(0) 或 常量表达式(2) -> 下推到左子节点
      left_filters.push_back(expr);
    } else if (status == 1) {
      // 只涉及右表 -> 下推到右子节点 (需重写索引)
      right_filters.push_back(rewrite_expr(expr));
    } else {
      // 复杂连接（涉及两个表但不是等值条件的连接），无法处理，直接返回原计划
      return optimized_plan;
    }
  }

  // 如果没有提取出任何连接键，无法转为 HashJoin
  if (left_key_expressions.empty()) {
    return optimized_plan;
  }

  // 构建带有下推过滤条件的新子节点
  auto left_child = nlj_plan.GetLeftPlan();
  auto right_child = nlj_plan.GetRightPlan();

  // 如果有左表过滤条件，包裹一层 FilterPlanNode
  if (!left_filters.empty()) {
    auto final_left_filter = left_filters[0];
    for (size_t i = 1; i < left_filters.size(); ++i) {
      final_left_filter = std::make_shared<LogicExpression>(final_left_filter, left_filters[i], LogicType::And);
    }
    left_child =
        std::make_shared<FilterPlanNode>(nlj_plan.GetLeftPlan()->output_schema_, final_left_filter, left_child);
  }

  // 如果有右表过滤条件，包裹一层 FilterPlanNode
  if (!right_filters.empty()) {
    auto final_right_filter = right_filters[0];
    for (size_t i = 1; i < right_filters.size(); ++i) {
      final_right_filter = std::make_shared<LogicExpression>(final_right_filter, right_filters[i], LogicType::And);
    }
    right_child =
        std::make_shared<FilterPlanNode>(nlj_plan.GetRightPlan()->output_schema_, final_right_filter, right_child);
  }

  return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, left_child, right_child, left_key_expressions,
                                            right_key_expressions, nlj_plan.GetJoinType());
}

/*
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  // 将 NestedLoopJoinPlan 转换为 HashJoinPlan
  // 当连接谓词（Join Predicate）是两个列之间若干个等值条件的逻辑与（AND）组合时，就可以使用哈希连接算法。
  // 这里不需要决定左右表（不考虑大小，这个项目正确性优先于性能），只需要提取出所有的等值条件即可。

  // 必须先递归优化子节点，因为优化是从底部往上进行的
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // 如果 new_plan 的类型不是 NestedLoopJoin，直接返回 new_plan
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

    // 这个plan有没有谓词,即有没有连接条件
    if (nlj_plan.Predicate() == nullptr) {
      return optimized_plan;
    }

    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;

    // 递归遍历谓词树，提取出所有的等值对
    std::function<bool(const AbstractExpressionRef &)> check_and_extract =
        [&](const AbstractExpressionRef &expr) -> bool {
      // 如果是 LogicExpression 且类型是 AND
      if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get()); logic_expr != nullptr) {
        if (logic_expr->logic_type_ == LogicType::And) {
          return check_and_extract(logic_expr->GetChildAt(0)) && check_and_extract(logic_expr->GetChildAt(1));
        }
        // OR 不支持
        // 其他情况：不是 AND 或 =，就直接返回false
        return false;
      }

      // 如果是 ComparisonExpression 且类型是 Equal
      if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get()); cmp_expr != nullptr) {
        if (cmp_expr->comp_type_ == ComparisonType::Equal) {
          const auto &left_child = cmp_expr->GetChildAt(0);
          const auto &right_child = cmp_expr->GetChildAt(1);

          const auto *left_col = dynamic_cast<const ColumnValueExpression *>(left_child.get());
          const auto *right_col = dynamic_cast<const ColumnValueExpression *>(right_child.get());

          if (left_col != nullptr && right_col != nullptr) {
            // 利用 GetTupleIdx() 判断哪边是左表的列（Idx=0），哪边是右表的列（Idx=1）
            if (left_col->GetTupleIdx() == 0 && right_col->GetTupleIdx() == 1) {
              left_key_expressions.push_back(left_child);
              right_key_expressions.push_back(right_child);
              return true;
            }
            if (left_col->GetTupleIdx() == 1 && right_col->GetTupleIdx() == 0) {
              left_key_expressions.push_back(right_child);
              right_key_expressions.push_back(left_child);
              return true;
            }
          }
        }
      }

      // 其他情况：不是 AND 或 =，
      return false;
    };

    if (check_and_extract(nlj_plan.Predicate())) {
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), std::move(left_key_expressions),
                                                std::move(right_key_expressions), nlj_plan.GetJoinType());
    }
  }

  return optimized_plan;
}
  */

}  // namespace bustub
