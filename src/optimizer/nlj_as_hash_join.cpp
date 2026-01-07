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

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  //将 NestedLoopJoinPlan 转换为 HashJoinPlan
  //当连接谓词（Join Predicate）是两个列之间若干个等值条件的逻辑与（AND）组合时，就可以使用哈希连接算法。
  //这里不需要决定左右表（不考虑大小，这个项目正确性优先于性能），只需要提取出所有的等值条件即可。

  //必须先递归优化子节点，因为优化是从底部往上进行的
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
                    return check_and_extract(logic_expr->GetChildAt(0)) && 
                           check_and_extract(logic_expr->GetChildAt(1));
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
          return std::make_shared<HashJoinPlanNode>(
              nlj_plan.output_schema_,
              nlj_plan.GetLeftPlan(),
              nlj_plan.GetRightPlan(),
              std::move(left_key_expressions),
              std::move(right_key_expressions),
              nlj_plan.GetJoinType()
          );
      }
  }

  return optimized_plan;
}

}  // namespace bustub
