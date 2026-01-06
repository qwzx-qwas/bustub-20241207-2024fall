#include "optimizer/optimizer.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule

  //如果plan是一个带谓词的SeqScanPlanNode
  //并且这个谓词可以直接通过索引来处理
  //则将其转换为IndexScanPlanNode
  //否则直接返回原plan
  
  //必须先递归优化子节点，因为优化是从底部往上进行的
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  //重建当前plan节点，使用优化后的子节点
  AbstractPlanNodeRef new_plan = plan->CloneWithChildren(std::move(children));

  //检查是不是SeqScanPlanNode,不是就返回原plan
  if(new_plan->GetType() == bustub::PlanType::SeqScan) {
    auto seq_scan_plan = std::dynamic_pointer_cast<const bustub::SeqScanPlanNode>(new_plan);
    auto predicate = seq_scan_plan->filter_predicate_;
    //这个plan有没有谓词,即有没有过滤条件
    if(predicate != nullptr) {
        
        // 辅助函数：检查表达式是否为 column = constant 或 constant = column
        // 如果是，返回 {column_idx, constant_expr}
        // 否则返回 nullopt
        auto check_equal_expr = [](const AbstractExpressionRef &expr) 
            -> std::optional<std::pair<uint32_t, AbstractExpressionRef>> {
            auto comparison_expr = dynamic_cast<const bustub::ComparisonExpression *>(expr.get());
            if (comparison_expr != nullptr && comparison_expr->comp_type_ == bustub::ComparisonType::Equal) {
                const auto &left_expr = comparison_expr->GetChildAt(0);
                const auto &right_expr = comparison_expr->GetChildAt(1);
                
                const bustub::ColumnValueExpression *column_expr = nullptr;
                AbstractExpressionRef constant_expr = nullptr;

                if (dynamic_cast<const bustub::ColumnValueExpression *>(left_expr.get()) != nullptr &&
                    dynamic_cast<const bustub::ConstantValueExpression *>(right_expr.get()) != nullptr) {
                    column_expr = dynamic_cast<const bustub::ColumnValueExpression *>(left_expr.get());
                    constant_expr = right_expr;
                } else if (dynamic_cast<const bustub::ColumnValueExpression *>(right_expr.get()) != nullptr &&
                           dynamic_cast<const bustub::ConstantValueExpression *>(left_expr.get()) != nullptr) {
                    column_expr = dynamic_cast<const bustub::ColumnValueExpression *>(right_expr.get());
                    constant_expr = left_expr;
                }
                
                if (column_expr != nullptr) {
                    return std::make_pair(column_expr->GetColIdx(), constant_expr);
                }
            }
            return std::nullopt;
        };

        std::vector<AbstractExpressionRef> pred_keys;
        uint32_t target_col_idx = -1;
        bool is_valid_index_scan = false;

        // Recursive helper to collect OR conditions
        //遍历谓词表达树，收集所有等值条件
        //它获取当前表达式节点，一个存放key的vector引用和目标列索引引用
        std::function<bool(const AbstractExpressionRef &, std::vector<AbstractExpressionRef> &, uint32_t &)> 
        collect_conditions = [&](const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &keys, uint32_t &col_idx) -> bool {
            //表达树中OR作为根节点，递归处理其子节点
             if (auto logic_expr = dynamic_cast<const bustub::LogicExpression *>(expr.get())) {
                if (logic_expr->logic_type_ == bustub::LogicType::Or) {
                    return collect_conditions(logic_expr->GetChildAt(0), keys, col_idx) &&
                           collect_conditions(logic_expr->GetChildAt(1), keys, col_idx);
                }
                return false;
            }
            
            auto equal_res = check_equal_expr(expr);
            if (equal_res.has_value()) {
                //是第一次发现等值条件，记录列索引
                if (col_idx == static_cast<uint32_t>(-1)) {
                    col_idx = equal_res->first;
                } else if (col_idx != equal_res->first) {
                    //发现了不同列的等值条件，不能使用单列索引扫描
                    return false;
                }
                //收集等值条件（把常量表达式存下来）
                keys.push_back(equal_res->second);
                return true;
            }
            return false;
        };

        if (collect_conditions(predicate, pred_keys, target_col_idx)) {
             is_valid_index_scan = true;
        }
        
        if (is_valid_index_scan) {
            //去catalog里查找有没有对应的索引（看它有没有建立B+树索引）
            auto table_info = catalog_.GetTable(seq_scan_plan->GetTableOid());
            auto indexes = catalog_.GetTableIndexes(table_info->name_);
            for (const auto &index_info : indexes) {
                //获取该索引所包含的所有列的id
                const auto &key_attrs = index_info->index_->GetKeyAttrs();
                //检查第一个属性是否是谓词中涉及的列
                if(!key_attrs.empty() && key_attrs[0] == target_col_idx) {
                    //找到了合适的索引，可以转换为IndexScanPlanNode
                    auto index_scan_plan = std::make_shared<bustub::IndexScanPlanNode>(
                        std::make_shared<Schema>(new_plan->OutputSchema()), seq_scan_plan->GetTableOid(), index_info->index_oid_, 
                        seq_scan_plan->filter_predicate_, pred_keys);
                    return index_scan_plan; 
                }
            }
        }
    }
  }
  return new_plan;
}
}  // namespace bustub
