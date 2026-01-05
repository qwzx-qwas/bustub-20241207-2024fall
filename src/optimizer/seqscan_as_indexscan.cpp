#include "optimizer/optimizer.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"

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
  auto new_plan = plan->CloneWithChildren(std::move(children));

  if(new_plan->GetType() != bustub::PlanType::SeqScan) {
      return new_plan;
  }
  //检查是不是SeqScanPlanNode,不是就返回原plan
  if(new_plan->GetType() == bustub::PlanType::SeqScan) {
    auto seq_scan_plan = std::dynamic_pointer_cast<const bustub::SeqScanPlanNode>(new_plan);
    auto predicate = seq_scan_plan->filter_predicate_;
    //这个plan有没有谓词,即有没有过滤条件
    if(predicate != nullptr) {
    //解析谓词
    //看他是不是简单的等值比较（比较表达式）
    auto comparison_expr = dynamic_cast<const bustub::ComparisonExpression *>(predicate.get());
    //只处理等值比较
    if(comparison_expr != nullptr && comparison_expr->comp_type_ == bustub::ComparisonType::Equal) {
        //获取比较表达式的左右操作数
        const auto &left_expr = comparison_expr->GetChildAt(0);
        const auto &right_expr = comparison_expr->GetChildAt(1);
        //其中一个操作数是列引用，另一个是常量
        const bustub::ColumnValueExpression *column_expr = nullptr;
        std::shared_ptr<bustub::AbstractExpression> constant_expr = nullptr;

        if(dynamic_cast<const bustub::ColumnValueExpression *>(left_expr.get()) != nullptr &&
           dynamic_cast<const bustub::ConstantValueExpression *>(right_expr.get()) != nullptr) {
            column_expr = dynamic_cast<const bustub::ColumnValueExpression *>(left_expr.get());
            constant_expr = right_expr;
        } else if(dynamic_cast<const bustub::ColumnValueExpression *>(right_expr.get()) != nullptr &&
                  dynamic_cast<const bustub::ConstantValueExpression *>(left_expr.get()) != nullptr) {
            column_expr = dynamic_cast<const bustub::ColumnValueExpression *>(right_expr.get());
            constant_expr = left_expr;
        } else {
            //不是列和值的比较，返回原plan
            return new_plan;
        }

    //去catalog里查找有没有对应的索引（看它有没有建立B+树索引）
    auto table_info = catalog_->GetTable(seq_scan_plan->GetTableOid());
    auto indexes = catalog_->GetTableIndexes(table_info->name_);
    for (const auto &index_info : indexes) {
        //获取该索引所包含的所有列的id
        const auto &key_attrs = index_info->index_->GetKeyAttrs();
        //检查第一个属性是否是谓词中涉及的列
        if(!key_attrs.empty() && key_attrs[0] == column_expr->GetColIdx()) {
            //找到了合适的索引，可以转换为IndexScanPlanNode
            std::vector<std::shared_ptr<bustub::AbstractExpression>> pred_keys;
            //构造索引键值表达式列表
            pred_keys.push_back(constant_expr);
            //构造IndexScanPlanNode
            auto index_scan_plan = std::make_shared<bustub::IndexScanPlanNode>(
                new_plan->GetOutputSchema(), seq_scan_plan->GetTableOid(), index_info->index_oid_, 
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
