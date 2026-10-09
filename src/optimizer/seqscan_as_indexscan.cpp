#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {
namespace {
// Only conjunctions imply both bounds. OR branches remain in the full predicate.
void CollectBounds(const AbstractExpressionRef &expr, uint32_t column, TypeId type, IndexScanBounds *bounds) {
  if (const auto *logic = dynamic_cast<const LogicExpression *>(expr.get())) {
    if (logic->logic_type_ == LogicType::And) {
      CollectBounds(logic->GetChildAt(0), column, type, bounds);
      CollectBounds(logic->GetChildAt(1), column, type, bounds);
    }
    return;
  }
  const auto *comparison = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (!comparison) return;
  auto *col = dynamic_cast<const ColumnValueExpression *>(comparison->GetChildAt(0).get());
  auto *constant = dynamic_cast<const ConstantValueExpression *>(comparison->GetChildAt(1).get());
  auto op = comparison->comp_type_;
  if (!col || !constant) {
    col = dynamic_cast<const ColumnValueExpression *>(comparison->GetChildAt(1).get());
    constant = dynamic_cast<const ConstantValueExpression *>(comparison->GetChildAt(0).get());
    if (op == ComparisonType::LessThan)
      op = ComparisonType::GreaterThan;
    else if (op == ComparisonType::LessThanOrEqual)
      op = ComparisonType::GreaterThanOrEqual;
    else if (op == ComparisonType::GreaterThan)
      op = ComparisonType::LessThan;
    else if (op == ComparisonType::GreaterThanOrEqual)
      op = ComparisonType::LessThanOrEqual;
  }
  if (!col || !constant || col->GetColIdx() != column || constant->GetReturnType().GetType() != type) return;
  const auto value = constant->Evaluate(nullptr, Schema({}));
  if (value.IsNull()) return;
  if (op == ComparisonType::GreaterThan || op == ComparisonType::GreaterThanOrEqual || op == ComparisonType::Equal) {
    const bool inclusive = op != ComparisonType::GreaterThan;
    if (!bounds->lower_ || value.CompareGreaterThan(*bounds->lower_) == CmpBool::CmpTrue) {
      bounds->lower_ = value;
      bounds->lower_inclusive_ = inclusive;
    } else if (value.CompareEquals(*bounds->lower_) == CmpBool::CmpTrue)
      bounds->lower_inclusive_ &= inclusive;
  }
  if (op == ComparisonType::LessThan || op == ComparisonType::LessThanOrEqual || op == ComparisonType::Equal) {
    const bool inclusive = op != ComparisonType::LessThan;
    if (!bounds->upper_ || value.CompareLessThan(*bounds->upper_) == CmpBool::CmpTrue) {
      bounds->upper_ = value;
      bounds->upper_inclusive_ = inclusive;
    } else if (value.CompareEquals(*bounds->upper_) == CmpBool::CmpTrue)
      bounds->upper_inclusive_ &= inclusive;
  }
}
}  // namespace

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule

  // 如果plan是一个带谓词的SeqScanPlanNode
  // 并且这个谓词可以直接通过索引来处理
  // 则将其转换为IndexScanPlanNode
  // 否则直接返回原plan

  // 必须先递归优化子节点，因为优化是从底部往上进行的
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  // 重建当前plan节点，使用优化后的子节点
  AbstractPlanNodeRef new_plan = plan->CloneWithChildren(std::move(children));

  // 检查是不是SeqScanPlanNode,不是就返回原plan
  if (new_plan->GetType() == bustub::PlanType::SeqScan) {
    auto seq_scan_plan = std::dynamic_pointer_cast<const bustub::SeqScanPlanNode>(new_plan);
    auto predicate = seq_scan_plan->filter_predicate_;
    // 这个plan有没有谓词,即有没有过滤条件
    if (predicate != nullptr) {
      // 辅助函数：检查表达式是否为 column = constant 或 constant = column
      // 如果是，返回 {column_idx, constant_expr}
      // 否则返回 nullopt
      auto check_equal_expr =
          [](const AbstractExpressionRef &expr) -> std::optional<std::pair<uint32_t, AbstractExpressionRef>> {
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

          // Tuple serializes the value's type, not the index column's type.
          // Mixed-type comparisons keep the SQL predicate's comparison rules
          // through the normal scan instead of encoding an incompatible key.
          if (column_expr != nullptr &&
              column_expr->GetReturnType().GetType() == constant_expr->GetReturnType().GetType()) {
            return std::make_pair(column_expr->GetColIdx(), constant_expr);
          }
        }
        return std::nullopt;
      };

      std::vector<AbstractExpressionRef> pred_keys;
      uint32_t target_col_idx = -1;
      bool is_valid_index_scan = false;

      // Recursive helper to collect OR conditions
      // 遍历谓词表达树，收集所有等值条件
      // 它获取当前表达式节点，一个存放key的vector引用和目标列索引引用
      std::function<bool(const AbstractExpressionRef &, std::vector<AbstractExpressionRef> &, uint32_t &)>
          collect_conditions = [&](const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &keys,
                                   uint32_t &col_idx) -> bool {
        // 表达树中OR作为根节点，递归处理其子节点
        if (auto logic_expr = dynamic_cast<const bustub::LogicExpression *>(expr.get())) {
          if (logic_expr->logic_type_ == bustub::LogicType::Or) {
            return collect_conditions(logic_expr->GetChildAt(0), keys, col_idx) &&
                   collect_conditions(logic_expr->GetChildAt(1), keys, col_idx);
          }
          return false;
        }

        auto equal_res = check_equal_expr(expr);
        if (equal_res.has_value()) {
          // 是第一次发现等值条件，记录列索引
          if (col_idx == static_cast<uint32_t>(-1)) {
            col_idx = equal_res->first;
          } else if (col_idx != equal_res->first) {
            // 发现了不同列的等值条件，不能使用单列索引扫描
            return false;
          }
          // 收集等值条件（把常量表达式存下来）
          keys.push_back(equal_res->second);
          return true;
        }
        return false;
      };

      // A conjunct can restrict candidates; the executor still checks the full
      // predicate. An OR must be covered in full, never by just one branch.
      std::function<bool(const AbstractExpressionRef &)> choose = [&](const AbstractExpressionRef &expr) {
        pred_keys.clear();
        target_col_idx = static_cast<uint32_t>(-1);
        if (collect_conditions(expr, pred_keys, target_col_idx)) return true;
        const auto logic = dynamic_cast<const LogicExpression *>(expr.get());
        return logic != nullptr && logic->logic_type_ == LogicType::And &&
               (choose(logic->GetChildAt(0)) || choose(logic->GetChildAt(1)));
      };
      is_valid_index_scan = choose(predicate);

      if (is_valid_index_scan) {
        // 去catalog里查找有没有对应的索引（看它有没有建立B+树索引）
        auto table_info = catalog_.GetTable(seq_scan_plan->GetTableOid());
        auto indexes = catalog_.GetTableIndexes(table_info->name_);
        for (const auto &index_info : indexes) {
          // 获取该索引所包含的所有列的id
          const auto &key_attrs = index_info->index_->GetKeyAttrs();
          // 检查第一个属性是否是谓词中涉及的列
          if (key_attrs.size() == 1 && key_attrs[0] == target_col_idx) {
            // 找到了合适的索引，可以转换为IndexScanPlanNode
            auto index_scan_plan = std::make_shared<bustub::IndexScanPlanNode>(
                std::make_shared<Schema>(new_plan->OutputSchema()), seq_scan_plan->GetTableOid(),
                index_info->index_oid_, seq_scan_plan->filter_predicate_, pred_keys);
            return index_scan_plan;
          }
        }
      }
      // Reuse the existing ordered B+Tree snapshot and MVCC executor. Other
      // index shapes retain the original scan; no key truncation or cast.
      const auto table = catalog_.GetTable(seq_scan_plan->GetTableOid());
      for (const auto &index : catalog_.GetTableIndexes(table->name_)) {
        if (index->index_type_ != IndexType::BPlusTreeIndex || index->key_size_ != 8 || index->key_attrs_.size() != 1)
          continue;
        IndexScanBounds bounds;
        const auto column = index->key_attrs_[0];
        const auto type = table->schema_.GetColumn(column).GetType();
        if (type != TypeId::INTEGER && type != TypeId::BIGINT) continue;
        CollectBounds(predicate, column, type, &bounds);
        if (bounds.lower_ || bounds.upper_) {
          return std::make_shared<IndexScanPlanNode>(std::make_shared<Schema>(new_plan->OutputSchema()), table->oid_,
                                                     index->index_oid_, predicate, std::move(bounds));
        }
      }
    }
  }
  return new_plan;
}
}  // namespace bustub
