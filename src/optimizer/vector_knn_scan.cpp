#include "optimizer/optimizer.h"

#include "catalog/catalog.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/vector_distance_expression.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/vector_index_scan_plan.h"

namespace bustub {

namespace {

auto IsVectorDistanceExpr(const AbstractExpressionRef &expr) -> bool {
  return dynamic_cast<const VectorDistanceExpression *>(expr.get()) != nullptr;
}

auto IsSupportedVectorIndexType(IndexType index_type) -> bool {
  switch (index_type) {
    case IndexType::IVFFlatIndex:
      return true;
    default:
      return false;
  }
}

auto GetVectorDistanceMetric(const VectorDistanceExpression *expr) -> VectorIndexDistanceMetric {
  switch (expr->expr_type_) {
    case VectorDistanceExpressionType::L2Distance:
      return VectorIndexDistanceMetric::L2;
    case VectorDistanceExpressionType::CosineDistance:
      return VectorIndexDistanceMetric::Cosine;
    case VectorDistanceExpressionType::IPDistance:
      return VectorIndexDistanceMetric::InnerProduct;
  }
  UNREACHABLE("unsupported vector distance expression");
}

auto ContainsColumnValueExpr(const AbstractExpressionRef &expr) -> bool {
  if (dynamic_cast<const ColumnValueExpression *>(expr.get()) != nullptr) {
    return true;
  }
  for (const auto &child : expr->GetChildren()) {
    if (ContainsColumnValueExpr(child)) {
      return true;
    }
  }
  return false;
}

auto ExtractIndexedVectorQuery(const AbstractExpressionRef &expr)
    -> std::optional<std::pair<uint32_t, AbstractExpressionRef>> {
  const auto *distance_expr = dynamic_cast<const VectorDistanceExpression *>(expr.get());
  if (distance_expr == nullptr) {
    return std::nullopt;
  }

  const auto &left = distance_expr->GetChildAt(0);
  const auto &right = distance_expr->GetChildAt(1);

  const auto *left_col = dynamic_cast<const ColumnValueExpression *>(left.get());
  if (left_col != nullptr && left_col->GetTupleIdx() == 0 && !ContainsColumnValueExpr(right)) {
    return std::make_pair(left_col->GetColIdx(), right);
  }

  const auto *right_col = dynamic_cast<const ColumnValueExpression *>(right.get());
  if (right_col != nullptr && right_col->GetTupleIdx() == 0 && !ContainsColumnValueExpr(left)) {
    return std::make_pair(right_col->GetColIdx(), left);
  }

  return std::nullopt;
}

}  // namespace

// 识别 Limit(Sort(child)) 模式。只有命中匹配的向量索引时才改写为
// VectorIndexScanPlanNode；否则保持原计划，让后续的 TopN 规则走 exact path。
auto Optimizer::OptimizeVectorKnnScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeVectorKnnScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::Limit) {
    return optimized_plan;
  }

  const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
  const auto &child_plan = limit_plan.GetChildPlan();
  if (child_plan->GetType() != PlanType::Sort) {
    return optimized_plan;
  }

  const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child_plan);
  const auto &order_bys = sort_plan.GetOrderBy();
  if (order_bys.size() != 1) {
    return optimized_plan;
  }

  const auto &[order_type, distance_expr] = order_bys[0];
  if (!(order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) || !IsVectorDistanceExpr(distance_expr)) {
    return optimized_plan;
  }

  if (sort_plan.GetChildPlan()->GetType() == PlanType::SeqScan) {
    const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(*sort_plan.GetChildPlan());
    if (seq_scan.filter_predicate_ == nullptr) {
      auto vector_query = ExtractIndexedVectorQuery(distance_expr);
      if (vector_query.has_value()) {
        const auto *distance = dynamic_cast<const VectorDistanceExpression *>(distance_expr.get());
        BUSTUB_ASSERT(distance != nullptr, "vector distance expression expected");
        const auto required_metric = GetVectorDistanceMetric(distance);
        auto table_info = catalog_.GetTable(seq_scan.GetTableOid());
        const auto indexes = catalog_.GetTableIndexes(table_info->name_);
        for (const auto &index_info : indexes) {
          if (!IsSupportedVectorIndexType(index_info->index_type_)) {
            continue;
          }
          const auto &key_attrs = index_info->index_->GetKeyAttrs();
          if (key_attrs.size() == 1 && key_attrs[0] == vector_query->first &&
              index_info->index_->GetVectorDistanceMetric() == required_metric) {
            return std::make_shared<VectorIndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_,
                                                             index_info->index_oid_, vector_query->second,
                                                             limit_plan.GetLimit(), distance_expr);
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
