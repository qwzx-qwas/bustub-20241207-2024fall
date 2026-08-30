//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// type_cast_expression.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "execution/expressions/abstract_expression.h"

namespace bustub {

/** Converts its child value to the declared SQL type. */
class TypeCastExpression : public AbstractExpression {
 public:
  TypeCastExpression(AbstractExpressionRef child, Column return_type)
      : AbstractExpression({std::move(child)}, std::move(return_type)) {}

  auto Evaluate(const Tuple *tuple, const Schema &schema) const -> Value override {
    return GetChildAt(0)->Evaluate(tuple, schema).CastAs(GetReturnType().GetType());
  }

  auto EvaluateJoin(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
                    const Schema &right_schema) const -> Value override {
    return GetChildAt(0)
        ->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema)
        .CastAs(GetReturnType().GetType());
  }

  auto ToString() const -> std::string override {
    return fmt::format("CAST({} AS {})", GetChildAt(0), Type::TypeIdToString(GetReturnType().GetType()));
  }

  BUSTUB_EXPR_CLONE_WITH_CHILDREN(TypeCastExpression);
};

}  // namespace bustub
