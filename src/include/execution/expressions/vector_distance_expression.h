#pragma once

#include <cmath>
#include <functional>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "common/exception.h"
#include "execution/expressions/abstract_expression.h"
#include "fmt/format.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

enum class VectorDistanceExpressionType { L2Distance, CosineDistance, IPDistance };

/**
 * VectorDistanceExpression represents two expressions being computed.
 */
class VectorDistanceExpression : public AbstractExpression {
 public:
  VectorDistanceExpression(AbstractExpressionRef left, AbstractExpressionRef right,
                           VectorDistanceExpressionType expr_type)
      : AbstractExpression({std::move(left), std::move(right)}, Column{"<val>", TypeId::DECIMAL}),
        expr_type_{expr_type} {
    if (GetChildAt(0)->GetReturnType().GetType() != TypeId::VECTOR ||
        GetChildAt(1)->GetReturnType().GetType() != TypeId::VECTOR) {
      BUSTUB_ENSURE(GetChildAt(0)->GetReturnType().GetType() == TypeId::VECTOR &&
                        GetChildAt(1)->GetReturnType().GetType() == TypeId::VECTOR,
                    "unexpected arg");
    }
  }

  auto Compute(const std::vector<double> &left, const std::vector<double> &right) const -> double {
    // Compute the selected distance using the fixed V1 vector semantics.
    if (left.size() != right.size()) {
      throw Exception(fmt::format("vector size mismatch: {} and {}", left.size(), right.size()));
    }
    const double dot =
        std::transform_reduce(left.begin(), left.end(), right.begin(), 0.0, std::plus<>(), std::multiplies<>());
    switch (expr_type_) {
      case VectorDistanceExpressionType::L2Distance:
        // compute L2 distance
        {
          const double l2_distance = std::transform_reduce(left.begin(), left.end(), right.begin(), 0.0, std::plus<>(),
                                                           [](double a, double b) { return (a - b) * (a - b); });
          return std::sqrt(l2_distance);
        }
        break;
      case VectorDistanceExpressionType::CosineDistance:
        // compute cosine distance
        {
          const double norm_l_sq =
              std::transform_reduce(left.begin(), left.end(), 0.0, std::plus<>(), [](double x) { return x * x; });

          const double norm_r_sq =
              std::transform_reduce(right.begin(), right.end(), 0.0, std::plus<>(), [](double x) { return x * x; });

          const double denom = std::sqrt(norm_l_sq) * std::sqrt(norm_r_sq);
          if (denom == 0.0) {
            return 1.0;
          }

          return 1.0 - dot / denom;
        }

        break;
      case VectorDistanceExpressionType::IPDistance:
        // compute inner product distance
        return -dot;
        break;
      default:
        throw Exception(fmt::format("unsupported vector distance expression type: {}", static_cast<int>(expr_type_)));
    }
  }

  // 单表普通场景
  auto Evaluate(const Tuple *tuple, const Schema &schema) const -> Value override {
    auto left_val = GetChildAt(0)->Evaluate(tuple, schema);
    auto right_val = GetChildAt(1)->Evaluate(tuple, schema);
    if (left_val.IsNull() || right_val.IsNull()) {
      return ValueFactory::GetNullValueByType(TypeId::DECIMAL);
    }
    auto left_vec = left_val.GetVector();
    auto right_vec = right_val.GetVector();
    return ValueFactory::GetDecimalValue(Compute(left_vec, right_vec));
  }

  // join场景
  auto EvaluateJoin(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
                    const Schema &right_schema) const -> Value override {
    auto left_val = GetChildAt(0)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    auto right_val = GetChildAt(1)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    if (left_val.IsNull() || right_val.IsNull()) {
      return ValueFactory::GetNullValueByType(TypeId::DECIMAL);
    }
    auto left_vec = left_val.GetVector();
    auto right_vec = right_val.GetVector();
    return ValueFactory::GetDecimalValue(Compute(left_vec, right_vec));
  }

  auto ToString() const -> std::string override {
    const char *name = "unknown";
    switch (expr_type_) {
      case VectorDistanceExpressionType::L2Distance:
        name = "l2_distance";
        break;
      case VectorDistanceExpressionType::CosineDistance:
        name = "cosine_distance";
        break;
      case VectorDistanceExpressionType::IPDistance:
        name = "ip_distance";
        break;
    }
    // 用 fmt::format（用于把模板字符串和参数拼成最终字符串）来格式化字符串，输出类似 "l2_distance(child0, child1)"
    // 的结果
    return fmt::format("{}({}, {})", name, *GetChildAt(0), *GetChildAt(1));
  }

  /*
  这个宏的作用：给你的表达式类自动实现 CloneWithChildren。
  在 abstract_expression.h:19 里可以看到展开内容：
  复制当前表达式对象，然后把新的 children 塞进去，
  返回一个新的表达式实例。
  这是优化器/重写规则在“替换子表达式”时必须用到的能力；
  没有它，表达式树无法安全复制改写。
  因为基类把 CloneWithChildren 设成纯虚函数
  （见 abstract_expression.h:78），你不实现就会导致类仍是抽象类，
  后续构造/克隆时报编译错误。
  放到向量扩展里，它的意义是：
  当你有 VectorDistanceExpression(left, right) 这种二元表达式时，
  优化器可以把 left/right 换成别的表达式，重新生成一个等价的新节点，
  而不破坏原树。
  */
  BUSTUB_EXPR_CLONE_WITH_CHILDREN(VectorDistanceExpression);

  VectorDistanceExpressionType expr_type_;
};
}  // namespace bustub
