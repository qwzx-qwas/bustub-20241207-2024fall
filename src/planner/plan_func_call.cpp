#include <memory>
#include <tuple>
#include "binder/bound_expression.h"
#include "binder/bound_statement.h"
#include "binder/expressions/bound_agg_call.h"
#include "binder/expressions/bound_alias.h"
#include "binder/expressions/bound_binary_op.h"
#include "binder/expressions/bound_column_ref.h"
#include "binder/expressions/bound_constant.h"
#include "binder/expressions/bound_func_call.h"
#include "binder/expressions/bound_unary_op.h"
#include "binder/statement/select_statement.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/util/string_util.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/string_expression.h"
#include "execution/plans/abstract_plan.h"
#include "fmt/format.h"
#include "planner/planner.h"
#include "execution/expressions/vector_distance_expression.h"

namespace bustub {

// NOLINTNEXTLINE
auto Planner::GetFuncCallFromFactory(const std::string &func_name, std::vector<AbstractExpressionRef> args)
    -> AbstractExpressionRef {
  // 1. check if the parsed function name is "lower" or "upper".
  // 2. verify the number of args (should be 1), refer to the test cases for when you should throw an `Exception`.
  // 3. return a `StringExpression` std::shared_ptr.
  const auto name = StringUtil::Lower(func_name);
  if (name == "lower" || name == "upper") {
    if (args.size() != 1) {
      throw Exception(fmt::format("function {} expects 1 argument, but got {}", func_name, args.size()));
    }
    return std::make_shared<StringExpression>(args[0], name == "lower" ? StringExpressionType::Lower
                                                                        : StringExpressionType::Upper);
    }
  //l2_distance是欧几里得距离，cosine_distance是余弦距离，ip_distance是内积距离
  //取两个向量作为输入，输出一个标量值，分别表示两个向量之间的距离或相似度。
  if (name == "l2_distance" || name == "cosine_distance" || name == "ip_distance" ) {
    if (args.size() != 2) {
      throw Exception(fmt::format("function {} expects 2 arguments, but got {}", func_name, args.size()));
    }
    
    if (args[0] -> GetReturnType().GetType() != TypeId::VECTOR || args[1] -> GetReturnType().GetType() != TypeId::VECTOR) {
      throw Exception(fmt::format("function {} expects arguments of type VECTOR, but got {} and {}", func_name, args[0] -> GetReturnType(), args[1] -> GetReturnType()));
    }

    return std::make_shared<VectorDistanceExpression>(args[0], args[1], name == "l2_distance" ? VectorDistanceExpressionType::L2Distance
                                                                                          : (name == "cosine_distance" ? VectorDistanceExpressionType::CosineDistance : VectorDistanceExpressionType::IPDistance));
  }
  
  throw Exception(fmt::format("function {} is not supported", func_name));
}

}  // namespace bustub
