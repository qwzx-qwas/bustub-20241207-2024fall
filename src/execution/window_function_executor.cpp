#include "execution/executors/window_function_executor.h"

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

namespace {

struct OrderedRow {
  size_t row_idx_;
  std::vector<Value> order_key_;
};

auto KeysEqual(const std::vector<Value> &lhs, const std::vector<Value> &rhs) -> bool {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (size_t idx = 0; idx < lhs.size(); idx++) {
    if (lhs[idx].IsNull() || rhs[idx].IsNull()) {
      if (!(lhs[idx].IsNull() && rhs[idx].IsNull())) {
        return false;
      }
      continue;
    }
    if (lhs[idx].CompareEquals(rhs[idx]) != CmpBool::CmpTrue) {
      return false;
    }
  }
  return true;
}

auto LessByOrder(const std::vector<Value> &lhs, const std::vector<Value> &rhs,
                 const std::vector<OrderByType> &order_types) -> bool {
  for (size_t idx = 0; idx < lhs.size(); idx++) {
    const auto order_type = order_types[idx];
    if (lhs[idx].IsNull() || rhs[idx].IsNull()) {
      if (lhs[idx].IsNull() && rhs[idx].IsNull()) {
        continue;
      }
      // Match PostgreSQL defaults: NULLS LAST for ASC/DEFAULT and NULLS FIRST for DESC.
      return order_type == OrderByType::DESC ? lhs[idx].IsNull() : !lhs[idx].IsNull();
    }
    if (lhs[idx].CompareEquals(rhs[idx]) == CmpBool::CmpTrue) {
      continue;
    }
    if (order_type == OrderByType::DESC) {
      return lhs[idx].CompareGreaterThan(rhs[idx]) == CmpBool::CmpTrue;
    }
    return lhs[idx].CompareLessThan(rhs[idx]) == CmpBool::CmpTrue;
  }
  return false;
}

auto EvaluateExpressions(const Tuple &tuple, const Schema &schema,
                         const std::vector<AbstractExpressionRef> &expressions) -> std::vector<Value> {
  std::vector<Value> values;
  values.reserve(expressions.size());
  for (const auto &expression : expressions) {
    values.push_back(expression->Evaluate(&tuple, schema));
  }
  return values;
}

auto EvaluateOrderKey(const Tuple &tuple, const Schema &schema, const std::vector<OrderBy> &order_bys)
    -> std::vector<Value> {
  std::vector<Value> values;
  values.reserve(order_bys.size());
  for (const auto &order_by : order_bys) {
    values.push_back(order_by.second->Evaluate(&tuple, schema));
  }
  return values;
}

auto InitialWindowValue(WindowFunctionType type, TypeId output_type) -> Value {
  if (type == WindowFunctionType::CountStarAggregate || type == WindowFunctionType::CountAggregate ||
      type == WindowFunctionType::Rank) {
    return ValueFactory::GetIntegerValue(0);
  }
  return ValueFactory::GetNullValueByType(output_type);
}

void AccumulateWindowValue(Value *state, WindowFunctionType type, const Value &input) {
  switch (type) {
    case WindowFunctionType::CountStarAggregate:
      *state = ValueFactory::GetIntegerValue(state->GetAs<int32_t>() + 1);
      return;
    case WindowFunctionType::CountAggregate:
      if (!input.IsNull()) {
        *state = ValueFactory::GetIntegerValue(state->GetAs<int32_t>() + 1);
      }
      return;
    case WindowFunctionType::SumAggregate:
      if (!input.IsNull()) {
        *state = state->IsNull() ? input : state->Add(input);
      }
      return;
    case WindowFunctionType::MinAggregate:
      if (!input.IsNull() && (state->IsNull() || input.CompareLessThan(*state) == CmpBool::CmpTrue)) {
        *state = input;
      }
      return;
    case WindowFunctionType::MaxAggregate:
      if (!input.IsNull() && (state->IsNull() || input.CompareGreaterThan(*state) == CmpBool::CmpTrue)) {
        *state = input;
      }
      return;
    case WindowFunctionType::Rank:
      return;
  }
}

}  // namespace

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  result_tuples_.clear();
  result_idx_ = 0;

  std::vector<Tuple> input_tuples;
  Tuple input_tuple;
  RID input_rid;
  while (child_executor_->Next(&input_tuple, &input_rid)) {
    input_tuples.push_back(input_tuple);
  }
  if (input_tuples.empty()) {
    return;
  }

  const auto &input_schema = child_executor_->GetOutputSchema();
  std::vector<std::vector<Value>> output_values(input_tuples.size());
  for (size_t row_idx = 0; row_idx < input_tuples.size(); row_idx++) {
    output_values[row_idx].reserve(plan_->columns_.size());
    for (uint32_t column_idx = 0; column_idx < plan_->columns_.size(); column_idx++) {
      if (plan_->window_functions_.count(column_idx) != 0) {
        output_values[row_idx].push_back(
            ValueFactory::GetNullValueByType(plan_->OutputSchema().GetColumn(column_idx).GetType()));
      } else {
        output_values[row_idx].push_back(plan_->columns_[column_idx]->Evaluate(&input_tuples[row_idx], input_schema));
      }
    }
  }

  std::vector<uint32_t> window_columns;
  window_columns.reserve(plan_->window_functions_.size());
  for (const auto &[column_idx, window_function] : plan_->window_functions_) {
    static_cast<void>(window_function);
    window_columns.push_back(column_idx);
  }
  std::sort(window_columns.begin(), window_columns.end());

  for (const auto column_idx : window_columns) {
    const auto &window_function = plan_->window_functions_.at(column_idx);
    std::unordered_map<AggregateKey, std::vector<size_t>> partitions;
    for (size_t row_idx = 0; row_idx < input_tuples.size(); row_idx++) {
      auto partition_key = EvaluateExpressions(input_tuples[row_idx], input_schema, window_function.partition_by_);
      partitions[AggregateKey{std::move(partition_key)}].push_back(row_idx);
    }

    std::vector<OrderByType> order_types;
    order_types.reserve(window_function.order_by_.size());
    for (const auto &order_by : window_function.order_by_) {
      order_types.push_back(order_by.first);
    }

    for (const auto &[partition_key, partition_rows] : partitions) {
      static_cast<void>(partition_key);
      std::vector<OrderedRow> ordered_rows;
      ordered_rows.reserve(partition_rows.size());
      for (const auto row_idx : partition_rows) {
        ordered_rows.push_back(
            OrderedRow{row_idx, EvaluateOrderKey(input_tuples[row_idx], input_schema, window_function.order_by_)});
      }
      if (!window_function.order_by_.empty()) {
        std::stable_sort(ordered_rows.begin(), ordered_rows.end(), [&](const auto &lhs, const auto &rhs) {
          return LessByOrder(lhs.order_key_, rhs.order_key_, order_types);
        });
      }

      if (window_function.type_ == WindowFunctionType::Rank) {
        int32_t rank = 1;
        for (size_t pos = 0; pos < ordered_rows.size(); pos++) {
          if (pos > 0 && !KeysEqual(ordered_rows[pos - 1].order_key_, ordered_rows[pos].order_key_)) {
            rank = static_cast<int32_t>(pos + 1);
          }
          output_values[ordered_rows[pos].row_idx_][column_idx] = ValueFactory::GetIntegerValue(rank);
        }
        continue;
      }

      auto state = InitialWindowValue(window_function.type_, plan_->OutputSchema().GetColumn(column_idx).GetType());
      if (window_function.order_by_.empty()) {
        for (const auto &ordered_row : ordered_rows) {
          auto input = window_function.function_->Evaluate(&input_tuples[ordered_row.row_idx_], input_schema);
          AccumulateWindowValue(&state, window_function.type_, input);
        }
        for (const auto &ordered_row : ordered_rows) {
          output_values[ordered_row.row_idx_][column_idx] = state;
        }
      } else {
        for (const auto &ordered_row : ordered_rows) {
          auto input = window_function.function_->Evaluate(&input_tuples[ordered_row.row_idx_], input_schema);
          AccumulateWindowValue(&state, window_function.type_, input);
          output_values[ordered_row.row_idx_][column_idx] = state;
        }
      }
    }
  }

  std::vector<size_t> output_order(input_tuples.size());
  std::iota(output_order.begin(), output_order.end(), 0);
  for (const auto column_idx : window_columns) {
    const auto &window_function = plan_->window_functions_.at(column_idx);
    if (window_function.order_by_.empty()) {
      continue;
    }
    std::vector<OrderedRow> output_rows;
    output_rows.reserve(input_tuples.size());
    std::vector<OrderByType> combined_order_types(window_function.partition_by_.size(), OrderByType::ASC);
    for (const auto &order_by : window_function.order_by_) {
      combined_order_types.push_back(order_by.first);
    }
    for (size_t row_idx = 0; row_idx < input_tuples.size(); row_idx++) {
      auto key = EvaluateExpressions(input_tuples[row_idx], input_schema, window_function.partition_by_);
      auto order_key = EvaluateOrderKey(input_tuples[row_idx], input_schema, window_function.order_by_);
      key.insert(key.end(), order_key.begin(), order_key.end());
      output_rows.push_back(OrderedRow{row_idx, std::move(key)});
    }
    std::stable_sort(output_rows.begin(), output_rows.end(), [&](const auto &lhs, const auto &rhs) {
      return LessByOrder(lhs.order_key_, rhs.order_key_, combined_order_types);
    });
    for (size_t idx = 0; idx < output_rows.size(); idx++) {
      output_order[idx] = output_rows[idx].row_idx_;
    }
    break;
  }

  result_tuples_.reserve(input_tuples.size());
  for (const auto row_idx : output_order) {
    result_tuples_.emplace_back(output_values[row_idx], &plan_->OutputSchema());
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_idx_ >= result_tuples_.size()) {
    return false;
  }
  *tuple = result_tuples_[result_idx_++];
  *rid = RID{};
  return true;
}
}  // namespace bustub
