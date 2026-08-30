//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct HashJoinKey {
  // 对于不同属性的连接键,需要支持多个属性的连接键
  std::vector<Value> column_values_;

  // 重载==运算符
  // 如果存在哈希冲突，就要逐个对比属性值是否相等
  auto operator==(const HashJoinKey &other) const -> bool {
    // 先检查属性值数量是否相等
    auto other_size = other.column_values_.size();
    if (column_values_.size() != other_size) {
      return false;
    }

    // 直接将other和vector里的每个值进行对比
    for (uint32_t i = 0; i < other_size; i++) {
      if (column_values_[i].CompareEquals(other.column_values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

// 告诉编译器，当遇到bustub::HashJoinKey时，使用这个哈希函数
// 特化std::hash模板
namespace std {
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &key) const -> std::size_t {
    // 初始化哈希值
    size_t curr_hash = 0;

    // 对每个属性值计算哈希值，并组合成最终哈希值
    for (const auto &val : key.column_values_) {
      if (!val.IsNull()) {
        // 使用已有的哈希函数计算单个属性值的哈希值
        // HashUtil::HashValue(&val) 用于算出当前这个属性值的哈希值
        // HashUtil::CombineHashes(curr_hash, new_hash) 用于把当前属性值的哈希值和之前的哈希值结合起来
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&val));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  // 左右子执行器
  std::unique_ptr<AbstractExecutor> left_child_executor_;
  std::unique_ptr<AbstractExecutor> right_child_executor_;
  // 基于右子执行器构建的哈希表
  std::unordered_map<HashJoinKey, std::vector<Tuple>> ht_;
  // 输出缓冲区
  std::vector<Tuple> result_buffer_;

  // 暂存schema
  Schema left_schema_{std::vector<Column>{}};
  Schema right_schema_{std::vector<Column>{}};

  // 布隆过滤器
  std::vector<bool> bloom_filter_;
};

}  // namespace bustub
