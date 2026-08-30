#include "execution/executors/topn_executor.h"
#include <queue>
#include "execution/execution_common.h"
#include "execution/executor_factory.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      heap_(TopNComparator(TupleComparator(plan->GetOrderBy()))),
      cmp_(plan->GetOrderBy()) {}

/** “从子执行器读入所有 tuple，只保留按 ORDER BY 排序后最靠前的 N 条，然后按正确顺序逐条输出”。 */
void TopNExecutor::Init() {
  if (plan_->GetN() == 0) {
    return;
  }
  results_.clear();
  result_idx_ = 0;
  heap_ = std::priority_queue<TopNEntry, std::vector<TopNEntry>, TopNComparator>(
      TopNComparator(cmp_));  // 重新初始化堆，清空之前的内容
  if (child_executor_ == nullptr) {
    child_executor_ = ExecutorFactory::CreateExecutor(exec_ctx_, plan_->GetChildPlan());
  }
  child_executor_->Init();

  // 维护优于堆顶的topn
  Tuple tuple;
  RID rid;
  const auto &schema = child_executor_->GetOutputSchema();
  while (child_executor_->Next(&tuple, &rid)) {
    auto key = GenerateSortKey(tuple, plan_->GetOrderBy(), schema);
    TopNEntry entry{key, {tuple, rid}};

    if (heap_.size() < plan_->GetN()) {
      heap_.push(entry);
      continue;
    }

    // 如果新tuple比当前top-n里最差的那个还好，就把它加入top-n
    const auto &worst_entry = heap_.top();
    if (cmp_({entry.first, entry.second.first}, {worst_entry.first, worst_entry.second.first})) {
      heap_.pop();
      heap_.push(std::move(entry));
    }
  }

  // 把堆里的元素按正确顺序输出到一个pair里
  while (!heap_.empty()) {
    results_.push_back(heap_.top().second);  // 注意这里是push_back到vector里，不是push到heap里
    heap_.pop();
  }
  std::reverse(results_.begin(), results_.end());  // 因为堆顶是最差的那个，所以要反转一下
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_idx_ >= results_.size()) {
    return false;
  }
  *tuple = results_[result_idx_].first;
  *rid = results_[result_idx_].second;
  result_idx_++;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_.size(); };

}  // namespace bustub
