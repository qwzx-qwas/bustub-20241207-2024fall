//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <algorithm>
#include <iostream>
#include <optional>
#include <vector>
#include "common/config.h"
#include "execution/plans/sort_plan.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()), child_executor_(std::move(child_executor)) {}

template <size_t K>
void ExternalMergeSortExecutor<K>::GenerateRun(const std::vector<SortEntry> &run, std::vector<page_id_t> &pages) {
  if (run.empty()) {
    return;
  }

  // 先创建第一个排序页,用bpm先分配一个page，然后将其转换为SortPage指针
  page_id_t current_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
  if (current_page_id == INVALID_PAGE_ID) {
    throw Exception("BufferPoolManager is full");
  }
  auto guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_page_id);
  auto *sort_page = reinterpret_cast<SortPage *>(guard.GetDataMut());

  pages.push_back(current_page_id);
  size_t slot_idx = 0;

  for (const auto &entry : run) {
    const Tuple &t = entry.second;
    // 尝试将元组写入当前排序页
    if (!sort_page->WriteTuple(t, tuple_size_, slot_idx)) {
      // 页面满，更新旧页面的 TupleCount
      sort_page->SetTupleCount(slot_idx);

      // 如果当前排序页已满，申请新页
      current_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      if (current_page_id == INVALID_PAGE_ID) {
        throw Exception("BufferPoolManager is full");
      }
      // 转移 Guard 所有权到新页
      guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_page_id);
      // 因为sort_page是指针，重新赋值guard后需要重新获取指针
      sort_page = reinterpret_cast<SortPage *>(guard.GetDataMut());
      pages.push_back(current_page_id);
      slot_idx = 0;
      // 将元组写入新的排序页
      bool write_success = sort_page->WriteTuple(t, tuple_size_, slot_idx);
      BUSTUB_ASSERT(write_success, "Tuple size exceeds SortPage capacity");
    }
    slot_idx++;
  }
  // 更新最后一页的 TupleCount
  sort_page->SetTupleCount(slot_idx);
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::MergeRuns() -> std::optional<MergeSortRun> {
  // TODO(qwzx): implement merge logic if needed, but for now we focus on Init
  return std::nullopt;
}

template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  if (current_iterator_.has_value()) {
    current_iterator_ = std::nullopt;
  }
  for (auto &run : sorted_runs_) {
    run.DeletePages();
  }
  sorted_runs_.clear();

  //初始化子执行器
  child_executor_->Init();
  // tuple长度
  const Schema &schema = child_executor_->GetOutputSchema();
  //因为序列化和反序列化时会在开头写入或读取一个4字节的uint32_t表示元组的长度
  //所以这里要加上这个长度
  tuple_size_ = schema.GetInlinedStorageSize() + sizeof(uint32_t);

  //在init中将子执行器的所有元组读出并写入到排序页中
  //同时记录排序页的page_id到sorted_page_ids_,方便之后的合并逻辑
  Tuple tuple;
  RID rid;

  std::vector<SortEntry> current_run;
  // 计算每个 run 能容纳的最大元组数
  // K * SortPage::DATA_SIZE / tuple_size_
  size_t max_tuples_per_run = (SortPage::DATA_SIZE * K) / tuple_size_;

  while (child_executor_->Next(&tuple, &rid)) {
    SortKey key = GenerateSortKey(tuple, plan_->GetOrderBy(), schema);
    current_run.emplace_back(std::move(key), std::move(tuple));

    if (current_run.size() >= max_tuples_per_run) {
      std::sort(current_run.begin(), current_run.end(), cmp_);

      // 将当前 run 写入磁盘
      std::vector<page_id_t> run_page_ids;
      GenerateRun(current_run, run_page_ids);
      sorted_runs_.emplace_back(run_page_ids, tuple_size_, exec_ctx_->GetBufferPoolManager());

      current_run.clear();
    }
  }

  // 处理剩余的元组
  if (!current_run.empty()) {
    std::sort(current_run.begin(), current_run.end(), cmp_);

    std::vector<page_id_t> run_page_ids;
    GenerateRun(current_run, run_page_ids);
    sorted_runs_.emplace_back(run_page_ids, tuple_size_, exec_ctx_->GetBufferPoolManager());
  }

  // 如果没有数据，直接返回
  if (sorted_runs_.empty()) {
    return;
  }

  // 开始归并排序
  // 只要还有超过1个 run，就继续归并
  while (sorted_runs_.size() > 1) {
    std::vector<MergeSortRun> next_sorted_runs;
    // 每次取两个 run 进行归并
    for (size_t i = 0; i < sorted_runs_.size(); i += 2) {
      if (i + 1 >= sorted_runs_.size()) {
        // 最后一个单独的 run，直接放入下一轮
        next_sorted_runs.push_back(std::move(sorted_runs_[i]));
        continue;
      }

      auto &run1 = sorted_runs_[i];
      auto &run2 = sorted_runs_[i + 1];

      // 归并 run1 和 run2

      auto iter1 = run1.Begin();
      auto iter2 = run2.Begin();

      // key 缓存
      std::optional<SortEntry> entry1;
      std::optional<SortEntry> entry2;

      auto update_entry1 = [&]() {
        if (iter1 != run1.End()) {
          SortKey key = GenerateSortKey(*iter1, plan_->GetOrderBy(), schema);
          entry1.emplace(std::move(key), *iter1);
        } else {
          entry1.reset();
        }
      };

      auto update_entry2 = [&]() {
        if (iter2 != run2.End()) {
          SortKey key = GenerateSortKey(*iter2, plan_->GetOrderBy(), schema);
          entry2.emplace(std::move(key), *iter2);
        } else {
          entry2.reset();
        }
      };

      // 初始加载
      update_entry1();
      update_entry2();

      // 创建新的 run 的 pages
      std::vector<page_id_t> new_run_pages;

      // 准备写 Buffer
      page_id_t current_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      if (current_page_id == INVALID_PAGE_ID) {
        throw Exception("BufferPoolManager is full");
      }
      auto guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_page_id);
      auto *sort_page = reinterpret_cast<SortPage *>(guard.GetDataMut());
      new_run_pages.push_back(current_page_id);
      size_t slot_idx = 0;

      while (entry1.has_value() || entry2.has_value()) {
        Tuple selected_tuple;
        bool use_iter1 = false;

        if (!entry1.has_value()) {
          use_iter1 = false;
        } else if (!entry2.has_value()) {
          use_iter1 = true;
        } else {
          // 比较两个缓存的 SortEntry
          use_iter1 = cmp_(*entry1, *entry2);
        }

        if (use_iter1) {
          selected_tuple = entry1->second;  // 从缓存中取 tuple
          ++iter1;
          update_entry1();  // 更新缓存
        } else {
          selected_tuple = entry2->second;
          ++iter2;
          update_entry2();
        }

        // 写 Tuple 到 new run
        if (!sort_page->WriteTuple(selected_tuple, tuple_size_, slot_idx)) {
          sort_page->SetTupleCount(slot_idx);
          current_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
          if (current_page_id == INVALID_PAGE_ID) {
            throw Exception("BufferPoolManager is full");
          }
          guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_page_id);
          sort_page = reinterpret_cast<SortPage *>(guard.GetDataMut());
          new_run_pages.push_back(current_page_id);
          slot_idx = 0;
          sort_page->WriteTuple(selected_tuple, tuple_size_, slot_idx);
        }
        slot_idx++;
      }
      sort_page->SetTupleCount(slot_idx);
      next_sorted_runs.emplace_back(new_run_pages, tuple_size_, exec_ctx_->GetBufferPoolManager());

      // 清除旧的 pages
      run1.DeletePages();
      run2.DeletePages();
    }
    sorted_runs_ = std::move(next_sorted_runs);
  }

  if (!sorted_runs_.empty()) {
    current_iterator_ = sorted_runs_[0].Begin();
  }
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  //如果没有排序结果（数据），直接返回false
  if (!current_iterator_.has_value()) {
    return false;
  }
  //检查是否到达结尾
  if (*current_iterator_ == sorted_runs_[0].End()) {
    return false;
  }

  // 取当前元组并推进迭代器
  *tuple = **current_iterator_;
  // Rid不太重要
  *rid = tuple->GetRid();

  // 必须使用引用解耦，因为 operator++ 是成员函数
  auto &iter_ref = *current_iterator_;
  ++iter_ref;

  return true;
}

auto MergeSortRun::Iterator::operator++() -> Iterator & {
  if (sort_page_ == nullptr) {
    return *this;
  }

  slot_idx_++;
  if (slot_idx_ >= static_cast<size_t>(sort_page_->GetTupleCount())) {
    // 释放旧页
    if (page_guard_.has_value()) {
      page_guard_->Drop();
    }
    sort_page_ = nullptr;
    page_idx_++;

    slot_idx_ = 0;  // 重置 slot_idx_, 确保与 End() 状态一致

    // 如果还有页，加载下一页
    if (page_idx_ < run_->pages_.size()) {
      auto guard = run_->bpm_->ReadPage(run_->pages_[page_idx_]);
      sort_page_ = guard.As<SortPage>();
      page_guard_ = std::move(guard);

      // 如果读取失败或者页面无效，强制置为 End 状态
      if (sort_page_ == nullptr) {
        page_idx_ = run_->pages_.size();
        if (page_guard_.has_value()) {
          page_guard_->Drop();
        }
      }
    }
  }
  return *this;
}

auto MergeSortRun::Iterator::operator*() -> Tuple {
  BUSTUB_ASSERT(sort_page_ != nullptr, "Dereferencing end iterator or invalid page");
  return sort_page_->ReadTuple(run_->tuple_size_, slot_idx_);
}

auto MergeSortRun::Iterator::operator==(const Iterator &other) const -> bool {
  return page_idx_ == other.page_idx_ && slot_idx_ == other.slot_idx_ && run_ == other.run_;
}

auto MergeSortRun::Iterator::operator!=(const Iterator &other) const -> bool { return !(*this == other); }

auto MergeSortRun::Begin() -> Iterator {
  Iterator iter(this);
  if (!pages_.empty()) {
    iter.page_idx_ = 0;
    iter.slot_idx_ = 0;
    auto guard = bpm_->ReadPage(pages_[0]);
    iter.sort_page_ = guard.As<SortPage>();
    iter.page_guard_ = std::move(guard);

    // 如果读取失败，直接作为 End 处理
    if (iter.sort_page_ == nullptr) {
      iter.page_guard_ = std::nullopt;
      iter.page_idx_ = pages_.size();
      return iter;
    }

    // 处理空 run 的情况
    if (iter.sort_page_->GetTupleCount() == 0) {
      // 如果第一页就是空的，直接跳到 End
      iter.page_guard_ = std::nullopt;
      iter.sort_page_ = nullptr;
      iter.page_idx_ = pages_.size();
    }
  } else {
    // 空 run，直接是 End 状态
    iter.page_idx_ = 0;
    iter.slot_idx_ = 0;
    iter.sort_page_ = nullptr;
  }
  return iter;
}

auto MergeSortRun::End() -> Iterator {
  Iterator iter(this);
  iter.page_idx_ = pages_.size();
  iter.slot_idx_ = 0;
  iter.sort_page_ = nullptr;
  return iter;
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
