//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <iostream>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Page to hold the intermediate data for external merge sort.
 *
 * Only fixed-length data will be supported in Fall 2024.
 */
// 用于专门储存外部归并排序的中间数据的页面
// 由于只测试固定长度元组
// 完全可以把page分为两部分，前面是元数据，后面是实际数据
// 元数据用来记录有多少元组
// 实际数据用来存储元组（连续的内存）
class SortPage {
 public:
  /**
   * TODO: Define and implement the methods for reading data from and writing data to the sort
   * page. Feel free to add other helper methods.
   */

  // 用32位整数记录当前页面中元组的数量
  static constexpr size_t HEADER_SIZE = sizeof(int32_t);
  // 剩下的页面用来存储实际数据
  static constexpr size_t DATA_SIZE = BUSTUB_PAGE_SIZE - HEADER_SIZE;

  // 获取当前页面中元组的数量
  auto GetTupleCount() const -> int32_t { return *reinterpret_cast<const int32_t *>(this); }

  // 设置当前页面中元组的数量
  auto SetTupleCount(int32_t count) -> void { *reinterpret_cast<int32_t *>(this) = count; }

  // 写入页面
  auto WriteTuple(const Tuple &tuple, size_t tuple_size, size_t slot_idx) -> bool {
    if (tuple_size < sizeof(uint32_t) || tuple.GetLength() > tuple_size - sizeof(uint32_t)) {
      throw ExecutionException("serialized sort tuple exceeds its schema-derived slot");
    }
    size_t offset = slot_idx * tuple_size;
    // 检查是否有足够的空间写入元组
    if (offset + tuple_size > DATA_SIZE) {
      return false;
    }
    // 计算写入位置
    auto *data_ptr = reinterpret_cast<char *>(this) + HEADER_SIZE + offset;
    // 将元组数据写入页面
    tuple.SerializeTo(data_ptr);
    return true;
  }

  // 读取页面
  auto ReadTuple(size_t tuple_size, size_t slot_idx) const -> Tuple {
    size_t offset = slot_idx * tuple_size;
    // 计算读取位置
    auto *data_ptr = reinterpret_cast<const char *>(this) + HEADER_SIZE + offset;
    // 从页面中反序列化出元组
    Tuple t;
    t.DeserializeFrom(data_ptr);
    return t;
  }

 private:
  /**
   * TODO: Define the private members. You may want to have some necessary metadata for
   * the sort page before the start of the actual data.
   */
};

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 *表示一组有序的磁盘页面数据（run），这些页面存储了外部归并排序过程中排序后的元组。
 *同时提供了在这些数据上进行迭代的功能。
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, size_t tuple_size, BufferPoolManager *bpm)
      : pages_(std::move(pages)), tuple_size_(tuple_size), bpm_(bpm) {}

  auto GetPageCount() -> size_t { return pages_.size(); }

  // 封装DeletePage逻辑，删除该run所包含的所有页面
  void DeletePages() {
    for (auto page_id : pages_) {
      bpm_->DeletePage(page_id);
    }
    pages_.clear();
  }

  /** Iterator for iterating on the sorted tuples in one run. */
  // 因为run的数据分散在多个磁盘页面中，用Iterator来实现对这些数据的迭代访问
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;

    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     *
     * TODO: Implement this method.
     */
    auto operator++() -> Iterator &;

    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     *
     * TODO: Implement this method.
     */
    auto operator*() -> Tuple;

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     *
     * TODO: Implement this method.
     */
    auto operator==(const Iterator &other) const -> bool;

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     *
     * TODO: Implement this method.
     */
    auto operator!=(const Iterator &other) const -> bool;

   private:
    explicit Iterator(const MergeSortRun *run) : run_(run) {}

    /** The sorted run that the iterator is iterating on. */
    [[maybe_unused]] const MergeSortRun *run_;

    /**
     * TODO: Add your own private members here. You may want something to record your current
     * position in the sorted run. Also feel free to add additional constructors to initialize
     * your private members.
     */
    // 记录是第几页
    size_t page_idx_{0};
    // 记录是当前页的第几个元组
    size_t slot_idx_{0};
    std::optional<ReadPageGuard> page_guard_;
    const SortPage *sort_page_{nullptr};
  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   *
   * TODO: Implement this method.
   */
  auto Begin() -> Iterator;

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   *
   * TODO: Implement this method.
   */
  auto End() -> Iterator;

 private:
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  size_t tuple_size_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  [[maybe_unused]] BufferPoolManager *bpm_;
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Fall 2024, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  ~ExternalMergeSortExecutor() override;

  /** Initialize the external merge sort */
  void Init() override;

  /**
   * Yield the next tuple from the external merge sort.
   * @param[out] tuple The next tuple produced by the external merge sort.
   * @param[out] rid The next tuple RID produced by the external merge sort.
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;

  /** TODO: You will want to add your own private members here. */
  std::unique_ptr<AbstractExecutor> child_executor_;

  // tuple长度
  size_t tuple_size_;

  // 存储当前有序的runs
  std::vector<MergeSortRun> sorted_runs_;
  std::optional<MergeSortRun::Iterator> current_iterator_;

  void GenerateRun(const std::vector<SortEntry> &run, std::vector<page_id_t> &pages);

  auto MergeRuns() -> std::optional<MergeSortRun>;
};

}  // namespace bustub
