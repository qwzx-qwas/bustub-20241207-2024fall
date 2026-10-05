//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// table_iterator.cpp
//
// Identification: src/storage/table/table_iterator.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <optional>

#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "storage/table/table_heap.h"

namespace bustub {

TableIterator::TableIterator(TableHeap *table_heap, bool eager)
    : table_heap_(table_heap), rid_(table_heap->first_page_id_, 0), stop_at_rid_(INVALID_PAGE_ID, 0) {
  std::unique_lock<std::mutex> lock(table_heap_->latch_);
  if (!eager) {
    auto tail = table_heap_->bpm_->ReadPage(table_heap_->last_page_id_);
    stop_at_rid_ = RID(table_heap_->last_page_id_, tail.As<TablePage>()->GetNumTuples());
  }
  // Initialization may read multiple empty pages; publish the scan only after it succeeds.
  SkipVacant();
  ++table_heap_->active_iterators_;
}

TableIterator::TableIterator(TableIterator &&other) noexcept
    : table_heap_(std::exchange(other.table_heap_, nullptr)), rid_(other.rid_), stop_at_rid_(other.stop_at_rid_) {}

TableIterator::~TableIterator() {
  if (table_heap_ != nullptr) {
    std::unique_lock<std::mutex> lock(table_heap_->latch_);
    --table_heap_->active_iterators_;
  }
}

void TableIterator::SkipVacant() {
  while (rid_.GetPageId() != INVALID_PAGE_ID) {
    auto guard = table_heap_->bpm_->ReadPage(rid_.GetPageId());
    const auto *page = guard.As<TablePage>();
    auto slot = rid_.GetSlotNum();
    while (slot < page->GetNumTuples()) {
      if (RID(rid_.GetPageId(), slot) == stop_at_rid_) {
        rid_ = RID(INVALID_PAGE_ID, 0);
        return;
      }
      if (page->IsOccupied(slot)) {
        rid_ = RID(rid_.GetPageId(), slot);
        return;
      }
      ++slot;
    }
    if (rid_.GetPageId() == stop_at_rid_.GetPageId()) {
      rid_ = RID(INVALID_PAGE_ID, 0);
      return;
    }
    rid_ = RID(page->GetNextPageId(), 0);
  }
}

auto TableIterator::GetTuple() -> std::pair<TupleMeta, Tuple> { return table_heap_->GetTuple(rid_); }
auto TableIterator::GetRID() -> RID { return rid_; }
auto TableIterator::IsEnd() -> bool { return rid_.GetPageId() == INVALID_PAGE_ID; }

auto TableIterator::operator++() -> TableIterator & {
  rid_ = RID(rid_.GetPageId(), rid_.GetSlotNum() + 1);
  SkipVacant();
  return *this;
}

}  // namespace bustub
