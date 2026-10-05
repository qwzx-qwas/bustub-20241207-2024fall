//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// table_heap.cpp
//
// Identification: src/storage/table/table_heap.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <array>
#include <cassert>
#include <map>
#include <mutex>  // NOLINT
#include <unordered_set>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "fmt/format.h"
#include "storage/page/page_guard.h"
#include "storage/page/table_page.h"
#include "storage/table/table_heap.h"

namespace bustub {

TableHeap::TableHeap(BufferPoolManager *bpm) : bpm_(bpm) {
  // Initialize the first table page.
  first_page_id_ = bpm->NewPage();
  last_page_id_ = first_page_id_;

  auto guard = bpm->WritePage(first_page_id_);
  auto first_page = guard.AsMut<TablePage>();
  BUSTUB_ASSERT(first_page != nullptr,
                "Couldn't create a page for the table heap. Have you completed the buffer pool manager project?");

  first_page->Init();
  free_pages_.emplace(first_page->GetFreeSpace(), first_page_id_);
}

TableHeap::TableHeap(BufferPoolManager *bpm, page_id_t first_page_id, page_id_t last_page_id)
    : bpm_(bpm), first_page_id_(first_page_id), last_page_id_(last_page_id) {}

auto TableHeap::Open(BufferPoolManager *bpm, page_id_t first_page_id) -> std::unique_ptr<TableHeap> {
  if (bpm == nullptr) {
    throw Exception("cannot open a table heap without a buffer pool manager");
  }
  if (first_page_id == INVALID_PAGE_ID || first_page_id < 0) {
    throw Exception("cannot open a table heap with an invalid first page id");
  }

  std::unordered_set<page_id_t> visited;
  auto heap = std::unique_ptr<TableHeap>(new TableHeap(bpm, first_page_id, INVALID_PAGE_ID));
  auto page_id = first_page_id;
  while (true) {
    if (!visited.emplace(page_id).second) {
      throw Exception("corrupt table heap: page chain contains a cycle");
    }

    auto guard = bpm->ReadPage(page_id);
    auto page = guard.As<TablePage>();
    if (page == nullptr) {
      throw Exception("corrupt table heap: table page cannot be read");
    }
    heap->free_pages_.emplace(page->GetFreeSpace(), page_id);
    if (page_id != first_page_id && page->IsEmpty()) {
      heap->empty_pages_.insert(page_id);
    }
    const auto next_page_id = page->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      heap->last_page_id_ = page_id;
      return heap;
    }
    if (next_page_id < 0) {
      throw Exception("corrupt table heap: invalid next page id");
    }
    heap->predecessors_.emplace(next_page_id, page_id);
    page_id = next_page_id;
  }
}

TableHeap::TableHeap(bool create_table_heap) : bpm_(nullptr) {}

auto TableHeap::InsertTuple(const TupleMeta &meta, const Tuple &tuple, LockManager *lock_mgr, Transaction *txn,
                            table_oid_t oid) -> std::optional<RID> {
  // Reject impossible rows before linking a page or changing the space summary.
  if (tuple.GetLength() > TABLE_PAGE_MAX_TUPLE_SIZE) {
    return std::nullopt;
  }
  std::unique_lock<std::mutex> guard(latch_);
  const bool reuse = active_iterators_ == 0;
  std::optional<RID> result;
  auto insert = [&](page_id_t id, std::optional<uint16_t> hint) {
    auto page_guard = bpm_->WritePage(id);
    auto *page = page_guard.AsMut<TablePage>();
    const auto old_space = page->GetFreeSpace();
    auto slot = page->InsertTuple(meta, tuple, reuse);
    // Update the existing hint even if this candidate was stale.
    free_pages_.erase({hint.value_or(old_space), id});
    free_pages_.emplace(page->GetFreeSpace(), id);
    if (slot) {
      empty_pages_.erase(id);
      result = RID(id, *slot);
    }
  };
  if (reuse) {
    for (size_t attempt = 0; attempt < 2 && !result; ++attempt) {
      const auto candidate = free_pages_.lower_bound({tuple.GetLength(), 0});
      if (candidate == free_pages_.end()) {
        break;
      }
      const auto [space, id] = *candidate;
      // Retain the hint if acquiring the page fails; replace it only after the page operation.
      insert(id, space);
    }
  }
  if (!result) {
    insert(last_page_id_, std::nullopt);
  }
  if (!result) {
    const auto next_id = bpm_->NewPage();
    predecessors_.emplace(next_id, last_page_id_);
    {
      auto next_guard = bpm_->WritePage(next_id);
      next_guard.AsMut<TablePage>()->Init();
    }
    {
      auto tail_guard = bpm_->WritePage(last_page_id_);
      tail_guard.AsMut<TablePage>()->SetNextPageId(next_id);
    }
    last_page_id_ = next_id;
    insert(next_id, std::nullopt);
  }
  guard.unlock();
#ifndef DISABLE_LOCK_MANAGER
  if (lock_mgr != nullptr) {
    BUSTUB_ENSURE(lock_mgr->LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, *result),
                  "failed to lock when inserting new tuple");
  }
#endif
  return result;
}

void TableHeap::ReclaimTuples(const std::vector<RID> &rids) {
  std::map<page_id_t, std::vector<uint16_t>> pages;
  for (const auto &rid : rids) {
    if (rid.GetSlotNum() > UINT16_MAX) {
      throw Exception("invalid reclaimed slot");
    }
    pages[rid.GetPageId()].push_back(rid.GetSlotNum());
  }
  std::unique_lock<std::mutex> lock(latch_);
  if (active_iterators_ != 0) {
    throw Exception("cannot reclaim records while a table scan owns RIDs");
  }
  for (const auto &[id, slots] : pages) {
    auto page_guard = bpm_->WritePage(id);
    auto *page = page_guard.AsMut<TablePage>();
    const auto old_space = page->GetFreeSpace();
    page->ReclaimTuples(slots);
    free_pages_.erase({old_space, id});
    free_pages_.emplace(page->GetFreeSpace(), id);
    if (id != first_page_id_ && page->IsEmpty()) {
      empty_pages_.insert(id);
    }
  }
}

void TableHeap::ReclaimEmptyPages() {
  if (!bpm_->SupportsPageRetirement()) {
    return;  // Legacy file storage keeps page-local reuse without false durability.
  }
  std::unique_lock<std::mutex> lock(latch_);
  if (active_iterators_ != 0) {
    return;
  }
  bpm_->ReclaimRetiredPages();
  const auto attempts = std::min<size_t>(4, empty_pages_.size());
  for (size_t i = 0; i < attempts; ++i) {
    auto candidate = empty_pages_.lower_bound(empty_cursor_);
    if (candidate == empty_pages_.end()) {
      candidate = empty_pages_.begin();
    }
    const auto id = *candidate;
    empty_cursor_ = id == INT32_MAX ? 0 : id + 1;
    auto victim = bpm_->ReadPage(id);
    const auto *page = victim.As<TablePage>();
    if (!page->IsEmpty()) {
      empty_pages_.erase(candidate);
      continue;
    }
    const auto previous = predecessors_.at(id);
    const auto next = page->GetNextPageId();
    const auto free = page->GetFreeSpace();
    auto link = bpm_->WritePage(previous);
    alignas(TablePage) std::array<char, BUSTUB_PAGE_SIZE> replacement;
    std::memcpy(replacement.data(), link.GetData(), replacement.size());
    reinterpret_cast<TablePage *>(replacement.data())->SetNextPageId(next);
    if (!bpm_->RetirePage(victim, link, replacement)) {
      continue;
    }
    if (next == INVALID_PAGE_ID) {
      last_page_id_ = previous;
    } else {
      predecessors_.at(next) = previous;
    }
    free_pages_.erase({free, id});
    predecessors_.erase(id);
    empty_pages_.erase(candidate);
  }
  bpm_->ReclaimRetiredPages();
}

void TableHeap::UpdateTupleMeta(const TupleMeta &meta, RID rid) {
  auto page_guard = bpm_->WritePage(rid.GetPageId());
  auto page = page_guard.AsMut<TablePage>();
  page->UpdateTupleMeta(meta, rid);
}

auto TableHeap::GetTuple(RID rid) -> std::pair<TupleMeta, Tuple> {
  auto page_guard = bpm_->ReadPage(rid.GetPageId());
  auto page = page_guard.As<TablePage>();
  auto [meta, tuple] = page->GetTuple(rid);
  tuple.rid_ = rid;
  return std::make_pair(meta, std::move(tuple));
}

auto TableHeap::GetTupleMeta(RID rid) -> TupleMeta {
  auto page_guard = bpm_->ReadPage(rid.GetPageId());
  auto page = page_guard.As<TablePage>();
  return page->GetTupleMeta(rid);
}

auto TableHeap::MakeIterator() -> TableIterator { return TableIterator(this, false); }

auto TableHeap::MakeEagerIterator() -> TableIterator { return TableIterator(this, true); }
/*UpdateTupleInPlace：这个函数是用来覆盖（Overwrite）一个已经在页面上占了位置的 Tuple 的。在 Insert 时，
由于你是调用 TableHeap::InsertTuple 来寻找空闲空间并插入新数据，
InsertTuple 内部已经帮你完成了写入操作。你不需要“原地更新”一个不存在的东西。*/
auto TableHeap::UpdateTupleInPlace(const TupleMeta &meta, const Tuple &tuple, RID rid,
                                   std::function<bool(const TupleMeta &meta, const Tuple &table, RID rid)> &&check)
    -> bool {
  auto page_guard = bpm_->WritePage(rid.GetPageId());
  auto page = page_guard.AsMut<TablePage>();
  auto [old_meta, old_tup] = page->GetTuple(rid);
  if (check == nullptr || check(old_meta, old_tup, rid)) {
    page->UpdateTupleInPlaceUnsafe(meta, tuple, rid);
    return true;
  }
  return false;
}

auto TableHeap::AcquireTablePageReadLock(RID rid) -> ReadPageGuard { return bpm_->ReadPage(rid.GetPageId()); }

auto TableHeap::AcquireTablePageWriteLock(RID rid) -> WritePageGuard { return bpm_->WritePage(rid.GetPageId()); }

void TableHeap::UpdateTupleInPlaceWithLockAcquired(const TupleMeta &meta, const Tuple &tuple, RID rid,
                                                   TablePage *page) {
  page->UpdateTupleInPlaceUnsafe(meta, tuple, rid);
}

auto TableHeap::GetTupleWithLockAcquired(RID rid, const TablePage *page) -> std::pair<TupleMeta, Tuple> {
  auto [meta, tuple] = page->GetTuple(rid);
  tuple.rid_ = rid;
  return std::make_pair(meta, std::move(tuple));
}

auto TableHeap::GetTupleMetaWithLockAcquired(RID rid, const TablePage *page) -> TupleMeta {
  return page->GetTupleMeta(rid);
}

}  // namespace bustub
