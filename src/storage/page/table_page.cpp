//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// table_page.cpp
//
// Identification: src/storage/page/table_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/table_page.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <optional>
#include <tuple>
#include "common/config.h"
#include "common/exception.h"
#include "storage/table/tuple.h"

namespace bustub {

void TablePage::Init() {
  next_page_id_ = INVALID_PAGE_ID;
  num_tuples_ = 0;
  num_deleted_tuples_ = 0;
}

auto TablePage::IsOccupied(uint32_t slot) const -> bool {
  return slot < num_tuples_ && std::get<0>(TupleInfoAt(slot)) != 0;
}

void TablePage::RefreshDeletedCount() {
  num_deleted_tuples_ = 0;
  for (uint16_t i = 0; i < num_tuples_; ++i) {
    if (IsOccupied(i) && std::get<2>(TupleInfoAt(i)).is_deleted_) {
      ++num_deleted_tuples_;
    }
  }
}

auto TablePage::BodyStart() const -> uint16_t {
  uint16_t begin = BUSTUB_PAGE_SIZE;
  for (uint32_t i = 0; i < num_tuples_; ++i) {
    if (IsOccupied(i)) {
      begin = std::min(begin, std::get<0>(TupleInfoAt(i)));
    }
  }
  return begin;
}

auto TablePage::GetFreeSpace() const -> uint16_t {
  bool vacant = false;
  for (uint32_t i = 0; i < num_tuples_; ++i) {
    vacant |= !IsOccupied(i);
  }
  const auto directory_end = TABLE_PAGE_HEADER_SIZE + TUPLE_INFO_SIZE * (num_tuples_ + (vacant ? 0 : 1));
  const auto begin = BodyStart();
  return begin >= directory_end ? begin - directory_end : 0;
}

auto TablePage::InsertTuple(const TupleMeta &meta, const Tuple &tuple, bool reuse_slots) -> std::optional<uint16_t> {
  uint16_t slot = num_tuples_;
  if (reuse_slots) {
    for (uint16_t i = 0; i < num_tuples_; ++i) {
      if (!IsOccupied(i)) {
        slot = i;
        break;
      }
    }
  }
  const auto begin = BodyStart();
  const auto end = TABLE_PAGE_HEADER_SIZE + TUPLE_INFO_SIZE * (num_tuples_ + (slot == num_tuples_ ? 1 : 0));
  if (end > begin || tuple.GetLength() > begin - end) {
    return std::nullopt;
  }
  const auto offset = begin - tuple.GetLength();
  TupleInfoAt(slot) = std::make_tuple(offset, tuple.GetLength(), meta);
  if (slot == num_tuples_) {
    ++num_tuples_;
  }
  RefreshDeletedCount();
  memcpy(PageData() + offset, tuple.data_.data(), tuple.GetLength());
  return slot;
}

void TablePage::ReclaimTuples(const std::vector<uint16_t> &slots) {
  // Build the replacement before changing the guarded page. One page bounds all work and temporary memory.
  alignas(8) std::array<char, BUSTUB_PAGE_SIZE> image;
  memcpy(image.data(), PageData(), image.size());
  auto *replacement = reinterpret_cast<TablePage *>(image.data());
  for (const auto slot : slots) {
    if (!replacement->IsOccupied(slot) || !std::get<2>(replacement->TupleInfoAt(slot)).is_deleted_) {
      throw Exception("reclamation requires distinct, occupied deleted slots");
    }
    replacement->TupleInfoAt(slot) = std::make_tuple(0, 0, TupleMeta{0, true});
  }
  replacement->num_deleted_tuples_ = 0;
  uint16_t end = BUSTUB_PAGE_SIZE;
  for (uint16_t i = 0; i < num_tuples_; ++i) {
    if (!replacement->IsOccupied(i)) {
      continue;
    }
    const auto &[offset, size, meta] = TupleInfoAt(i);
    replacement->num_deleted_tuples_ += meta.is_deleted_ ? 1 : 0;
    end -= size;
    memcpy(image.data() + end, PageData() + offset, size);
    replacement->TupleInfoAt(i) = std::make_tuple(end, size, meta);
  }
  memcpy(PageData(), image.data(), image.size());
}

void TablePage::UpdateTupleMeta(const TupleMeta &meta, const RID &rid) {
  auto tuple_id = rid.GetSlotNum();
  if (!IsOccupied(tuple_id)) {
    throw bustub::Exception("Tuple ID is out of range or reclaimed");
  }
  auto &[offset, size, old_meta] = TupleInfoAt(tuple_id);
  const bool changed = old_meta.is_deleted_ != meta.is_deleted_;
  TupleInfoAt(tuple_id) = std::make_tuple(offset, size, meta);
  if (changed) {
    RefreshDeletedCount();
  }
}

auto TablePage::GetTuple(const RID &rid) const -> std::pair<TupleMeta, Tuple> {
  auto tuple_id = rid.GetSlotNum();
  if (!IsOccupied(tuple_id)) {
    throw bustub::Exception("Tuple ID is out of range or reclaimed");
  }
  const auto &[offset, size, meta] = TupleInfoAt(tuple_id);
  Tuple tuple;
  tuple.data_.resize(size);
  memmove(tuple.data_.data(), PageData() + offset, size);
  tuple.rid_ = rid;
  return std::make_pair(meta, std::move(tuple));
}

auto TablePage::GetTupleMeta(const RID &rid) const -> TupleMeta {
  auto tuple_id = rid.GetSlotNum();
  if (!IsOccupied(tuple_id)) {
    throw bustub::Exception("Tuple ID is out of range or reclaimed");
  }
  const auto &[_1, _2, meta] = TupleInfoAt(tuple_id);
  return meta;
}

void TablePage::UpdateTupleInPlaceUnsafe(const TupleMeta &meta, const Tuple &tuple, RID rid) {
  auto tuple_id = rid.GetSlotNum();
  if (!IsOccupied(tuple_id)) {
    throw bustub::Exception("Tuple ID is out of range or reclaimed");
  }
  auto &[offset, size, old_meta] = TupleInfoAt(tuple_id);
  if (size != tuple.GetLength()) {
    throw bustub::Exception("Tuple size mismatch");
  }
  const bool changed = old_meta.is_deleted_ != meta.is_deleted_;
  TupleInfoAt(tuple_id) = std::make_tuple(offset, size, meta);
  if (changed) {
    RefreshDeletedCount();
  }
  memcpy(PageData() + offset, tuple.data_.data(), tuple.GetLength());
}

}  // namespace bustub
