//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <memory>
#include <utility>
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(ReadPageGuard &&leaf_guard, const LeafPage *current_leaf_page, page_id_t page_id, int index,
                BufferPoolManager *bpm);
  ~IndexIterator();  // NOLINT

  IndexIterator(IndexIterator &&) noexcept = default;
  auto operator=(IndexIterator &&) noexcept -> IndexIterator & = default;

  IndexIterator(const IndexIterator &) = delete;
  auto operator=(const IndexIterator &) -> IndexIterator & = delete;

  auto IsEnd() const -> bool;

  /**
   * Return an owning copy of the current entry.
   *
   * LeafPage::KeyAt and ValueAt return by value. Returning references to those
   * temporaries leaves callers with dangling RIDs before they can consume the
   * iterator result.
   */
  auto operator*() -> std::pair<KeyType, ValueType>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool;

  auto operator!=(const IndexIterator &itr) const -> bool;

 private:
  // add your own private member variables here
  std::unique_ptr<ReadPageGuard> leaf_guard_;
  const LeafPage *current_leaf_page_;
  page_id_t page_id_;
  int index_;
  BufferPoolManager *bpm_;
};

}  // namespace bustub
