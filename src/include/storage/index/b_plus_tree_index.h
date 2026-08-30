//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree_index.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <memory>
#include <mutex>  // NOLINT(build/c++11)
#include <string>
#include <utility>
#include <vector>

#include "container/hash/hash_function.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index.h"
#include "storage/index/stl_comparator_wrapper.h"

namespace bustub {

#define BPLUSTREE_INDEX_TYPE BPlusTreeIndex<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeIndex : public Index {
 public:
  BPlusTreeIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager);

  auto InsertEntry(const Tuple &key, RID rid, Transaction *transaction) -> bool override;

  void DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) override;

  void ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) override;

  auto GetBeginIterator() -> INDEXITERATOR_TYPE;

  auto GetBeginIterator(const KeyType &key) -> INDEXITERATOR_TYPE;

  auto GetEndIterator() -> INDEXITERATOR_TYPE;

  /** Return a key-ordered snapshot containing every RID, including non-unique secondary-index buckets. */
  auto GetAllEntriesSnapshot() -> std::vector<std::pair<KeyType, ValueType>>;

 protected:
  // comparator for key
  KeyComparator comparator_;
  // container
  std::shared_ptr<BPlusTree<KeyType, ValueType, KeyComparator>> container_;
  // BusTub's B+Tree stores one RID per key. Ordinary secondary indexes add an exact-key RID bucket while the tree
  // retains one representative for ordered traversal; primary indexes continue to reject duplicates.
  bool is_primary_key_;
  std::mutex non_unique_latch_;
  std::map<KeyType, std::vector<RID>, StlComparatorWrapper<KeyType, KeyComparator>> non_unique_entries_;
};

/** We only support index table with one integer key for now in BusTub. Hardcode everything here. */

constexpr static const auto TWO_INTEGER_SIZE_B_TREE = 8;
using IntegerKeyType_BTree = GenericKey<TWO_INTEGER_SIZE_B_TREE>;
using IntegerValueType_BTree = RID;
using IntegerComparatorType_BTree = GenericComparator<TWO_INTEGER_SIZE_B_TREE>;
using BPlusTreeIndexForTwoIntegerColumn =
    BPlusTreeIndex<IntegerKeyType_BTree, IntegerValueType_BTree, IntegerComparatorType_BTree>;
using BPlusTreeIndexIteratorForTwoIntegerColumn =
    IndexIterator<IntegerKeyType_BTree, IntegerValueType_BTree, IntegerComparatorType_BTree>;
using IntegerHashFunctionType = HashFunction<IntegerKeyType_BTree>;

}  // namespace bustub
