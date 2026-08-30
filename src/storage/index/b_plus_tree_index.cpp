//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree_index.cpp
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree_index.h"

#include <algorithm>

#include "common/exception.h"

namespace bustub {
/*
 * Constructor
 */
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_INDEX_TYPE::BPlusTreeIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager)
    : Index(std::move(metadata)),
      comparator_(GetMetadata()->GetKeySchema()),
      is_primary_key_(GetMetadata()->IsPrimaryKey()),
      non_unique_entries_(StlComparatorWrapper<KeyType, KeyComparator>(comparator_)) {
  page_id_t header_page_id = buffer_pool_manager->NewPage();
  container_ = std::make_shared<BPlusTree<KeyType, ValueType, KeyComparator>>(GetMetadata()->GetName(), header_page_id,
                                                                              buffer_pool_manager, comparator_);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::InsertEntry(const Tuple &key, RID rid, Transaction *transaction) -> bool {
  // construct insert index key
  KeyType index_key;
  index_key.SetFromKey(key);
  if (is_primary_key_) {
    return container_->Insert(index_key, rid);
  }
  std::scoped_lock lock(non_unique_latch_);
  auto &rids = non_unique_entries_[index_key];
  if (std::find(rids.begin(), rids.end(), rid) != rids.end()) {
    return false;
  }
  if (rids.empty() && !container_->Insert(index_key, rid)) {
    non_unique_entries_.erase(index_key);
    return false;
  }
  rids.push_back(rid);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) {
  // construct delete index key
  KeyType index_key;
  index_key.SetFromKey(key);
  if (is_primary_key_) {
    container_->Remove(index_key);
    return;
  }
  std::scoped_lock lock(non_unique_latch_);
  const auto iterator = non_unique_entries_.find(index_key);
  if (iterator == non_unique_entries_.end()) {
    return;
  }
  auto &rids = iterator->second;
  const auto rid_iterator = std::find(rids.begin(), rids.end(), rid);
  if (rid_iterator == rids.end()) {
    return;
  }
  const bool removed_representative = rid_iterator == rids.begin();
  if (removed_representative) {
    container_->Remove(index_key);
  }
  rids.erase(rid_iterator);
  if (rids.empty()) {
    non_unique_entries_.erase(iterator);
  } else if (removed_representative && !container_->Insert(index_key, rids.front())) {
    throw Exception("failed to replace non-unique B+Tree representative");
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) {
  // construct scan index key
  KeyType index_key;
  index_key.SetFromKey(key);
  if (is_primary_key_) {
    container_->GetValue(index_key, result);
    return;
  }
  std::scoped_lock lock(non_unique_latch_);
  const auto iterator = non_unique_entries_.find(index_key);
  *result = iterator == non_unique_entries_.end() ? std::vector<RID>{} : iterator->second;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::GetBeginIterator() -> INDEXITERATOR_TYPE { return container_->Begin(); }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::GetBeginIterator(const KeyType &key) -> INDEXITERATOR_TYPE { return container_->Begin(key); }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::GetEndIterator() -> INDEXITERATOR_TYPE { return container_->End(); }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::GetAllEntriesSnapshot() -> std::vector<std::pair<KeyType, ValueType>> {
  std::vector<std::pair<KeyType, ValueType>> entries;
  if (!is_primary_key_) {
    std::scoped_lock lock(non_unique_latch_);
    for (const auto &[key, rids] : non_unique_entries_) {
      for (const auto &rid : rids) {
        entries.emplace_back(key, rid);
      }
    }
    return entries;
  }

  auto iterator = container_->Begin();
  auto end = container_->End();
  while (iterator != end) {
    const auto &[key, rid] = *iterator;
    entries.emplace_back(key, rid);
    ++iterator;
  }
  return entries;
}

template class BPlusTreeIndex<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeIndex<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeIndex<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeIndex<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
