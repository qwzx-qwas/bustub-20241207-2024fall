#include <algorithm>
#include <vector>

#include "common/exception.h"

#include "storage/index/extendible_hash_table_index.h"

namespace bustub {
/*
 * Constructor
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_INDEX_TYPE::ExtendibleHashTableIndex(std::unique_ptr<IndexMetadata> &&metadata,
                                                BufferPoolManager *buffer_pool_manager,
                                                const HashFunction<KeyType> &hash_fn)
    : Index(std::move(metadata)),
      comparator_(GetMetadata()->GetKeySchema()),
      container_(GetMetadata()->GetName(), buffer_pool_manager, comparator_, hash_fn),
      is_primary_key_(GetMetadata()->IsPrimaryKey()),
      non_unique_entries_(StlComparatorWrapper<KeyType, KeyComparator>(comparator_)) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_INDEX_TYPE::InsertEntry(const Tuple &key, RID rid, Transaction *transaction) -> bool {
  // construct insert index key
  KeyType index_key;
  index_key.SetFromKey(key);

  if (is_primary_key_) {
    return container_.Insert(index_key, rid, transaction);
  }
  std::scoped_lock lock(non_unique_latch_);
  auto &rids = non_unique_entries_[index_key];
  if (std::find(rids.begin(), rids.end(), rid) != rids.end()) {
    return false;
  }
  if (rids.empty() && !container_.Insert(index_key, rid, transaction)) {
    non_unique_entries_.erase(index_key);
    return false;
  }
  rids.push_back(rid);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_INDEX_TYPE::DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) {
  // construct delete index key
  KeyType index_key;
  index_key.SetFromKey(key);

  if (is_primary_key_) {
    container_.Remove(index_key, transaction);
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
    container_.Remove(index_key, transaction);
  }
  rids.erase(rid_iterator);
  if (rids.empty()) {
    non_unique_entries_.erase(iterator);
  } else if (removed_representative && !container_.Insert(index_key, rids.front(), transaction)) {
    throw Exception("failed to replace non-unique hash-index representative");
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_INDEX_TYPE::ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) {
  // construct scan index key
  KeyType index_key;
  index_key.SetFromKey(key);

  if (is_primary_key_) {
    container_.GetValue(index_key, result, transaction);
    return;
  }
  std::scoped_lock lock(non_unique_latch_);
  const auto iterator = non_unique_entries_.find(index_key);
  *result = iterator == non_unique_entries_.end() ? std::vector<RID>{} : iterator->second;
}
template class ExtendibleHashTableIndex<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTableIndex<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTableIndex<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTableIndex<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTableIndex<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
