#pragma once

#include <algorithm>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "container/hash/hash_function.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index.h"
#include "storage/index/stl_comparator_wrapper.h"
#include "storage/index/stl_equal_wrapper.h"
#include "storage/index/stl_hasher_wrapper.h"

namespace bustub {

template <typename KT, typename VT, typename Cmp>
class STLUnorderedIndex : public Index {
 public:
  STLUnorderedIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
                    const HashFunction<KT> &hash_fn)
      : Index(std::move(metadata)),
        comparator_(StlComparatorWrapper<KT, Cmp>(Cmp(metadata_->GetKeySchema()))),
        hash_fn_(StlHasherWrapper<KT>(hash_fn)),
        eq_(StlEqualWrapper<KT, Cmp>(Cmp(metadata_->GetKeySchema()))),
        data_(0, hash_fn_, eq_),
        is_primary_key_(metadata_->IsPrimaryKey()),
        non_unique_entries_(0, hash_fn_, eq_) {}

  auto InsertEntry(const Tuple &key, VT rid, Transaction *transaction) -> bool override {
    KT index_key;
    index_key.SetFromKey(key);
    std::scoped_lock<std::mutex> lck(lock_);
    if (is_primary_key_) {
      if (data_.find(index_key) != data_.end()) {
        return false;
      }
      data_.emplace(index_key, rid);
      return true;
    }
    auto &rids = non_unique_entries_[index_key];
    if (std::find(rids.begin(), rids.end(), rid) != rids.end()) {
      return false;
    }
    if (rids.empty()) {
      data_.emplace(index_key, rid);
    }
    rids.push_back(rid);
    return true;
  }

  void DeleteEntry(const Tuple &key, VT rid, Transaction *transaction) override {
    KT index_key;
    index_key.SetFromKey(key);
    std::scoped_lock<std::mutex> lck(lock_);
    if (is_primary_key_) {
      data_.erase(index_key);
      return;
    }
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
    rids.erase(rid_iterator);
    if (rids.empty()) {
      non_unique_entries_.erase(iterator);
      data_.erase(index_key);
    } else if (removed_representative) {
      data_[index_key] = rids.front();
    }
  }

  void ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) override {
    KT index_key;
    index_key.SetFromKey(key);
    std::scoped_lock<std::mutex> lck(lock_);
    if (is_primary_key_) {
      if (auto it = data_.find(index_key); it != data_.end()) {
        *result = std::vector{it->second};
        return;
      }
      *result = {};
      return;
    }
    const auto iterator = non_unique_entries_.find(index_key);
    *result = iterator == non_unique_entries_.end() ? std::vector<RID>{} : iterator->second;
  }

 protected:
  std::mutex lock_;
  StlComparatorWrapper<KT, Cmp> comparator_;
  StlHasherWrapper<KT> hash_fn_;
  StlEqualWrapper<KT, Cmp> eq_;
  std::unordered_map<KT, VT, StlHasherWrapper<KT>, StlEqualWrapper<KT, Cmp>> data_;
  bool is_primary_key_;
  std::unordered_map<KT, std::vector<VT>, StlHasherWrapper<KT>, StlEqualWrapper<KT, Cmp>> non_unique_entries_;
};

using STLUnorderedIndexForTwoIntegerColumn = STLUnorderedIndex<GenericKey<8>, RID, GenericComparator<8>>;

}  // namespace bustub
