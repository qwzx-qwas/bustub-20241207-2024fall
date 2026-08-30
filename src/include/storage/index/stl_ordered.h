#pragma once

#include <algorithm>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "container/hash/hash_function.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index.h"
#include "storage/index/stl_comparator_wrapper.h"

namespace bustub {

template <typename KT, typename VT, typename Cmp>
class STLOrderedIndexIterator {
 public:
  STLOrderedIndexIterator(const std::map<KT, VT, StlComparatorWrapper<KT, Cmp>> *map,
                          typename std::map<KT, VT, StlComparatorWrapper<KT, Cmp>>::const_iterator iter)
      : map_(map), iter_(std::move(iter)) {}

  ~STLOrderedIndexIterator() = default;

  auto IsEnd() -> bool { return iter_ == map_->cend(); }

  auto operator*() -> const std::pair<KT, VT> & {
    ret_val_ = *iter_;
    return ret_val_;
  }

  auto operator++() -> STLOrderedIndexIterator & {
    iter_++;
    return *this;
  }

  inline auto operator==(const STLOrderedIndexIterator &itr) const -> bool { return itr.iter_ == iter_; }

  inline auto operator!=(const STLOrderedIndexIterator &itr) const -> bool { return !(*this == itr); }

 private:
  const std::map<KT, VT, StlComparatorWrapper<KT, Cmp>> *map_;
  typename std::map<KT, VT, StlComparatorWrapper<KT, Cmp>>::const_iterator iter_;
  std::pair<KT, VT> ret_val_;
};

template <typename KT, typename VT, typename Cmp>
class STLOrderedIndex : public Index {
 public:
  STLOrderedIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager)
      : Index(std::move(metadata)),
        comparator_(StlComparatorWrapper<KT, Cmp>(Cmp(metadata_->GetKeySchema()))),
        data_(comparator_),
        is_primary_key_(metadata_->IsPrimaryKey()),
        non_unique_entries_(comparator_) {}

  auto InsertEntry(const Tuple &key, VT rid, Transaction *transaction) -> bool override {
    KT index_key;
    index_key.SetFromKey(key);
    std::scoped_lock<std::mutex> lck(lock_);
    if (is_primary_key_) {
      if (data_.count(index_key) == 1) {
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
      if (data_.count(index_key) == 1) {
        *result = std::vector{data_[index_key]};
        return;
      }
      *result = {};
      return;
    }
    const auto iterator = non_unique_entries_.find(index_key);
    *result = iterator == non_unique_entries_.end() ? std::vector<RID>{} : iterator->second;
  }

  auto GetBeginIterator() -> STLOrderedIndexIterator<KT, VT, Cmp> { return {&data_, data_.cbegin()}; }

  auto GetBeginIterator(const KT &key) -> STLOrderedIndexIterator<KT, VT, Cmp> {
    return {&data_, data_.lower_bound(key)};
  }

  auto GetEndIterator() -> STLOrderedIndexIterator<KT, VT, Cmp> { return {&data_, data_.cend()}; }

 protected:
  std::mutex lock_;
  StlComparatorWrapper<KT, Cmp> comparator_;
  std::map<KT, VT, StlComparatorWrapper<KT, Cmp>> data_;
  bool is_primary_key_;
  std::map<KT, std::vector<VT>, StlComparatorWrapper<KT, Cmp>> non_unique_entries_;
};

using STLOrderedIndexForTwoIntegerColumn = STLOrderedIndex<GenericKey<8>, RID, GenericComparator<8>>;

}  // namespace bustub
