#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { 
  //如果此B+树没有键和值则返回true
  //获取header page
  auto root_page_id = GetRootPageId();
  if (root_page_id == INVALID_PAGE_ID) {
    return true;
  }
  return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
 INDEX_TEMPLATE_ARGUMENTS
 auto BinarySearch(const BPlusTreePage *page, const KeyType &key, const KeyComparator &comparator, bool skip_first_key) ->int {
  int left = skip_first_key ? 1 : 0;
  int right = page->GetSize() - 1;
  int result_index = -1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    KeyType mid_key;
    //这里不用再判断到底是叶子节点还是内部节点了，因为传进来的page已经是对应的类型了
    mid_key = page->KeyAt(mid);
    int cmp = comparator(mid_key, key);
    if (cmp == 0) {
      result_index = mid;
      break;
    } else if (cmp < 0) {
      left = mid + 1;
    } else {
      result_index = mid;
      right = mid - 1;
    }
  }
    return result_index;
 
}
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  /*Declaration of context instance.
  Context ctx;
  (void)ctx;
  return false;*/
  if (IsEmpty()) {
    return false;
  }
  //从根节点开始查找
  page_id_t current_page_id = GetRootPageId();

  //如果根节点是叶子节点
  while (true) {
  ReadPageGuard current_guard = bpm_->ReadPage(current_page_id);
  const BPlusTreePage *current_page = current_guard.As<BPlusTreePage>();
  if (current_page->IsLeafPage()) {
    auto leaf_page = current_guard.As<BPlusTreeLeafPage>();
    //在叶子节点中二分查找key
    int index = BinarySearch(leaf_page, key, comparator_, false);
    if (index != -1 &&index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
      //找到了key
      result->push_back(leaf_page->ValueAt(index));
      return true;
    }
    return false;
  } else {
    auto internal_page = current_guard.As<BPlusTreeInternalPage>();
    //在内部节点中二分查找key,需要略过第一个key,用true来说明这一点
    int index = BinarySearch(internal_page, key, comparator_, true);
    current_page_id = internal_page->ValueAt(index);
    }
  }
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  //使用pageguard中的As方法将页面转换为BPlusTreeHeaderPage类型
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}
 

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
