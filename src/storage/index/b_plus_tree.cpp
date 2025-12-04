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
auto BPLUSTREE_TYPE::FindLeafPage(page_id_t page_id, const KeyType &key, std::vector<ValueType> *result) -> page_id_t {
  //从根节点开始查找
  page_id_t current_page_id = page_id;

  while (true) {
  ReadPageGuard current_guard = bpm_->ReadPage(current_page_id);
  const BPlusTreePage *current_page = current_guard.As<BPlusTreePage>();
  //如果根节点是叶子节点
  if (current_page->IsLeafPage()) {
    auto leaf_page = current_guard.As<BPlusTreeLeafPage>();
    //在叶子节点中二分查找key
    int index = BinarySearch(leaf_page, key, comparator_, false);
    if (index != -1 &&index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
      //找到了key
      result->push_back(leaf_page->ValueAt(index));
    }
      return current_page_id;
  }
    auto internal_page = current_guard.As<BPlusTreeInternalPage>();
    //在内部节点中二分查找key,需要略过第一个key,用true来说明这一点
    int index = BinarySearch(internal_page, key, comparator_, true);
    current_page_id = internal_page->ValueAt(index);
    
  }
 
}
//二分查找返回的是小于等于key的最大索引
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

  page_id_t leaf_page_id = FindLeafPage(current_page_id, key, &result);

  if (!result->empty()) {
    return true;
  }
  return false;

  /*
  while (true) {
  ReadPageGuard current_guard = bpm_->ReadPage(current_page_id);
  const BPlusTreePage *current_page = current_guard.As<BPlusTreePage>();
  //如果根节点是叶子节点
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
    */
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
 INDEX_TEMPLATE_ARGUMENTS
 auto BPLUSTREE_TYPE::InsertIntoNewTree(const KeyType &key, const ValueType &value, Context &ctx) -> bool {
  //如果B+树为空，创建新的根节点
    auto new_root_page_id = bpm_->NewPage();
    if (new_root_page_id == INVALID_PAGE_ID) {
      throw Exception("Failed to allocate new page for root");
    }
    //在此保存根节点的id
    ctx.root_page_id_ = new_root_page_id;
    //将根节点页面的写保护存放到context中
    WritePageGuard new_root_guard = bpm_->WritePage(new_root_page_id);
    ctx.write_set_.push_back(std::move(new_root_guard));
    //初始化根节点为叶子节点
    auto &root_guard = ctx.write_set_.back();
    auto root_page = root_guard.AsMut<BPlusTreeLeafPage>();
    root_page->Init(leaf_max_size_);
    //插入键值对到根节点
    root_page->key_array_[0] = key;
    root_page->rid_array_[0] = value;
    root_page->SetSize(1);
    //更新header_page中的root_page_id
    auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = new_root_page_id;
    return true;
}

//处理叶子节点分裂
auto BPLUSTREE_TYPE::HandleLeafSplit(BPlusTreeLeafPage *leaf_page, Context &ctx) -> void {
  //这里由外部函数来承担检查是否要分裂的责任

  //创建新的叶子节点
  auto new_leaf_page_id = bpm_->NewPage();
  if (new_leaf_page_id == INVALID_PAGE_ID) {
    throw Exception("Failed to allocate new page for leaf split");
  }
  WritePageGuard new_leaf_guard = bpm_->WritePage(new_leaf_page_id);
  ctx.write_set_.push_back(std::move(new_leaf_guard));

  //获取新叶子节点的可变引用，其实也就是刚才push进去的那个guard
  auto &new_leaf_page_guard = ctx.write_set_.back();
  auto new_leaf_page = new_leaf_page_guard.AsMut<BPlusTreeLeafPage>();
  new_leaf_page->Init(leaf_max_size_);

  //将原叶子节点的一半数据移动到新叶子节点
  int total_size = leaf_page->GetSize();
  int move_start_index = total_size / 2;
  int move_count = total_size - move_start_index;

  for (int i = 0; i < move_count; i++) {
    new_leaf_page->key_array_[i] = leaf_page->key_array_[move_start_index + i];
    new_leaf_page->rid_array_[i] = leaf_page->rid_array_[move_start_index + i];
  }

  new_leaf_page->SetSize(move_count);
  leaf_page->SetSize(move_start_index);

  //更新叶子节点的链表指针
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_page_id);

  //如果当前是根节点(注意，当需要调用这个分裂函数时，
  // 就说明当前节点肯定会分裂，所以这里肯定会对上层传递"影响")
  //通过deque的大小来判断当前节点是否为根节点
  if(ctx.write_set_.back().GetPageId() == ctx.root_page_id_){
     //处理根节点分裂
    CreateNewRoot(leaf_page, new_leaf_page, ctx);
  } else {
    //不是根节点就要往上走
    //将新叶子节点的第一个key插入到父节点中
    KeyType new_key = new_leaf_page->KeyAt(0);
    //注意ctx的write_set_中的倒数第二个才是parent guard

    InsertIntoParent(leaf_page, new_key, new_leaf_page, ctx);
  
    //释放叶子节点的写保护
    ctx.write_set_.pop_back();
    //检查父节点是否溢出需要分裂
    BPlusTreeInternalPage *parent_page = ctx.write_set_.back().AsMut<BPlusTreeInternalPage>();
    if (parent_page->GetSize() > parent_page->GetMaxSize()) {
      HandleInternalSplit(parent_page, ctx);
    }
  }
}

//将新节点插入到父节点中
auto BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *left_child, const KeyType &key, BPlusTreePage *right_child, Context &ctx) -> void {

  //通过ctx的write_set_找到parent guard
  WritePageGuard &parent_guard = ctx.write_set_.back();
  auto parent_page = parent_guard.AsMut<BPlusTreeInternalPage>();

  //在parent_page中找到插入key的位置
  int insert_index = BinarySearch(parent_page, key, comparator_, true);
  insert_index++; //因为BinarySearch返回的是小于等于key的最大索引，所以要加1

  //将parent_page中insert_index及之后的元素后移一位
  for (int i = parent_page->GetSize(); i > insert_index; i--) {
    parent_page->SetKeyAt(i, parent_page->KeyAt(i - 1));
    parent_page->page_id_array_[i + 1] = parent_page->page_id_array_[i];
  }

  //插入新的key和right_child的page_id
  parent_page->SetKeyAt(insert_index, key);
  parent_page->page_id_array_[insert_index + 1] = right_child->GetPageId();
  parent_page->SetSize(parent_page->GetSize() + 1);
}



auto BPLUSTREE_TYPE::HandleInternalSplit(BPlusTreeInternalPage *internal_page, Context &ctx) -> void {
  auto new_internal_page_id = bpm_->NewPage();
  if (new_internal_page_id == INVALID_PAGE_ID) {
    throw Exception("Failed to allocate new page for internal split");
  }
  WritePageGuard new_internal_guard = bpm_->WritePage(new_internal_page_id);
  ctx.write_set_.push_back(std::move(new_internal_guard));
  auto &new_internal_page_guard = ctx.write_set_.back();
  auto new_internal_page = new_internal_page_guard.AsMut<BPlusTreeInternalPage>();
  new_internal_page->Init(internal_max_size_);
  //将原内部节点的一半数据移动到新内部节点
  int old_keys = internal_page->GetSize() - 1; //不包括第一个无效key
  int mid = old_keys / 2; //中间位置
  int j = 0;
  for (int i = mid + 1; i <= old_keys; i++) {
    new_internal_page->SetKeyAt(j + 1, internal_page->KeyAt(i));
    new_internal_page->page_id_array_[j] = internal_page->page_id_array_[i];
    j++;
  }
  //移动最后一个page_id
  new_internal_page->page_id_array_[j] = internal_page->page_id_array_[old_keys + 1];

  new_internal_page->SetSize(old_keys - mid);
  internal_page->SetSize(mid); //保留中间key给父节点
  
  //如果当前是根节点
  if(ctx.write_set_.back().GetPageId() == ctx.root_page_id_){
     //处理根节点分裂
     CreateNewRoot(internal_page, new_internal_page, ctx);
  } else {
     //不是根节点就要往上走
    //将原来中间的key插入到父节点中
    KeyType new_key = internal_page->KeyAt(mid);
    InsertIntoParent(internal_page, new_key, new_internal_page, ctx);
    //释放内部节点的写保护
    ctx.write_set_.pop_back();
    //检查父节点是否溢出需要分裂
    BPlusTreeInternalPage *parent_page = ctx.write_set_.back().AsMut<BPlusTreeInternalPage>();
    if (parent_page->GetSize() > parent_page->GetMaxSize()) {
      HandleInternalSplit(parent_page, ctx);
    }
  }
}


auto BPLUSTREE_TYPE::CreateNewRoot(BPlusTreePage *left_child, BPlusTreePage *right_child, Context &ctx) -> void {
  //创建新的根节点
  auto new_root_page_id = bpm_->NewPage();
  if (new_root_page_id == INVALID_PAGE_ID) {
    throw Exception("Failed to allocate new page for new root");
  }
  WritePageGuard new_root_guard = bpm_->WritePage(new_root_page_id);
  ctx.write_set_.push_back(std::move(new_root_guard));
  auto &new_root_page_guard = ctx.write_set_.back();
  auto new_root_page = new_root_page_guard.AsMut<BPlusTreeInternalPage>();
  new_root_page->Init(internal_max_size_);

  //设置新的根节点的第一个key和两个子指针
  KeyType new_key;
  if (left_child->IsLeafPage()) {
    auto left_leaf = dynamic_cast<BPlusTreeLeafPage *>(left_child);
    new_key = left_leaf->KeyAt(0);
  } else {
    auto left_internal = dynamic_cast<BPlusTreeInternalPage *>(left_child);
    new_key = left_internal->KeyAt(1); //第一个key无效
  }
  new_root_page->SetKeyAt(1, new_key);
  new_root_page->page_id_array_[0] = left_child->GetPageId();
  new_root_page->page_id_array_[1] = right_child->GetPageId();
  new_root_page->SetSize(1);

  //更新header_page中的root_page_id
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = new_root_page_id;

  //更新context中的root_page_id
  ctx.root_page_id_ = new_root_page_id;
}
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
  /*Context ctx;
  (void)ctx;      是一种无副作用的 C++ 技巧，用于在变量尚未被使用时抑制编译器警告。
  return false;*/
  Context ctx;
  
  //先获取header_page的写入guard，以便后续可能更新root_page_id
  WritePageGuard header_guard = bpm_->WritePage(header_page_id_);
  //将header_paged的写保护存放到context中
  ctx.header_page_ = std::move(header_guard);

  if (IsEmpty()) {
    return InsertIntoNewTree(key, value, ctx);
  }
  //如果B+树不为空
  //从根节点开始查找
  page_id_t current_page_id = GetRootPageId();
  std::vector<ValueType> temp_result;
  page_id_t leaf_page_id = FindLeafPage(current_page_id, key, &temp_result);

  if (!temp_result.empty()) {
    //说明key已经存在，返回false
    return false;
  }

  auto leaf_guard = bpm_->WritePage(leaf_page_id);
  ctx.write_set_.push_back(std::move(leaf_guard));
  auto &leaf_page_guard = ctx.write_set_.back();
  auto leaf_page = leaf_page_guard.AsMut<BPlusTreeLeafPage>();

  //在叶子节点中找到插入key的位置
  int insert_index = BinarySearch(leaf_page, key, comparator_, false);
  insert_index++; //因为BinarySearch返回的是小于等于key的最大索引，所以要加1
  //将叶子节点中insert_index及之后的元素后移一位
  for (int i = leaf_page->GetSize(); i > insert_index; i--) {
    leaf_page->SetKeyAt(i, leaf_page->KeyAt(i - 1));
    leaf_page->rid_array_[i] = leaf_page->rid_array_[i - 1];
  } 
  leaf_page->SetKeyAt(insert_index, key);
  leaf_page->rid_array_[insert_index] = value;
  leaf_page->IncreaseSize(1);

  //检查叶子节点是否溢出需要分裂
  if (leaf_page->GetSize() > leaf_page->GetMaxSize()) {
    HandleLeafSplit(leaf_page, ctx);
  }
  return true;

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
