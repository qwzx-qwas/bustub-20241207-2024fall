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
auto BPLUSTREE_TYPE::FindLeafPage(page_id_t page_id, const KeyType &key, std::vector<ValueType> *result) -> page_id_t {
  //从根节点开始查找
  page_id_t current_page_id = page_id;

  while (true) {
  ReadPageGuard current_guard = bpm_->ReadPage(current_page_id);
  const BPlusTreePage *current_page = current_guard.As<BPlusTreePage>();
  //如果根节点是叶子节点
  if (current_page->IsLeafPage()) {
    auto leaf_page = current_guard.As<LeafPage>();
    //在叶子节点中二分查找key
    int index = BinarySearch(leaf_page, key, comparator_, false);
    if (index != -1 &&index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
      //找到了key
      result->push_back(leaf_page->ValueAt(index));
    }
      return current_page_id;
  }
    auto internal_page = current_guard.As<InternalPage>();
    //在内部节点中二分查找key,需要略过第一个key,用true来说明这一点
    int index = BinarySearch(internal_page, key, comparator_, true);
    current_page_id = internal_page->ValueAt(index);
    
  }
 
}
//二分查找返回的是小于等于key的最大索引
 INDEX_TEMPLATE_ARGUMENTS
 auto BPLUSTREE_TYPE::BinarySearch(const BPlusTreePage *page, const KeyType &key, const KeyComparator &comparator, bool skip_first_key) ->int {
  int left = skip_first_key ? 1 : 0;
  int right = page->GetSize() - 1;
  int result_index = skip_first_key ? 0 : -1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    KeyType mid_key;
    if (page->IsLeafPage()) {
      auto leaf_page = reinterpret_cast<const LeafPage *>(page);
      mid_key = leaf_page->KeyAt(mid);
    } else {
      auto internal_page = reinterpret_cast<const InternalPage *>(page);
      mid_key = internal_page->KeyAt(mid);
    }
    int cmp = comparator(mid_key, key);
    //std::cout << "Compare: mid_key=" << mid_key.ToString() << " key=" << key.ToString() << " cmp=" << cmp << std::endl;
    if (cmp == 0) {
      result_index = mid;
      break;
    } else if (cmp < 0) {
      result_index = mid;
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
   //std::cout << "BinarySearch result_index=" << result_index << std::endl;
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

  page_id_t leaf_page_id = FindLeafPage(current_page_id, key, result);


  if (!result->empty() && leaf_page_id != INVALID_PAGE_ID) {
    return true;
  }
  return false;
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
    auto root_page = root_guard.AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    //插入键值对到根节点
    root_page->SetKeyAt(0, key);
    root_page->SetValueAt(0, value);
    root_page->SetSize(1);
    //更新header_page中的root_page_id
    auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = new_root_page_id;
    return true;
}

//处理叶子节点分裂
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::HandleLeafSplit(WritePageGuard &leaf_guard, Context &ctx) -> void {
  auto leaf_page = leaf_guard.AsMut<LeafPage>();
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
  auto new_leaf_page = new_leaf_page_guard.AsMut<LeafPage>();
  new_leaf_page->Init(leaf_max_size_);

  //将原叶子节点的一半数据移动到新叶子节点
  int total_size = leaf_page->GetSize();
  int move_start_index = total_size / 2;
  int move_count = total_size - move_start_index;

  for (int i = 0; i < move_count; i++) {
    new_leaf_page->SetKeyAt(i, leaf_page->KeyAt(move_start_index + i));
    new_leaf_page->SetValueAt(i, leaf_page->ValueAt(move_start_index + i));
    //别忘了清空原叶子节点的数据
    leaf_page->SetKeyAt(move_start_index + i, KeyType{});
    leaf_page->SetValueAt(move_start_index + i, ValueType{}); 
  }

  new_leaf_page->SetSize(move_count);
  leaf_page->SetSize(move_start_index);

  //更新叶子节点的链表指针
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_page_id);

  //如果当前是根节点(注意，当需要调用这个分裂函数时，
  // 就说明当前节点肯定会分裂，所以这里肯定会对上层传递"影响")
  //通过deque的大小来判断当前节点是否为根节点
  if(leaf_guard.GetPageId() == ctx.root_page_id_){
     //处理根节点分裂
    CreateNewRoot(leaf_guard, new_leaf_page_guard, new_leaf_page->KeyAt(0), ctx);
  } else {
    //不是根节点就要往上走
    //将新叶子节点的第一个key插入到父节点中
     KeyType new_key = new_leaf_page->KeyAt(0);
    //注意ctx的write_set_中的倒数第二个才是parent guard

    InsertIntoParent(leaf_guard, new_key, new_leaf_page_guard, ctx);
  
    //释放new叶子节点的写保护和叶子节点的写保护
    ctx.write_set_.pop_back();
    ctx.write_set_.pop_back();

    //检查父节点是否溢出需要分裂
    auto &parent_guard = ctx.write_set_.back();
    auto parent_page = parent_guard.AsMut<InternalPage>();
    if (parent_page->GetSize() > parent_page->GetMaxSize()) {
      HandleInternalSplit(parent_guard, ctx);
    }
  }
}

//将新节点插入到父节点中
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoParent(WritePageGuard &leaf_guard, const KeyType &key, WritePageGuard &right_guard, Context &ctx) -> void {
  //通过ctx的write_set_找到parent guard
  //直接write_set_.back()应该是有问题的，因为之前的代码中还新加入了new_leaf_page_guard
  //ai的建议
  int parent_index = -1;
  for (size_t i = 0; i < ctx.write_set_.size(); i++) {
    if (ctx.write_set_[i].GetPageId() == leaf_guard.GetPageId()) {
      parent_index = i - 1;
      break;
    }
  }
  
  if (parent_index < 0) {
     throw Exception("Parent not found in write_set_");
  }
//ai建议结束
  WritePageGuard &parent_guard = ctx.write_set_[parent_index];
  auto parent_page = parent_guard.AsMut<InternalPage>();

  //在parent_page中找到插入key的位置
  int insert_index = BinarySearch(parent_page, key, comparator_, true);
  insert_index++; //因为BinarySearch返回的是小于等于key的最大索引，所以要加1

  //将parent_page中insert_index及之后的元素后移一位
  for(int i = parent_page->GetSize(); i > insert_index; i--) {
    //这里不需要错位，正常插入即可（以key的索引为准）
    /*
      比如说当前parent_page的key和value分别是：
      keys: [invalid, 10]
      values:[page0,page1]
      我插入5，pnew
      则应该是：【invalid,5,10】
             【page0,page_new,page1】
      这里10从索引1后移到索引2，page1从索引1后移到索引2
      同步
    */
    parent_page->SetKeyAt(i, parent_page->KeyAt(i - 1));
    parent_page->SetValueAt(i, parent_page->ValueAt(i - 1));
  }

  //插入新的key和right_child的page_id
  parent_page->SetKeyAt(insert_index, key);
  parent_page->SetValueAt(insert_index, right_guard.GetPageId());
  parent_page->SetSize(parent_page->GetSize() + 1);
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::HandleInternalSplit(WritePageGuard &internal_guard, Context &ctx) -> void {
  auto internal_page = internal_guard.AsMut<InternalPage>();
  auto new_internal_page_id = bpm_->NewPage();
  if (new_internal_page_id == INVALID_PAGE_ID) {
    throw Exception("Failed to allocate new page for internal split");
  }
  WritePageGuard new_internal_guard = bpm_->WritePage(new_internal_page_id);
  ctx.write_set_.push_back(std::move(new_internal_guard));
  auto &new_internal_page_guard = ctx.write_set_.back();
  auto new_internal_page = new_internal_page_guard.AsMut<InternalPage>();
  new_internal_page->Init(internal_max_size_);
  //将原内部节点的一半数据移动到新内部节点
  int old_keys = internal_page->GetSize() - 1; //不包括第一个无效key
  int mid = old_keys / 2; //中间位置
  
  /*原代码


  int j = 0;

  for (int i = mid + 1; i <= old_keys; i++) {
    new_internal_page->SetKeyAt(j + 1, internal_page->KeyAt(i));
    new_internal_page->SetValueAt(j, internal_page->ValueAt(i));
    j++;
  }
  new_internal_page->SetValueAt(j, internal_page->ValueAt(old_keys + 1));
  */

  //ai建议
  // 1. 移动 Values (从 mid 到 old_keys)
  // Value(mid) -> New Value(0)
  // Value(mid+1) -> New Value(1) ...
  // 注意这里要移动到old_keys + 1，因为Value比Key多一个
  // 实际上 internal_page->GetSize() 就是 old_keys + 1
  for (int i = mid; i <= old_keys; i++) {
      new_internal_page->SetValueAt(i - mid, internal_page->ValueAt(i));
  }

  // 2. 移动 Keys (从 mid+1 到 old_keys)
  // Key(mid+1) -> New Key(1) ...
  for (int i = mid + 1; i <= old_keys; i++) {
      new_internal_page->SetKeyAt(i - mid, internal_page->KeyAt(i));
  }
  //ai建议结束
  //原代码：new_internal_page->SetSize(old_keys - mid);
  //修改后：注意这里要加1，可以看上面的移动逻辑，数一下发现移动的数目要加个1
  new_internal_page->SetSize(old_keys - mid + 1);
  internal_page->SetSize(mid); //保留中间key给父节点
  
  //如果当前是根节点
  if(internal_guard.GetPageId() == ctx.root_page_id_){
     //处理根节点分裂
     CreateNewRoot(internal_guard, new_internal_page_guard, internal_page->KeyAt(mid), ctx);
  } else {
     //不是根节点就要往上走
    //将原来中间的key插入到父节点中
    KeyType new_key = internal_page->KeyAt(mid);
    InsertIntoParent(internal_guard, new_key, new_internal_page_guard, ctx);
    //释放内部节点的写保护
    ctx.write_set_.pop_back();
    //检查父节点是否溢出需要分裂
    auto &parent_guard = ctx.write_set_.back();
    auto parent_page = parent_guard.AsMut<InternalPage>();
    if (parent_page->GetSize() > parent_page->GetMaxSize()) {
      HandleInternalSplit(parent_guard, ctx);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateNewRoot(WritePageGuard &left_guard, WritePageGuard &right_guard, const KeyType &key, Context &ctx) -> void {

  //创建新的根节点
  auto new_root_page_id = bpm_->NewPage();
  if (new_root_page_id == INVALID_PAGE_ID) {
    throw Exception("Failed to allocate new page for new root");
  }
  WritePageGuard new_root_guard = bpm_->WritePage(new_root_page_id);
  ctx.write_set_.push_back(std::move(new_root_guard));
  auto &new_root_page_guard = ctx.write_set_.back();
  auto new_root_page = new_root_page_guard.AsMut<InternalPage>();
  new_root_page->Init(internal_max_size_);

  //设置新的根节点的第一个key和两个子指针
  new_root_page->SetKeyAt(1, key);
  new_root_page->SetValueAt(0, left_guard.GetPageId());
  new_root_page->SetValueAt(1, right_guard.GetPageId());
  new_root_page->SetSize(2);

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
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  page_id_t current_page_id = header_page->root_page_id_;
  ctx.root_page_id_ = current_page_id; //ai建议：可能是之前没初始化

  if (current_page_id == INVALID_PAGE_ID) { 
    return InsertIntoNewTree(key, value, ctx);
  }
  //如果B+树不为空
  //从根节点开始查找
  
  std::vector<ValueType> temp_result;

  page_id_t leaf_page_id = FindLeafPageForWrite(key, &temp_result, ctx, true);

  if (!temp_result.empty() || leaf_page_id == INVALID_PAGE_ID) {
    //说明key已经存在，返回false
    return false;
  }

  auto &leaf_guard = ctx.write_set_.back();
  auto leaf_page = leaf_guard.AsMut<LeafPage>();

  //在叶子节点中找到插入key的位置
  int insert_index = BinarySearch(leaf_page, key, comparator_, false);
  insert_index++; //因为BinarySearch返回的是小于等于key的最大索引，所以要加1
  //将叶子节点中insert_index及之后的元素后移一位
  for (int i = leaf_page->GetSize(); i > insert_index; i--) {
    leaf_page->SetKeyAt(i, leaf_page->KeyAt(i - 1));
    leaf_page->SetValueAt(i, leaf_page->ValueAt(i - 1));
  } 
  leaf_page->SetKeyAt(insert_index, key);
  leaf_page->SetValueAt(insert_index, value);
  leaf_page->ChangeSizeBy(1);

  //检查叶子节点是否溢出需要分裂
  if (leaf_page->GetSize() > leaf_page->GetMaxSize()) {
    HandleLeafSplit(leaf_guard, ctx);
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
//用于Remove的findleaf逻辑
//这次从find开始就层层设锁
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageForWrite(const KeyType &key, std::vector<ValueType> *result, Context &ctx, bool is_insert) -> page_id_t {
  //从根节点开始查找
  page_id_t current_page_id = ctx.root_page_id_;

  while (true) {
    WritePageGuard current_guard = bpm_->WritePage(current_page_id);
    auto current_page = current_guard.As<BPlusTreePage>();

    bool is_safe = false;
    if (is_insert) {
      //insert安全：小于maxsize
      is_safe = current_page->GetSize() < current_page->GetMaxSize();
    } else {
      //remove安全：大于minsize
      int min_size = current_page->GetMinSize();
      is_safe = current_page->GetSize() > min_size;

      if (ctx.IsRootPage(current_page_id)) {
        //根节点特殊处理
        //根节点安全：叶子节点大于1，内部节点大于2
        if (current_page->IsLeafPage()) {
          is_safe = current_page->GetSize() > 1;
        } else {
          is_safe = current_page->GetSize() > 2;
        }
      }
    }

    if (is_safe) {
      //当前节点安全，释放之前的锁，只保留当前节点的锁
      ctx.header_page_ = std::nullopt;
      ctx.write_set_.clear();
    }

    ctx.write_set_.push_back(std::move(current_guard));

    //如果根节点是叶子节点
    if (current_page->IsLeafPage()) {
      auto leaf_page = ctx.write_set_.back().As<LeafPage>();
      //在叶子节点中二分查找key
      int index = BinarySearch(leaf_page, key, comparator_, false);
      if (index != -1 && index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
        //找到了key
        result->push_back(leaf_page->ValueAt(index));
      }
      return current_page_id;
    }
    auto internal_page = ctx.write_set_.back().As<InternalPage>();
    //在内部节点中二分查找key,需要略过第一个key,用true来说明这一点
    int index = BinarySearch(internal_page, key, comparator_, true);
    current_page_id = internal_page->ValueAt(index);
  }
}

/*
auto BPLUSTREE_TYPE::FindLeafPageForWrite(const KeyType &key, std::vector<ValueType> *result, Context &ctx) -> page_id_t {
  //从根节点开始查找
  page_id_t current_page_id = GetRootPageId();

  while (true) {
  WritePageGuard current_guard = bpm_->WritePage(current_page_id);
  ctx.write_set_.push_back(std::move(current_guard));
  const BPlusTreePage *current_page = ctx.write_set_.back().As<BPlusTreePage>();
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
    auto internal_page = ctx.write_set_.back().As<BPlusTreeInternalPage>();
    //在内部节点中二分查找key,需要略过第一个key,用true来说明这一点
    int index = BinarySearch(internal_page, key, comparator_, true);
    current_page_id = internal_page->ValueAt(index);
  }
}

*/


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  if (IsEmpty()) {
    return; //如果B+树为空，直接返回
  }  
  // Declaration of context instance.
  Context ctx;
  //(void)ctx;
  
  WritePageGuard header_guard = bpm_->WritePage(header_page_id_);
  //将header_paged的写保护存放到context中
  ctx.header_page_ = std::move(header_guard);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;

  std::vector<ValueType> temp_result;
  page_id_t leaf_page_id = FindLeafPageForWrite(key, &temp_result, ctx, false);
  
  if (temp_result.empty() && leaf_page_id == INVALID_PAGE_ID) {
    //说明key不存在，直接返回
    return;
  }
  auto &leaf_page_guard = ctx.write_set_.back();
  auto leaf_page = leaf_page_guard.AsMut<LeafPage>();

  //在叶子节点中找到key的位置
  int delete_index = BinarySearch(leaf_page, key, comparator_, false);
  if (delete_index == -1 || delete_index >= leaf_page->GetSize() ||
      comparator_(leaf_page->KeyAt(delete_index), key) != 0) {
    //key不存在，直接返回
    return;
  }
  for (int i = delete_index; i < leaf_page->GetSize() - 1; i++) {
    leaf_page->SetKeyAt(i, leaf_page->KeyAt(i + 1));
    leaf_page->SetValueAt(i, leaf_page->ValueAt(i + 1));
  }
  leaf_page->ChangeSizeBy(-1);

  //现在要处理删除后可能引起的合并或重分配
  int leaf_min_size = leaf_page->GetMinSize();
  if (leaf_page->GetSize() < leaf_min_size) {
    HandleLeafUnderFlow(leaf_page_guard, ctx);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::HandleLeafUnderFlow(WritePageGuard &leaf_guard, Context &ctx) -> void {
  auto leaf_page = leaf_guard.AsMut<LeafPage>();
   //根节点特殊处理
  if (leaf_guard.GetPageId() == ctx.root_page_id_) {
    //如果仍有key就不管了
    //没有就把树变空
    if (leaf_page->GetSize() == 0) {
      auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = INVALID_PAGE_ID;
      bpm_->DeletePage(leaf_guard.GetPageId());
    }
    return;
  }
  auto &parent_guard = ctx.write_set_.rbegin()[1];
   //优先考虑重分配
  if(RedistributeLeaf(leaf_guard, parent_guard, ctx)){
    //重分配成功，释放锁
    ctx.write_set_.pop_back();
    return;
  }

   //合并,返回的值表示父节点是否也欠满
   //MergeLeaf还必须负责删除从父节点指向被合并叶子节点的page_id
  bool parent_underflow = MergeLeaf(leaf_guard, parent_guard, ctx);


   //释放锁
   ctx.write_set_.pop_back();

  //把任务丢给上层去做
  if (parent_underflow) {
    HandleInternalUnderFlow(parent_guard, ctx);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::HandleInternalUnderFlow(WritePageGuard &internal_guard, Context &ctx) -> void {
  auto page = internal_guard.AsMut<BPlusTreePage>();
  //根节点特殊处理
   if (internal_guard.GetPageId() == ctx.root_page_id_) {
      //如果根还有大于俩子节点就不管了
      if (page->GetSize() > 1) {
        return;
      }
      //否则把树高度降低一层,就是把根节点的唯一子节点提升为新的根节点
      auto internal_page = internal_guard.AsMut<InternalPage>();
      page_id_t new_root_page_id = internal_page->ValueAt(0);
      //更新header_page中的root_page_id
      auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = new_root_page_id;
      //释放旧根节点
      ctx.write_set_.pop_back();
      bpm_->DeletePage(internal_guard.GetPageId());
      return;
    }
  //重分配，注意如果size够就不需要重分配也不需要合并
  auto &parent_guard = ctx.write_set_.rbegin()[1];
   if(RedistributeInternal(internal_guard, parent_guard, ctx)){
    //重分配成功，释放锁
    ctx.write_set_.pop_back();
    return;
  }

  //合并
  bool parent_underflow = MergeInternal(internal_guard, parent_guard, ctx);

   //释放锁
  ctx.write_set_.pop_back();


  //向上传递
  if (parent_underflow) {
    HandleInternalUnderFlow(parent_guard, ctx);
  }
  
}

//重分配叶子节点
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RedistributeLeaf(WritePageGuard &leaf_guard, WritePageGuard &parent_guard, Context &ctx) -> bool {
  auto leaf_page = leaf_guard.AsMut<LeafPage>();
  auto parent_page = parent_guard.AsMut<InternalPage>();

  //看当前叶子是否没达到minsize
  if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    return true; //不需要重分配也不需要合并
  }

  auto current_index = parent_page->ValueIndex(leaf_guard.GetPageId());

  //尝试从左兄弟节点借
  if (current_index > 0) {
    //获取左兄弟节点的page_id
    //这里不将writeguard放到ctx中，因为借完就自动释放锁（RAII）
    page_id_t left_sibling_page_id = parent_page->ValueAt(current_index - 1);
    WritePageGuard left_sibling_guard = bpm_->WritePage(left_sibling_page_id);
    auto left_sibling_page = left_sibling_guard.AsMut<LeafPage>();

    //检查左兄弟节点是否可以借
    if (left_sibling_page->GetSize() > left_sibling_page->GetMinSize()) {
      //可以借
      //将左兄弟节点的最后一个元素移动到当前叶子节点的首部
      KeyType borrowed_key = left_sibling_page->KeyAt(left_sibling_page->GetSize() - 1);
      ValueType borrowed_value = left_sibling_page->ValueAt(left_sibling_page->GetSize() - 1);

      //将借来的元素放到当前叶子节点的首部
      for (int i = leaf_page->GetSize(); i > 0; i--) {
        leaf_page->SetKeyAt(i, leaf_page->KeyAt(i - 1));
        leaf_page->SetValueAt(i, leaf_page->ValueAt(i - 1));
      }

      leaf_page->SetKeyAt(0, borrowed_key);
      leaf_page->SetValueAt(0, borrowed_value);
      leaf_page->ChangeSizeBy(1);
      left_sibling_page->ChangeSizeBy(-1);

      //更新父亲节点的key
      parent_page->SetKeyAt(current_index, leaf_page->KeyAt(0));

      return true; //借成功
    }
  }
    //现在尝试从右兄弟节点借
  page_id_t sibling_page_id = leaf_page->GetNextPageId();
  if (sibling_page_id == INVALID_PAGE_ID ) {
    return false; //没有兄弟节点，同上面不需要重分配一样处理
  }
  
  //能借就借，借完更新父节点
  WritePageGuard sibling_guard = bpm_->WritePage(sibling_page_id);
  auto sibling_page = sibling_guard.AsMut<LeafPage>();

  //从兄弟节点借一个元素到当前叶子节点
  //注意这里的借是指将兄弟节点的最后一个元素移动到当前叶子节点的末尾
  //并且更新兄弟节点的size
  if (sibling_page->GetSize() > sibling_page->GetMinSize()) {
    //兄弟节点可以借
    KeyType borrowed_key = sibling_page->KeyAt(0);
    ValueType borrowed_value = sibling_page->ValueAt(0);
    
    //将借来的元素放到当前叶子节点的末尾
    leaf_page->SetKeyAt(leaf_page->GetSize(), borrowed_key);
    leaf_page->SetValueAt(leaf_page->GetSize(), borrowed_value);
    leaf_page->ChangeSizeBy(1);

    //更新兄弟节点的size
    sibling_page->ChangeSizeBy(-1);

    //将兄弟节点的元素前移一位
    for (int i = 0; i < sibling_page->GetSize(); i++) {
      sibling_page->SetKeyAt(i, sibling_page->KeyAt(i + 1));
      sibling_page->SetValueAt(i, sibling_page->ValueAt(i + 1));
    }

    //更新父节点的key
    int parent_index = current_index + 1;
    parent_page->SetKeyAt(parent_index, sibling_page->KeyAt(0));

    return true;
     //重分配成功
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RedistributeInternal(WritePageGuard &internal_guard, WritePageGuard &parent_guard, Context &ctx) -> bool {
  auto internal_page = internal_guard.AsMut<InternalPage>();
  auto parent_page = parent_guard.AsMut<InternalPage>();

  //看当前内部节点是否没达到minsize
  if (internal_page->GetSize() >= internal_page->GetMinSize()) {
    return true; //不需要重分配，也不需要合并
  }

  auto current_index = parent_page->ValueIndex(internal_guard.GetPageId());

  //尝试从左兄弟节点借
  if (current_index > 0) {
    //获取左兄弟节点的page_id
    //这里不将writeguard放到ctx中，因为借完就自动释放锁（RAII）
    page_id_t left_sibling_page_id = parent_page->ValueAt(current_index - 1);
    WritePageGuard left_sibling_guard = bpm_->WritePage(left_sibling_page_id);
    auto left_sibling_page = left_sibling_guard.AsMut<InternalPage>();

    //检查左兄弟节点是否可以借
    if (left_sibling_page->GetSize() > left_sibling_page->GetMinSize()) {
      //可以借
      //将左兄弟节点的最后一个元素移动到当前内部节点的首部
      KeyType borrowed_key = left_sibling_page->KeyAt(left_sibling_page->GetSize() - 1);
      page_id_t borrowed_value = left_sibling_page->ValueAt(left_sibling_page->GetSize());

      //将借来的元素放到当前内部节点的首部
      //注意第一个key无效
      for (int i = internal_page->GetSize(); i > 0; i--) {
        internal_page->SetKeyAt(i + 1, internal_page->KeyAt(i));
        internal_page->SetValueAt(i + 1, internal_page->ValueAt(i));
      }

      //处理第一个page
      internal_page->SetValueAt(1, internal_page->ValueAt(0));

      //更新父亲节点的key
      KeyType parent_key = parent_page->KeyAt(current_index);
      internal_page->SetKeyAt(1, parent_key);
      internal_page->SetValueAt(0, borrowed_value);
      internal_page->ChangeSizeBy(1);
      left_sibling_page->ChangeSizeBy(-1);
      parent_page->SetKeyAt(current_index, borrowed_key);

      return true; //借成功
    }
  }
    //现在尝试从右兄弟节点借
  if (current_index + 1 >= parent_page->GetSize()) {
    return false; 
  }

  page_id_t right_sibling_page_id = parent_page->ValueAt(current_index + 1);
  WritePageGuard right_sibling_guard = bpm_->WritePage(right_sibling_page_id);
  auto right_sibling_page = right_sibling_guard.AsMut<InternalPage>();

  //能借就借，借完更新父节点
  if (right_sibling_page->GetSize() > right_sibling_page->GetMinSize()) {
    //兄弟节点可以借
    //KeyType borrowed_key = right_sibling_page->KeyAt(1);
    page_id_t borrowed_value = right_sibling_page->ValueAt(0);
    
    //将借来的元素放到当前内部节点的末尾
    internal_page->SetKeyAt(internal_page->GetSize() + 1, parent_page->KeyAt(current_index + 1));
    internal_page->SetValueAt(internal_page->GetSize() + 1, borrowed_value);
    internal_page->ChangeSizeBy(1);

    //更新兄弟节点的size
    right_sibling_page->ChangeSizeBy(-1);

    //将兄弟节点的元素前移一位
    for (int i = 1; i <= right_sibling_page->GetSize(); i++) {
      right_sibling_page->SetKeyAt(i, right_sibling_page->KeyAt(i + 1));
      right_sibling_page->SetValueAt(i - 1, right_sibling_page->ValueAt(i));
    }

    //最后一个page_id单独处理
    right_sibling_page->SetValueAt(right_sibling_page->GetSize(), right_sibling_page->ValueAt(right_sibling_page->GetSize() + 1));

    //更新父节点的key
    parent_page->SetKeyAt(current_index + 1, right_sibling_page->KeyAt(1));

    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeLeaf(WritePageGuard &leaf_guard, WritePageGuard &parent_guard, Context &ctx) -> bool {
  auto leaf_page = leaf_guard.AsMut<LeafPage>();
  auto parent_page = parent_guard.AsMut<InternalPage>();

  //不需要再考虑根节点，因为重分配逻辑已经处理过了
  //也不需要检查当前叶子是否欠满，因为重分配逻辑也已经处理过了
  //默认往左合并 

  //先看看有没有左兄弟节点
  auto current_index = parent_page->ValueIndex(leaf_guard.GetPageId());
  LeafPage *sibling_page = nullptr;
  WritePageGuard sibling_guard;
  auto sibling_size = 0;
  if (current_index > 0) {
    //有左兄弟节点
    page_id_t left_sibling_page_id = parent_page->ValueAt(current_index - 1);
    sibling_guard = bpm_->WritePage(left_sibling_page_id);
    sibling_page = sibling_guard.AsMut<LeafPage>();
    sibling_size = sibling_page->GetSize();
    if (sibling_size < sibling_page->GetMinSize()) {
      return false; //不能合并
    }
    MergeLeafHelper(sibling_guard, leaf_guard, parent_guard, current_index);
    return true;
  } 
    //没有左兄弟节点，只能用右兄弟节点
    page_id_t right_sibling_page_id = leaf_page->GetNextPageId();
    sibling_guard = bpm_->WritePage(right_sibling_page_id);
    sibling_page = sibling_guard.AsMut<LeafPage>();
    sibling_size = sibling_page->GetSize();
    if (sibling_size < sibling_page->GetMinSize()) {
      return false; //不能合并
    }
    MergeLeafHelper(leaf_guard, sibling_guard, parent_guard, current_index);
    return true;
  
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeLeafHelper(WritePageGuard &left_guard, WritePageGuard &right_guard, WritePageGuard &parent_guard, int index_in_parent) -> void {
  auto left_page = left_guard.AsMut<LeafPage>();
  auto right_page = right_guard.AsMut<LeafPage>();
  auto parent_page = parent_guard.AsMut<InternalPage>();
  
  //将右节点的数据移动到左兄弟节点的后面
  for (int i = 0; i < right_page->GetSize(); i++) {
      left_page->SetKeyAt(left_page->GetSize() + i, right_page->KeyAt(i));
      left_page->SetValueAt(left_page->GetSize() + i, right_page->ValueAt(i));
      left_page->ChangeSizeBy(1);
    }
    //更新左兄弟节点的size
    left_page->SetSize(left_page->GetSize() + right_page->GetSize());
    //更新左兄弟节点的next_page_id
    left_page->SetNextPageId(right_page->GetNextPageId());
    //删除当前节点
    bpm_->DeletePage(right_guard.GetPageId());
    //更新父节点，删除指向当前节点的page_id
    for (int i = index_in_parent + 1; i < parent_page->GetSize(); i++) {
      parent_page->SetKeyAt(i - 1, parent_page->KeyAt(i));
      parent_page->SetValueAt(i - 1, parent_page->ValueAt(i));
    }
    parent_page->ChangeSizeBy(-1);
  
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeInternal(WritePageGuard &internal_guard, WritePageGuard &parent_guard, Context &ctx) -> bool {
  //auto internal_page = internal_guard.AsMut<InternalPage>();
  auto parent_page = parent_guard.AsMut<InternalPage>();

  //不需要再考虑根节点，因为重分配逻辑已经处理过了
  //也不需要检查当前内部节点是否欠满，因为重分配逻辑也已经处理过了
  //默认往左合并 

  auto current_index = parent_page->ValueIndex(internal_guard.GetPageId());
  InternalPage *sibling_page = nullptr;
  WritePageGuard sibling_guard;
  auto sibling_size = 0;
  if (current_index > 1) {
    //有左兄弟节点
    page_id_t left_sibling_page_id = parent_page->ValueAt(current_index - 1);
    sibling_guard = bpm_->WritePage(left_sibling_page_id);
    sibling_page = sibling_guard.AsMut<InternalPage>();
    sibling_size = sibling_page->GetSize();
    if (sibling_size < sibling_page->GetMinSize()) {
      return false; //不能合并
    }
    MergeInternalHelper(sibling_guard, internal_guard, parent_guard, current_index);
    return true;
  } 
    //没有左兄弟节点，只能用右兄弟节点
    page_id_t right_sibling_page_id = parent_page->ValueAt(current_index + 1);
    sibling_guard = bpm_->WritePage(right_sibling_page_id);
    sibling_page = sibling_guard.AsMut<InternalPage>();
    sibling_size = sibling_page->GetSize();
    if (sibling_size < sibling_page->GetMinSize()) {
      return false; //不能合并
    }
    MergeInternalHelper(internal_guard, sibling_guard, parent_guard, current_index);
    return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeInternalHelper(WritePageGuard &left_guard, WritePageGuard &right_guard, WritePageGuard &parent_guard, int index_in_parent) -> void {
  auto left_page = left_guard.AsMut<InternalPage>();
  auto right_page = right_guard.AsMut<InternalPage>();
  auto parent_page = parent_guard.AsMut<InternalPage>();
  
    //将parent_page中的key插入到left_page后面
    KeyType parent_key = parent_page->KeyAt(index_in_parent);
    left_page->SetKeyAt(left_page->GetSize() + 1, parent_key);
    left_page->ChangeSizeBy(1);
    //将右节点的数据移动到左兄弟节点的后面
    for (int i = 1; i <= right_page->GetSize(); i++) {
      left_page->SetKeyAt(left_page->GetSize() + 1, right_page->KeyAt(i));
      left_page->SetValueAt(left_page->GetSize() + 1, right_page->ValueAt(i - 1));
      left_page->ChangeSizeBy(1);
    }
    //移动最后一个page_id
    left_page->SetValueAt(left_page->GetSize(), right_page->ValueAt(right_page->GetSize()));
    //删除当前节点
    bpm_->DeletePage(right_guard.GetPageId());
    //更新父节点，删除指向当前节点的page_id
    for (int i = index_in_parent + 1; i < parent_page->GetSize(); i++) {
      parent_page->SetKeyAt(i - 1, parent_page->KeyAt(i));
      parent_page->SetValueAt(i - 1, parent_page->ValueAt(i));
    }
    parent_page->ChangeSizeBy(-1);

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
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}
 

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
