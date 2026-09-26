// Template implementation shared by business and metadata page access.
#pragma once

namespace bustub {
/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
PAGE_INDEX_TEMPLATE_ARGUMENTS
PAGE_INDEXITERATOR_TYPE::IndexIterator()
    : leaf_guard_(nullptr), current_leaf_page_(nullptr), page_id_(INVALID_PAGE_ID), index_(-1), bpm_(nullptr) {}

PAGE_INDEX_TEMPLATE_ARGUMENTS
PAGE_INDEXITERATOR_TYPE::IndexIterator(ReadPageGuard &&leaf_guard, const LeafPage *current_leaf_page, page_id_t page_id,
                                       int index, PageAccess *bpm)
    : leaf_guard_(std::make_unique<ReadPageGuard>(std::move(leaf_guard))),
      current_leaf_page_(current_leaf_page),
      page_id_(page_id),
      index_(index),
      bpm_(bpm) {
  if (leaf_guard_->GetPageId() != INVALID_PAGE_ID) {
    this->current_leaf_page_ = leaf_guard_->template As<LeafPage>();
  } else {
    this->current_leaf_page_ = nullptr;
  }
}

PAGE_INDEX_TEMPLATE_ARGUMENTS
PAGE_INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

PAGE_INDEX_TEMPLATE_ARGUMENTS
auto PAGE_INDEXITERATOR_TYPE::IsEnd() const -> bool {
  // 应该是当前不指向任何leafpage了，而不是看index越界
  return current_leaf_page_ == nullptr;
}

PAGE_INDEX_TEMPLATE_ARGUMENTS
auto PAGE_INDEXITERATOR_TYPE::operator*() -> std::pair<KeyType, ValueType> {
  return {current_leaf_page_->KeyAt(index_), current_leaf_page_->ValueAt(index_)};
}

PAGE_INDEX_TEMPLATE_ARGUMENTS
auto PAGE_INDEXITERATOR_TYPE::operator++() -> PAGE_INDEXITERATOR_TYPE & {
  // 本页移动
  index_++;
  // 跨页判断
  if (index_ >= current_leaf_page_->GetSize()) {
    // 说明要跨页了
    page_id_t next_page_id = current_leaf_page_->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      // 说明没有下一页了，置为end状态
      page_id_ = INVALID_PAGE_ID;
      index_ = -1;
      current_leaf_page_ = nullptr;
      leaf_guard_.reset();
    } else {
      // 说明还有下一页,找兄弟节点
      // 先释放当前页的guard
      // 获取下一页的guard
      auto new_guard = bpm_->ReadPage(next_page_id);
      leaf_guard_ = std::make_unique<ReadPageGuard>(std::move(new_guard));
      current_leaf_page_ = leaf_guard_->template As<LeafPage>();
      page_id_ = next_page_id;
      index_ = 0;
    }
  }
  return *this;
}

PAGE_INDEX_TEMPLATE_ARGUMENTS
auto PAGE_INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  // 相等条件：page_id和index都相等
  // 或者同时到达末尾
  if (this->IsEnd() && itr.IsEnd()) {
    return true;
  }
  return this->page_id_ == itr.page_id_ && this->index_ == itr.index_;
}

PAGE_INDEX_TEMPLATE_ARGUMENTS
auto PAGE_INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
  bool tmp = !(*this == itr);
  return tmp;
}

}  // namespace bustub
