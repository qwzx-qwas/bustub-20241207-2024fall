//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
/*
 * 帮助方法：获取/设置页面类型。
 * Page type 的枚举类定义在 `b_plus_tree_page.h` 中。
 */
 //检查成员变量page_type_是否表示为叶子页的枚举值IndexPageType::LEAF_PAGE
auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
/*
 * 帮助方法：获取/设置页面的大小（页面中存储的键/值对数量）。
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }
void BPlusTreePage::SetSize(int size) { size_ = size; }
void BPlusTreePage::ChangeSizeBy(int amount) { size_ += amount; }

/*
 * Helper methods to get/set max size (capacity) of the page
 */
/*
 * 帮助方法：获取/设置页面的最大容量（max size）。
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 * But whether you will take ceil() or floor() depends on your implementation
 */
/*
 * 帮助方法：获取页面的最小大小（min size）。
 * 通常，min size == max size / 2，但是否采用上取整（ceil）或下取整（floor）取决于具体实现。
 */
auto BPlusTreePage::GetMinSize() const -> int { return max_size_ / 2; }

}  // namespace bustub
