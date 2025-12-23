// ===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
// ===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 12
#define INTERNAL_PAGE_SLOT_CNT \
  ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / ((int)(sizeof(KeyType) + sizeof(ValueType))))  // NOLINT

/**
 * Store `n` indexed keys and `n + 1` child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: Since the number of keys does not equal to number of child pointers,
 * the first key in key_array_ always remains invalid. That is to say, any search / lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  ---------
 * | HEADER |
 *  ---------
 *  ------------------------------------------
 * | KEY(1)(INVALID) | KEY(2) | ... | KEY(n) |
 *  ------------------------------------------
 *  ---------------------------------------------
 * | PAGE_ID(1) | PAGE_ID(2) | ... | PAGE_ID(n) |
 *  ---------------------------------------------
 */
/**
 * 存储 `n` 个索引键和 `n + 1` 个子指针（页面 ID）的内部页面结构。
 * 指针 PAGE_ID(i) 指向一个子树，该子树中所有键 K 满足：
 * K(i) <= K < K(i+1)。
 * 注意：因为键的数量与子指针数量不同，`key_array_` 中的第一个键始终无效。
 * 换句话说，任何搜索/查找都应忽略第一个键。
 *
 * 内部页面格式（键按递增顺序存储）：
 *  ---------
 * | HEADER |
 *  ---------
 *  ------------------------------------------
 * | KEY(1)(INVALID) | KEY(2) | ... | KEY(n) |
 *  ------------------------------------------
 *  ---------------------------------------------
 * | PAGE_ID(1) | PAGE_ID(2) | ... | PAGE_ID(n) |
 *  ---------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  // 删除所有构造函数/析构函数以确保内存安全
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage &other) = delete;

  /**
   * Writes the necessary header information to a newly created page, must be called after
   * the creation of a new page to make a valid `BPlusTreeInternalPage`
   * @param max_size Maximal size of the page
   */
  /**
   * 将必要的头部信息写入新创建的页面。必须在创建新页面后调用，
   * 以构造一个有效的 `BPlusTreeInternalPage`。
   * @param max_size 页面允许的最大容量
   */
  void Init(int max_size = INTERNAL_PAGE_SLOT_CNT);

  /**
   * @param index The index of the key to get. Index must be non-zero.
   * @return Key at index
   */
  /**
   * @param index 要获取的键的索引，索引必须非零。
   * @return 返回索引位置的键。
   */
  auto KeyAt(int index) const -> KeyType;

  /**
   * @param index The index of the key to set. Index must be non-zero.
   * @param key The new value for key
   */
  /**
   * @param index 要设置的键的索引，索引必须非零。
   * @param key 要设置的新键值。
   */
  void SetKeyAt(int index, const KeyType &key);

  /**
   * @param index The index of the value to set.
   * @param value The new value (page_id)
   */
  void SetValueAt(int index, const ValueType &value);

  /**
   * @param value The value to search for
   * @return The index that corresponds to the specified value
   */
  /**
   * @param value 要查找的值
   * @return 与指定值对应的索引位置
   */
  auto ValueIndex(const ValueType &value) const -> int;

  /**
   * @param index The index to search for
   * @return The value at the index
   */
  /**
   * @param index 要查询的索引位置
   * @return 索引位置对应的值（page id）
   */
  auto ValueAt(int index) const -> ValueType;

  /**
   * @brief For test only, return a string representing all keys in
   * this internal page, formatted as "(key1,key2,key3,...)"
   *
   * @return The string representation of all keys in the current internal page
   */
  /**
   * @brief 仅用于测试，返回表示当前内部页中所有键的字符串，格式为 "(key1,key2,key3,...)"。
   *
   * @return 当前内部页中所有键的字符串表示。
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    // First key of internal page is always invalid
    // 内部页的第一个键始终无效
    for (int i = 1; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

  auto CheckIndex(int index) const -> bool;

 private:
  // Array members for page data.
  // 用于存储页面数据的数组成员。
  KeyType key_array_[INTERNAL_PAGE_SLOT_CNT];
  ValueType page_id_array_[INTERNAL_PAGE_SLOT_CNT];
  // (Fall 2024) Feel free to add more fields and helper functions below if needed
  // （2024 秋）如有需要，可在下方添加更多字段和辅助函数
};

/*
 * Helper method to get/set the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return key_array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { key_array_[index] = key; }

/*
 * Helper method to get the value associated with input "index" (a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return page_id_array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { page_id_array_[index] = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); ++i) {
    if (page_id_array_[i] == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::CheckIndex(int index) const -> bool { return index >= 1 && index < GetSize(); }

}  // namespace bustub
