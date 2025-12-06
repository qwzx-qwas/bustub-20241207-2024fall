/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
/**
 * b_plus_tree.h
 *
 * 简单 B+ 树数据结构的实现：内部页面负责引导搜索，叶子页面包含实际数据。
 * (1) 仅支持唯一键
 * (2) 支持插入和删除
 * (3) 结构应能动态收缩与扩展
 * (4) 实现用于区间扫描的索引迭代器
 */
#pragma once

#include <algorithm>
#include <deque>
#include <filesystem>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;

/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
/**
 * @brief `Context` 类的定义。
 *
 * 提示：该类用于帮助跟踪你正在修改或访问的页面。
 */
class Context {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  // 在向 B+ 树插入或删除时，将 header 页的写保护（WritePageGuard）存放于此。
  // 当需要释放所有锁时，记得释放 header 页保护并将其设为 nullopt。
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  // 在此保存根页面 ID，便于判断当前页面是否为根页面。
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  // 将你正在修改的页面的写保护器（WritePageGuard）存放在此处。
  std::deque<WritePageGuard> write_set_;

  // You may want to use this when getting value, but not necessary.
  // 在获取值时可能会用到，但并非必需。
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

  // Main class providing the API for the Interactive B+ Tree.
  // 提供交互式 B+ 树 API 的主类。
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SLOT_CNT,
                     int internal_max_size = INTERNAL_PAGE_SLOT_CNT);

  //辅助方法，将Insert和GetValue的相似逻辑合并
  auto FindLeafPage(page_id_t page_id, const KeyType &key, std::vector<ValueType> *result) -> page_id_t

  //辅助方法，进行二分查找
  auto BinarySearch(const BPlusTreePage *page, const KeyType &key, const KeyComparator &comparator,
                        bool skip_first_key) -> int;
  
  //当B+树为空时，创建新的根节点
  auto InsertIntoNewTree(const KeyType &key, const ValueType &value, Context &ctx) -> bool;
  
  //执行叶子节点分裂
  auto HandleLeafSplit(LeafPage *leaf_page, Context &ctx) -> void;

  //将新节点插入到父节点中
  auto InsertIntoParent(BPlusTreePage *left_child, const KeyType &key, BPlusTreePage *right_child, Context &ctx) -> void;

  //执行内部节点分裂
  auto HandleInternalSplit(InternalPage *internal_page, Context &ctx) -> void;
  
  //创建新的根节点
  auto CreateNewRoot(BPlusTreePage *left_child, BPlusTreePage *right_child, Context &ctx) -> void;

  auto FindLeafPageForWrite(const KeyType &key, std::vector<ValueType> *result, Context &ctx) -> page_id_t

  auto HandleLeafUnderFlow(LeafPage *leaf_page, Context &ctx) -> void;

  auto HandleInternalUnderFlow(InternalPage *internal_page, Context &ctx) -> void;

  auto RedistributeLeaf(LeafPage *leaf_page, InternalPage *parent_page, Context &ctx) -> bool;

  auto RedistributeInternal(InternalPage *internal_page, InternalPage *parent_page, Context &ctx) -> bool;  

  auto MergeLeaf(LeafPage *leaf_page, InternalPage *parent_page, Context &ctx) -> bool;

  auto BPLUSTREE_TYPE::MergeLeafHelper(BPlusTreeLeafPage *left_page, BPlusTreeLeafPage *right_page,
                BPlusTreeInternalPage *parent_page, int index_in_parent) -> void;

  auto MergeInternal(InternalPage *internal_page, InternalPage *parent_page, Context &ctx) -> bool;
  
  // Returns true if this B+ tree has no keys and values.
  // 如果此 B+ 树没有键和值则返回 true。
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  // 将键值对插入到此 B+ 树中。
  auto Insert(const KeyType &key, const ValueType &value) -> bool;

  // Remove a key and its value from this B+ tree.
  // 从此 B+ 树中删除指定键及其对应的值。
  void Remove(const KeyType &key);

  // Return the value associated with a given key
  // 返回与指定键相关联的值
  auto GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool;

  // Return the page id of the root node
  // 返回根节点的页面 ID
  auto GetRootPageId() -> page_id_t;

  // Index iterator
  // 索引迭代器
  auto Begin() -> INDEXITERATOR_TYPE;

  auto End() -> INDEXITERATOR_TYPE;

  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  // Print the B+ tree
  // 打印 B+ 树结构
  void Print(BufferPoolManager *bpm);

  // Draw the B+ tree
  // 绘制 B+ 树（导出图形）
  void Draw(BufferPoolManager *bpm, const std::filesystem::path &outf);

  /**
   * @brief draw a B+ tree, below is a printed
   * B+ tree(3 max leaf, 4 max internal) after inserting key:
   *  {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 18, 19, 20}
   *
   *                               (25)
   *                 (9,17,19)                          (33)
   *  (1,5)    (9,13)    (17,18)    (19,20,21)    (25,29)    (33,37)
   *
   * @return std::string
   */
  /**
   * @brief 绘制 B+ 树，下图为插入键序列后（叶子最大 3，内部最大 4）的示例。
   *
   *                                (25)
   *                  (9,17,19)                          (33)
   *   (1,5)    (9,13)    (17,18)    (19,20,21)    (25,29)    (33,37)
   *
   * @return std::string
   */
  auto DrawBPlusTree() -> std::string;

  // read data from file and insert one by one
  // 从文件读取数据并逐条插入
  void InsertFromFile(const std::filesystem::path &file_name);

  // read data from file and remove one by one
  // 从文件读取数据并逐条删除
  void RemoveFromFile(const std::filesystem::path &file_name);

  /**
   * @brief Read batch operations from input file, below is a sample file format
   * insert some keys and delete 8, 9 from the tree with one step.
   * { i1 i2 i3 i4 i5 i6 i7 i8 i9 i10 i30 d8 d9 } //  batch.txt
   * B+ Tree(4 max leaf, 4 max internal) after processing:
   *                            (5)
   *                 (3)                (7)
   *            (1,2)    (3,4)    (5,6)    (7,10,30) //  The output tree example
   */
  /**
   * @brief 从输入文件读取批量操作，下面是示例文件格式：
   * 同时插入若干键并在一步中删除 8、9。
   * { i1 i2 i3 i4 i5 i6 i7 i8 i9 i10 i30 d8 d9 } // batch.txt
   * 处理后的 B+ 树示例（叶最大 4、内部最大 4）：
   *                             (5)
   *                  (3)                (7)
   *             (1,2)    (3,4)    (5,6)    (7,10,30)
   */
  void BatchOpsFromFile(const std::filesystem::path &file_name);

 private:
  /* Debug Routines for FREE!! */
  /* 调试例程（免费提供） */
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  /**
   * @brief Convert A B+ tree into a Printable B+ tree
   *
   * @param root_id
   * @return PrintableNode
   */
  /**
   * @brief 将 B+ 树转换为可打印的 PrintableBPlusTree 结构。
   *
   * @param root_id 根节点 ID
   * @return PrintableNode
   */
  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  // member variable
  // 成员变量
  std::string index_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
};

/**
 * @brief for test only. PrintableBPlusTree is a printable B+ tree.
 * We first convert B+ tree into a printable B+ tree and the print it.
 */
/**
 * @brief 仅用于测试。PrintableBPlusTree 是可打印的 B+ 树结构。
 * 我们先将 B+ 树转换为可打印形式，然后输出它。
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief BFS traverse a printable B+ tree and print it into
   * into out_buf
   *
   * @param out_buf
   */
  /**
   * @brief 对可打印的 B+ 树执行广度优先遍历（BFS），并将其打印到 `out_buf`。
   *
   * @param out_buf 输出缓冲流
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub
