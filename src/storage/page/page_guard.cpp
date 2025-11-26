//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2024-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to read.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 */
/**
 * @brief 只读页面保护器 `ReadPageGuard` 的唯一构造函数，用于创建有效的保护器。
 *
 * 注意：只有缓冲池管理器被允许调用此构造函数。
 *
 * TODO(P1)：添加实现。
 *
 * @param page_id 要读取的页面的页面 ID。
 * @param frame 指向包含要保护页面的帧的共享指针。
 * @param replacer 指向缓冲池管理器 replacer 的共享指针。
 * @param bpm_latch 指向缓冲池管理器 latch 的共享指针。
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch)
    : page_id_(page_id), frame_(std::move(frame)), replacer_(std::move(replacer)), bpm_latch_(std::move(bpm_latch)) {
  frame_->rwlatch_.lock();
  frame_->pin_count_.fetch_add(1);
  replacer_->RecordAccess(frame_->frame_id_);
  replacer_->SetEvictable(frame_->frame_id_, false);
  is_valid_ = true;
}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
/**
 * @brief `ReadPageGuard` 的移动构造函数。
 *
 * ### 实现说明
 *
 * 如果你对移动语义不熟悉，请在线查阅相关学习资料。有许多优质资源（文章、微软教程、
 * YouTube 视频等）对其进行了深入讲解。
 *
 * 确保使另一个保护器失效，否则可能遇到 double free 问题！对于两个对象，
 * 你至少需要更新 5 个字段。
 *
 * TODO(P1)：添加实现。
 *
 * @param that 另一个页面保护器。
 */
//移动构造函数
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  this->page_id_ = that.page_id_;
  this->frame_ = std::move(that.frame_);
  this->replacer_ = std::move(that.replacer_);
  this->bpm_latch_ = std::move(that.bpm_latch_);
  this->is_valid_ = that.is_valid_;
  // 确保使 `that` 失效
  that.page_id_ = INVALID_PAGE_ID;
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
/**
 * @brief `ReadPageGuard` 的移动赋值运算符。
 *
 * ### 实现说明
 *
 * 如果你对移动语义不熟悉，请在线查阅相关学习资料。有许多优质资源（文章、微软教程、
 * YouTube 视频等）对其进行了深入讲解。
 *
 * 确保使另一个保护器失效，否则可能遇到 double free 问题！对于两个对象，
 * 你至少需要更新 5 个字段；对于当前对象，确保释放它可能持有的任何资源。
 *
 * TODO(P1)：添加实现。
 *
 * @param that 另一个页面保护器。
 * @return ReadPageGuard& 新的有效 `ReadPageGuard` 引用。
 */
//移动赋值运算符
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  //防止自赋值
  if (this == &that) {
    return *this;
  }
  //释放当前对象的资源
  if (is_valid_) {
    Drop();
  }
  this->page_id_ = that.page_id_;
  this->frame_ = std::move(that.frame_);
  this->replacer_ = std::move(that.replacer_);
  this->bpm_latch_ = std::move(that.bpm_latch_);
  this->is_valid_ = that.is_valid_;
  //清空that的资源
  that.page_id_ = INVALID_PAGE_ID;
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.is_valid_ = false;
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
/**
 * @brief 获取该保护器所保护页面的页面 ID。
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
/**
 * @brief 获取指向该保护器保护的页面数据的只读指针（const 指针）。
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
/**
 * @brief 返回页面是否为脏页（已修改但尚未刷写到磁盘）。
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
/**
 * @brief 手动释放一个有效 `ReadPageGuard` 的资源。如果该保护器无效，此函数不执行任何操作。
 *
 * ### 实现说明
 *
 * 确保不要重复释放（double free）！同时要非常仔细地考虑你所拥有的资源以及释放这些资源的顺序。
 * 如果顺序错误，很可能导致后续的 Gradescope 测试失败。在某些特定场景下，你可能还需要获取缓冲池管理器的 latch。
 *
 * TODO(P1)：添加实现。
 */
void ReadPageGuard::Drop() {
  if (page_id_ == INVALID_PAGE_ID) {
    return;
  }
  frame_->rwlatch_.unlock();
  //更新pin计数
  std::scoped_lock latch(*bpm_latch_);
  if (frame_->pin_count_.load() > 0) {
    frame_->pin_count_.fetch_sub(1);
    if (frame_->pin_count_.load() == 0) {
      //只标记为可驱逐，不进行驱逐操作
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }
  //将当前对象置为无效
  page_id_ = INVALID_PAGE_ID;
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;
  is_valid_ = false;
}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
/** @brief `ReadPageGuard` 的析构函数。该析构函数简单地调用 `Drop()`。 */
ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 */
/**
 * @brief 只写页面保护器 `WritePageGuard` 的唯一构造函数，用于创建有效的保护器。
 *
 * 注意：只有缓冲池管理器被允许调用此构造函数。
 *
 * TODO(P1)：添加实现。
 *
 * @param page_id 要写入的页面的页面 ID。
 * @param frame 指向包含要保护页面的帧的共享指针。
 * @param replacer 指向缓冲池管理器 replacer 的共享指针。
 * @param bpm_latch 指向缓冲池管理器 latch 的共享指针。
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch)
    : page_id_(page_id), frame_(std::move(frame)), replacer_(std::move(replacer)), bpm_latch_(std::move(bpm_latch)) {
  frame_->rwlatch_.lock();
  frame_->pin_count_.fetch_add(1);
  replacer_->RecordAccess(frame_->frame_id_);
  replacer_->SetEvictable(frame_->frame_id_, false);
  is_valid_ = true;
}

/**
 * @brief The move constructor for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
/**
 * @brief `WritePageGuard` 的移动构造函数。
 *
 * ### 实现说明
 *
 * 如果你对移动语义不熟悉，请在线查阅相关学习资料。有许多优质资源（文章、微软教程、
 * YouTube 视频等）对其进行了深入讲解。
 *
 * 确保使另一个保护器失效，否则可能遇到 double free 问题！对于两个对象，
 * 你至少需要更新 5 个字段。
 *
 * TODO(P1)：添加实现。
 *
 * @param that 另一个页面保护器。
 */
/*
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
 this->page_id_ = that.page_id_;
 this->frame_ = std::move(that.frame_);
 this->replacer_ = std::move(that.replacer_);
 this->bpm_latch_ = std::move(that.bpm_latch_);
 this->is_valid_ = that.is_valid_;
 // 确保使 `that` 失效
 that.page_id_ = INVALID_PAGE_ID;
 //that.frame_ = nullptr;
 that.replacer_ = nullptr;
 that.bpm_latch_ = nullptr;
 that.is_valid_ = false;
}
*/
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept
    : page_id_(that.page_id_),
      frame_(std::move(that.frame_)),
      replacer_(std::move(that.replacer_)),
      bpm_latch_(std::move(that.bpm_latch_)),
      is_valid_(that.is_valid_) {
  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return WritePageGuard& The newly valid `WritePageGuard`.
 */
/**
 * @brief `WritePageGuard` 的移动赋值运算符。
 *
 * ### 实现说明
 *
 * 如果你对移动语义不熟悉，请在线查阅相关学习资料。有许多优质资源（文章、微软教程、
 * YouTube 视频等）对其进行了深入讲解。
 *
 * 确保使另一个保护器失效，否则可能遇到 double free 问题！对于两个对象，
 * 你至少需要更新 5 个字段；对于当前对象，确保释放它可能持有的任何资源。
 *
 * TODO(P1)：添加实现。
 *
 * @param that 另一个页面保护器。
 * @return WritePageGuard& 新的有效 `WritePageGuard` 引用。
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }
  //释放当前对象的资源
  if (is_valid_) {
    Drop();
  }
  this->page_id_ = that.page_id_;
  this->frame_ = std::move(that.frame_);
  this->replacer_ = std::move(that.replacer_);
  this->bpm_latch_ = std::move(that.bpm_latch_);
  this->is_valid_ = that.is_valid_;
  // 确保使 `that` 失效
  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
/**
 * @brief 获取该保护器所保护页面的页面 ID。
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
/**
 * @brief 获取指向该保护器保护的页面数据的只读指针（const 指针）。
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
/**
 * @brief 获取指向该保护器保护的页面数据的可变指针（用于写操作）。
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
/**
 * @brief 返回页面是否为脏页（已修改但尚未刷写到磁盘）。
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
/**
 * @brief 手动释放一个有效 `WritePageGuard` 的资源。如果该保护器无效，此函数不执行任何操作。
 *
 * ### 实现说明
 *
 * 确保不要重复释放（double free）！同时要非常仔细地考虑你所拥有的资源以及释放这些资源的顺序。
 * 如果顺序错误，很可能导致后续的 Gradescope 测试失败。在某些特定场景下，你可能还需要获取缓冲池管理器的 latch。
 *
 * TODO(P1)：添加实现。
 */
void WritePageGuard::Drop() {
  if (page_id_ == INVALID_PAGE_ID) {
    return;
  }

  //更新pin计数
  std::scoped_lock latch(*bpm_latch_);
  frame_->is_dirty_ = true;
  if (frame_->pin_count_.load() > 0) {
    frame_->pin_count_.fetch_sub(1);
  }
  if (frame_->pin_count_.load() == 0) {
    //只标记为可驱逐，不进行驱逐操作
    replacer_->SetEvictable(frame_->frame_id_, true);
  }
  frame_->rwlatch_.unlock();
  //将当前对象置为无效
  page_id_ = INVALID_PAGE_ID;
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;
  is_valid_ = false;
}

/** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
/** @brief `WritePageGuard` 的析构函数。该析构函数简单地调用 `Drop()`。 */
WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
