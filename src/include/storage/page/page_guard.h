//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.h
//
// Identification: src/include/storage/page/page_guard.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "buffer/buffer_pool_manager.h"
#include "storage/page/page.h"

namespace bustub {

class BufferPoolManager;
class FrameHeader;

/**
 * @brief An RAII object that grants thread-safe read access to a page of data.
 *
 * The _only_ way that the BusTub system should interact with the buffer pool's page data is via page guards. Since
 * `ReadPageGuard` is an RAII object, the system never has to manually lock and unlock a page's latch.
 *
 * With `ReadPageGuard`s, there can be multiple threads that share read access to a page's data. However, the existence
 * of any `ReadPageGuard` on a page implies that no thread can be mutating the page's data.
 *
 * @brief 一个 RAII 对象，提供对页面数据的线程安全只读访问。
 *
 * BusTub 系统与缓冲池中页面数据交互的唯一途径应该是通过页面保护器（page guards）。由于
 * `ReadPageGuard` 是一个 RAII 对象，系统无需手动加锁或解锁页面的 latch。
 *
 * 使用 `ReadPageGuard` 时，可以有多个线程共享对页面数据的只读访问。但只要存在任何一个
 * `ReadPageGuard`，就意味着没有线程可以修改该页面的数据。
 */
class ReadPageGuard {
  /** @brief Only the buffer pool manager is allowed to construct a valid `ReadPageGuard.` */
  /** @brief 只有缓冲池管理器被允许构造有效的 `ReadPageGuard`。 */
  friend class BufferPoolManager;

 public:
  /**
   * @brief The default constructor for a `ReadPageGuard`.
   *
   * Note that we do not EVER want use a guard that has only been default constructed. The only reason we even define
   * this default constructor is to enable placing an "uninitialized" guard on the stack that we can later move assign
   * via `=`.
   *
   * **Use of an uninitialized page guard is undefined behavior.**
   *
   * In other words, the only way to get a valid `ReadPageGuard` is through the buffer pool manager.
   *
   * @brief `ReadPageGuard` 的默认构造函数。
   *
   * 注意：我们绝不应该使用仅通过默认构造得到的保护器。定义默认构造函数的唯一目的是允许
   * 在栈上放置一个“未初始化”的保护器，然后通过移动赋值（`=`）来初始化它。
   *
   * **使用未初始化的页面保护器是未定义行为。**
   *
   * 换句话说，获取有效的 `ReadPageGuard` 的唯一方式是通过缓冲池管理器。
   */
  ReadPageGuard() = default;

  ReadPageGuard(const ReadPageGuard &) = delete;
  auto operator=(const ReadPageGuard &) -> ReadPageGuard & = delete;
  ReadPageGuard(ReadPageGuard &&that) noexcept;
  auto operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard &;
  auto GetPageId() const -> page_id_t;
  auto GetData() const -> const char *;
  template <class T>
  auto As() const -> const T * {
    return reinterpret_cast<const T *>(GetData());
  }
  auto IsDirty() const -> bool;
  void Drop();
  ~ReadPageGuard();

 private:
  /** @brief Only the buffer pool manager is allowed to construct a valid `ReadPageGuard.` */
  /** @brief 只有缓冲池管理器被允许构造有效的 `ReadPageGuard`。 */
  explicit ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame, std::shared_ptr<LRUKReplacer> replacer,
                         std::shared_ptr<std::mutex> bpm_latch);

  /** @brief The page ID of the page we are guarding. */
  /** @brief 我们正在保护的页面的页面 ID。 */
  page_id_t page_id_;

  /**
   * @brief The frame that holds the page this guard is protecting.
   *
   * Almost all operations of this page guard should be done via this shared pointer to a `FrameHeader`.
   */
  /**
   * @brief 保存被该保护器保护页面的帧。
   *
   * 该页面保护器的几乎所有操作都应通过指向 `FrameHeader` 的这个共享指针来完成。
   */
  std::shared_ptr<FrameHeader> frame_;

  /**
   * @brief A shared pointer to the buffer pool's replacer.
   *
   * Since the buffer pool cannot know when this `ReadPageGuard` gets destructed, we maintain a pointer to the buffer
   * pool's replacer in order to set the frame as evictable on destruction.
   */
  /**
   * @brief 指向缓冲池替换器（replacer）的共享指针。
   *
   * 由于缓冲池无法得知何时该 `ReadPageGuard` 被析构，我们保留对缓冲池 replacer 的指针，
   * 以便在析构时将帧设置为可驱逐（evictable）。
   */
  std::shared_ptr<LRUKReplacer> replacer_;

  /**
   * @brief A shared pointer to the buffer pool's latch.
   *
   * Since the buffer pool cannot know when this `ReadPageGuard` gets destructed, we maintain a pointer to the buffer
   * pool's latch for when we need to update the frame's eviction state in the buffer pool replacer.
   */
  /**
   * @brief 指向缓冲池 latch 的共享指针。
   *
   * 由于缓冲池无法得知何时该 `ReadPageGuard` 被析构，我们保留对缓冲池 latch 的指针，
   * 以便在需要更新帧在 replacer 中的驱逐状态时使用。
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /**
   * @brief The validity flag for this `ReadPageGuard`.
   *
   * Since we must allow for the construction of invalid page guards (see the documentation above), we must maintain
   * some sort of state that tells us if this page guard is valid or not. Note that the default constructor will
   * automatically set this field to `false`.
   *
   * If we did not maintain this flag, then the move constructor / move assignment operators could attempt to destruct
   * or `Drop()` invalid members, causing a segmentation fault.
   */
  /**
   * @brief 此 `ReadPageGuard` 的有效性标志。
   *
   * 由于我们必须允许构造无效的页面保护器（见上文），因此需要维护某种状态以指示该保护器是否有效。
   * 注意默认构造函数会将此字段自动设为 `false`。
   *
   * 如果不维护此标志，则移动构造或移动赋值可能会尝试析构或调用 `Drop()` 在无效成员上，
   * 导致段错误（segmentation fault）。
   */
   //标记它为有效标明它现在是一个活跃的保护器
  bool is_valid_{false};

  /**
   * TODO(P1): You may add any fields under here that you think are necessary.
   *
   * If you want extra (non-existent) style points, and you want to be extra fancy, then you can look into the
   * `std::shared_lock` type and use that for the latching mechanism instead of manually calling `lock` and `unlock`.
   */
  /**
   * TODO(P1)：你可以在此处添加任何你认为必要的字段。
   *
   * 如果你想额外加分并且想更优雅，可以考虑使用 `std::shared_lock` 类型来作为加锁机制，
   * 代替手动调用 `lock` 和 `unlock`。
   */
};

/**
 * @brief An RAII object that grants thread-safe write access to a page of data.
 *
 * The _only_ way that the BusTub system should interact with the buffer pool's page data is via page guards. Since
 * `WritePageGuard` is an RAII object, the system never has to manually lock and unlock a page's latch.
 *
 * With a `WritePageGuard`, there can be only be 1 thread that has exclusive ownership over the page's data. This means
 * that the owner of the `WritePageGuard` can mutate the page's data as much as they want. However, the existence of a
 * `WritePageGuard` implies that no other `WritePageGuard` or any `ReadPageGuard`s for the same page can exist at the
 * same time.
 *
 * @brief 一个 RAII 对象，提供对页面数据的线程安全写访问。
 *
 * BusTub 系统与缓冲池页面数据交互的唯一途径应当是通过页面保护器。由于 `WritePageGuard` 是 RAII 对象，
 * 系统无需手动加锁/解锁页面的 latch。
 *
 * 使用 `WritePageGuard` 时，最多只有 1 个线程对页面具有独占所有权，这意味着持有 `WritePageGuard` 的线程
 * 可以自由地修改页面数据。同时，若存在 `WritePageGuard`，则不会有其他 `WritePageGuard` 或该页面的
 * `ReadPageGuard` 并存。
 */
class WritePageGuard {
  /** @brief Only the buffer pool manager is allowed to construct a valid `WritePageGuard.` */
  /** @brief 只有缓冲池管理器被允许构造有效的 `WritePageGuard`。 */
  friend class BufferPoolManager;

 public:
  /**
   * @brief The default constructor for a `WritePageGuard`.
   *
   * Note that we do not EVER want use a guard that has only been default constructed. The only reason we even define
   * this default constructor is to enable placing an "uninitialized" guard on the stack that we can later move assign
   * via `=`.
   *
   * **Use of an uninitialized page guard is undefined behavior.**
   *
   * In other words, the only way to get a valid `WritePageGuard` is through the buffer pool manager.
   *
   * @brief `WritePageGuard` 的默认构造函数。
   *
   * 注意：我们绝不应该使用仅通过默认构造得到的保护器。定义默认构造函数的唯一目的是允许
   * 在栈上放置一个“未初始化”的保护器，然后通过移动赋值（`=`）来初始化它。
   *
   * **使用未初始化的页面保护器是未定义行为。**
   *
   * 换句话说，获取有效的 `WritePageGuard` 的唯一方式是通过缓冲池管理器。
   */
  WritePageGuard() = default;

  WritePageGuard(const WritePageGuard &) = delete;
  auto operator=(const WritePageGuard &) -> WritePageGuard & = delete;
  WritePageGuard(WritePageGuard &&that) noexcept;
  auto operator=(WritePageGuard &&that) noexcept -> WritePageGuard &;
  auto GetPageId() const -> page_id_t;
  auto GetData() const -> const char *;
  template <class T>
  auto As() const -> const T * {
    return reinterpret_cast<const T *>(GetData());
  }
  auto GetDataMut() -> char *;
  template <class T>
  auto AsMut() -> T * {
    return reinterpret_cast<T *>(GetDataMut());
  }
  auto IsDirty() const -> bool;
  void Drop();
  ~WritePageGuard();

 private:
  /** @brief Only the buffer pool manager is allowed to construct a valid `WritePageGuard.` */
  /** @brief 只有缓冲池管理器被允许构造有效的 `WritePageGuard`。 */
  explicit WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame, std::shared_ptr<LRUKReplacer> replacer,
                          std::shared_ptr<std::mutex> bpm_latch);

  /** @brief The page ID of the page we are guarding. */
  /** @brief 我们正在保护的页面的页面 ID。 */
  page_id_t page_id_;

  /**
   * @brief The frame that holds the page this guard is protecting.
   *
   * Almost all operations of this page guard should be done via this shared pointer to a `FrameHeader`.
   */
  /**
   * @brief 保存被该保护器保护页面的帧。
   *
   * 该页面保护器的几乎所有操作都应通过指向 `FrameHeader` 的这个共享指针来完成。
   */
  std::shared_ptr<FrameHeader> frame_;

  /**
   * @brief A shared pointer to the buffer pool's replacer.
   *
   * Since the buffer pool cannot know when this `WritePageGuard` gets destructed, we maintain a pointer to the buffer
   * pool's replacer in order to set the frame as evictable on destruction.
   */
  /**
   * @brief 指向缓冲池替换器（replacer）的共享指针。
   *
   * 由于缓冲池无法得知何时该 `WritePageGuard` 被析构，我们保留对缓冲池 replacer 的指针，
   * 以便在析构时将帧设置为可驱逐（evictable）。
   */
  std::shared_ptr<LRUKReplacer> replacer_;

  /**
   * @brief A shared pointer to the buffer pool's latch.
   *
   * Since the buffer pool cannot know when this `WritePageGuard` gets destructed, we maintain a pointer to the buffer
   * pool's latch for when we need to update the frame's eviction state in the buffer pool replacer.
   */
  /**
   * @brief 指向缓冲池 latch 的共享指针。
   *
   * 由于缓冲池无法得知何时该 `WritePageGuard` 被析构，我们保留对缓冲池 latch 的指针，
   * 以便在需要更新帧在 replacer 中的驱逐状态时使用。
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /**
   * @brief The validity flag for this `WritePageGuard`.
   *
   * Since we must allow for the construction of invalid page guards (see the documentation above), we must maintain
   * some sort of state that tells us if this page guard is valid or not. Note that the default constructor will
   * automatically set this field to `false`.
   *
   * If we did not maintain this flag, then the move constructor / move assignment operators could attempt to destruct
   * or `Drop()` invalid members, causing a segmentation fault.
   */
  /**
   * @brief 此 `WritePageGuard` 的有效性标志。
   *
   * 由于我们必须允许构造无效的页面保护器（见上文），因此需要维护某种状态以指示该保护器是否有效。
   * 注意默认构造函数会将此字段自动设为 `false`。
   *
   * 如果不维护此标志，则移动构造或移动赋值可能会尝试析构或调用 `Drop()` 在无效成员上，
   * 导致段错误（segmentation fault）。
   */
   //标记它为有效标明它现在是一个活跃的保护器
  bool is_valid_{false};

  /**
   * TODO(P1): You may add any fields under here that you think are necessary.
   *
   * If you want extra (non-existent) style points, and you want to be extra fancy, then you can look into the
   * `std::unique_lock` type and use that for the latching mechanism instead of manually calling `lock` and `unlock`.
   */
  /**
   * TODO(P1)：你可以在此处添加任何你认为必要的字段。
   *
   * 如果你想额外加分并且想更优雅，可以考虑使用 `std::unique_lock` 类型来作为加锁机制，
   * 代替手动调用 `lock` 和 `unlock`。
   */
};

}  // namespace bustub
