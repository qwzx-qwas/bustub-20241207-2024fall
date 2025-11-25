//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;

/**
 * @brief A helper class for `BufferPoolManager` that manages a frame of memory and related metadata.
 *
 * This class represents headers for frames of memory that the `BufferPoolManager` stores pages of data into. Note that
 * the actual frames of memory are not stored directly inside a `FrameHeader`, rather the `FrameHeader`s store pointer
 * to the frames and are stored separately them.
 *
 * ---
 *
 * Something that may (or may not) be of interest to you is why the field `data_` is stored as a vector that is
 * allocated on the fly instead of as a direct pointer to some pre-allocated chunk of memory.
 *
 * In a traditional production buffer pool manager, all memory that the buffer pool is intended to manage is allocated
 * in one large contiguous array (think of a very large `malloc` call that allocates several gigabytes of memory up
 * front). This large contiguous block of memory is then divided into contiguous frames. In other words, frames are
 * defined by an offset from the base of the array in page-sized (4 KB) intervals.
 *
 * In BusTub, we instead allocate each frame on its own (via a `std::vector<char>`) in order to easily detect buffer
 * overflow with address sanitizer. Since C++ has no notion of memory safety, it would be very easy to cast a page's
 * data pointer into some large data type and start overwriting other pages of data if they were all contiguous.
 *
 * If you would like to attempt to use more efficient data structures for your buffer pool manager, you are free to do
 * so. However, you will likely benefit significantly from detecting buffer overflow in future projects (especially
 * project 2).
 *
 * @brief 辅助类，为 `BufferPoolManager` 管理单个帧的内存及相关元数据。
 *
 * 该类表示缓冲池管理器用于存储页面数据的帧的头信息。注意，实际的内存帧并不直接保存在 `FrameHeader` 中，
 * 而是 `FrameHeader` 存储对帧数据的指针，并单独存放。
 *
 * ---
 *
 * 你可能会关心为什么字段 `data_` 使用按需分配的 `std::vector`，而不是指向预先分配的一大块内存的指针。
 *
 * 在传统生产环境的缓冲池中，通常会在一大块连续内存中（例如一次很大的 `malloc`）分配缓冲池所需的所有内存，
 * 然后将这块内存切分为多个帧（每帧大小为页面大小，如 4 KB）。帧的位置由基地址的偏移量确定。
 *
 * 在 BusTub 中，我们选择为每个帧单独分配（通过 `std::vector<char>`），以便更容易使用地址消毒器检测缓冲区溢出。
 * 由于 C++ 没有内存安全保障，如果所有页面都是连续的，很容易将页面数据指针转换为更大类型并覆盖其他页面数据。
 *
 * 如果你想尝试更高效的数据结构以实现缓冲池，也可以这样做。但使用按帧分配有助于在后续项目中（尤其是项目 2）检测溢出。
 */
class FrameHeader {
  friend class BufferPoolManager;
  friend class ReadPageGuard;
  friend class WritePageGuard;

 public:
  explicit FrameHeader(frame_id_t frame_id);

 private:
  //只读访问
  auto GetData() const -> const char *;
  //可变访问
  auto GetDataMut() -> char *;
  void Reset();

  /** @brief The frame ID / index of the frame this header represents. */
  /** @brief 此头信息所代表的帧的帧 ID / 索引。 */
  const frame_id_t frame_id_;

  /** @brief The readers / writer latch for this frame. */
  /** @brief 此帧的读写锁（读者/写者锁）。 */
  std::shared_mutex rwlatch_;

  /** @brief The number of pins on this frame keeping the page in memory. */
  /** @brief 该帧上保持页面在内存中的 pin 计数。 */
  std::atomic<size_t> pin_count_;

  /** @brief The dirty flag. */
  /** @brief 脏标记（页面是否被修改）。 */
  bool is_dirty_;

  /**
   * @brief A pointer to the data of the page that this frame holds.
   *
   * If the frame does not hold any page data, the frame contains all null bytes.
   */
  /**
   * @brief 指向该帧所保存页面数据的指针。
   *
   * 如果该帧不包含任何页面数据，则其全部字节为 0（空字节）。
   */
  std::vector<char> data_;

  /**
   * TODO(P1): You may add any fields or helper functions under here that you think are necessary.
   *
   * One potential optimization you could make is storing an optional page ID of the page that the `FrameHeader` is
   * currently storing. This might allow you to skip searching for the corresponding (page ID, frame ID) pair somewhere
   * else in the buffer pool manager...
   */
  /**
   * TODO(P1)：你可以在此处添加任何认为必要的字段或辅助函数。
   *
   * 一个可选的优化是记录 `FrameHeader` 当前存储的页面 ID（可选类型），
   * 这样可以在某些情况下避免在缓冲池管理器的其他结构中查找 (page ID, frame ID) 对。
   */
   auto GetLatch() -> std::shared_mutex & {
    return rwlatch_;
}
    //可以增加一个page_id,直接从 frame_header 里拿到 page_id,而不需要遍历

};

/**
 * @brief The declaration of the `BufferPoolManager` class.
 *
 * As stated in the writeup, the buffer pool is responsible for moving physical pages of data back and forth from
 * buffers in main memory to persistent storage. It also behaves as a cache, keeping frequently used pages in memory for
 * faster access, and evicting unused or cold pages back out to storage.
 *
 * Make sure you read the writeup in its entirety before attempting to implement the buffer pool manager. You also need
 * to have completed the implementation of both the `LRUKReplacer` and `DiskManager` classes.
 *
 * @brief `BufferPoolManager` 类的声明。
 *
 * 如项目说明所述，缓冲池负责在主内存缓冲区和持久化存储之间移动物理页面数据。同时它也充当缓存，
 * 将频繁使用的页面保存在内存中以提高访问速度，并将不常用或冷页面驱逐回存储。
 *
 * 在实现缓冲池管理器之前，请务必完整阅读项目说明。此外，你还需要完成 `LRUKReplacer` 和 `DiskManager` 的实现。
 */
class BufferPoolManager {
 public:
  BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist = LRUK_REPLACER_K,
                    LogManager *log_manager = nullptr);
  ~BufferPoolManager();

  auto Size() const -> size_t;
  auto NewPage() -> page_id_t;
  auto DeletePage(page_id_t page_id) -> bool;
  auto CheckedWritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown)
      -> std::optional<WritePageGuard>;
  auto CheckedReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> std::optional<ReadPageGuard>;
  auto WritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> WritePageGuard;
  auto ReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> ReadPageGuard;
  auto FlushPage(page_id_t page_id) -> bool;
  void FlushAllPages();
  auto GetPinCount(page_id_t page_id) -> std::optional<size_t>;
  auto PinPage(frame_id_t frame_id) -> void;
  //auto UnpinPage(frame_id_t frame_id) -> void;
  auto ReadPageFromDisk(page_id_t page_id, std::shared_ptr<FrameHeader> frame) -> void;

 private:
  /** @brief The number of frames in the buffer pool. */
  /** @brief 缓冲池中的帧数量。 */
  const size_t num_frames_;

  /** @brief The next page ID to be allocated.  */
  /** @brief 下一个要分配的页面 ID。 */
  std::atomic<page_id_t> next_page_id_;

  /**
   * @brief The latch protecting the buffer pool's inner data structures.
   *
   * TODO(P1) We recommend replacing this comment with details about what this latch actually protects.
   */
  /**
   * @brief 保护缓冲池内部数据结构的互斥锁（latch）。
   *
   * TODO(P1)：建议将此注释替换为更具体的说明，描述该锁实际保护的内容范围。
   *bpm_latch_实际是一个shared_ptr对象，它是一个指向互斥锁的智能指针，用于保护缓冲池管理器的内部数据结构，
   * 确保在多线程环境下对这些数据结构的访问是线程安全的。
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /** @brief The frame headers of the frames that this buffer pool manages. */
  /** @brief 缓冲池所管理的帧对应的帧头列表。 */
  std::vector<std::shared_ptr<FrameHeader>> frames_;

  /** @brief The page table that keeps track of the mapping between pages and buffer pool frames. */
  /** @brief 跟踪页面与缓冲池帧之间映射关系的页表。 */
  std::unordered_map<page_id_t, frame_id_t> page_table_;

  /** @brief A list of free frames that do not hold any page's data. */
  /** @brief 不包含任何页面数据的空闲帧列表。 */
  std::list<frame_id_t> free_frames_;

  /** @brief The replacer to find unpinned / candidate pages for eviction. */
  /** @brief 用于找到未被固定（unpinned）的或作为驱逐候选的页面的替换器（replacer）。 */
  std::shared_ptr<LRUKReplacer> replacer_;

  /** @brief A pointer to the disk scheduler. */
  /** @brief 指向磁盘调度器的指针。 */
  std::unique_ptr<DiskScheduler> disk_scheduler_;

  /**
   * @brief A pointer to the log manager.
   *
   * Note: Please ignore this for P1.
   */
  /** @brief 指向日志管理器的指针（P1 可忽略）。 */
  LogManager *log_manager_ __attribute__((__unused__));

  /**
   * TODO(P1): You may add additional private members and helper functions if you find them necessary.
   *
   * There will likely be a lot of code duplication between the different modes of accessing a page.
   *
   * We would recommend implementing a helper function that returns the ID of a frame that is free and has nothing
   * stored inside of it. Additionally, you may also want to implement a helper function that returns either a shared
   * pointer to a `FrameHeader` that already has a page's data stored inside of it, or an index to said `FrameHeader`.
   */
  /**
   * TODO(P1)：如果需要，你可以添加额外的私有成员和辅助函数。
   *
   * 在不同的页面访问模式之间很可能会出现大量代码重复。
   *
   * 我们建议实现一个辅助函数，用于返回一个空闲且不包含任何数据的帧的 ID。此外，你还可以实现一个辅助函数，
   * 返回已经包含某页面数据的 `FrameHeader` 的共享指针或该 `FrameHeader` 的索引，以便重用。
   */
};
}  // namespace bustub
