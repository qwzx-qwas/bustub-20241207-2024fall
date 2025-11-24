//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.h
//
// Identification: src/include/storage/disk/disk_scheduler.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <future>  // NOLINT
#include <optional>
#include <thread>  // NOLINT

#include "common/channel.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * @brief Represents a Write or Read request for the DiskManager to execute.
 * @brief 表示 DiskManager 要执行的写或读请求。
 */
struct DiskRequest {
  /** Flag indicating whether the request is a write or a read. */
  /** 标志，指示请求是写操作还是读操作。 */
  bool is_write_;

  /**
   *  Pointer to the start of the memory location where a page is either:
   *   1. being read into from disk (on a read).
   *   2. being written out to disk (on a write).
   *  
   *  指向页面在内存中起始位置的指针，页面数据用于以下两种情况：
   *   1. 在读操作时从磁盘读入内存。
   *   2. 在写操作时从内存写回磁盘。
   */
  char *data_;

  /** ID of the page being read from / written to disk. */
  /** 要从磁盘读取或写入到磁盘的页面的 ID。 */
  page_id_t page_id_;

  /** Callback used to signal to the request issuer when the request has been completed. */
  /** 回调，用于在请求完成时通知请求发起者（返回结果）。 */
  std::promise<bool> callback_;
};

/**
 * @brief The DiskScheduler schedules disk read and write operations.
 * @brief DiskScheduler 负责调度磁盘的读写操作。
 *
 * A request is scheduled by calling DiskScheduler::Schedule() with an appropriate DiskRequest object. The scheduler
 * maintains a background worker thread that processes the scheduled requests using the disk manager. The background
 * thread is created in the DiskScheduler constructor and joined in its destructor.
 *
 * 通过调用 `DiskScheduler::Schedule()` 并传入相应的 `DiskRequest` 对象来调度请求。
 * 该调度器维护一个后台工作线程，使用 `DiskManager` 处理已调度的请求。后台线程在 `DiskScheduler`
 * 构造函数中创建，并在析构函数中 join。
 */
class DiskScheduler {
 public:
  explicit DiskScheduler(DiskManager *disk_manager);
  ~DiskScheduler();

  /**
  * TODO(P1): Add implementation
  *
  * @brief Schedules a request for the DiskManager to execute.
  * @brief 将请求调度给 DiskManager 执行。
  *
  * @param r The request to be scheduled.
   */
  void Schedule(DiskRequest r);

  /**
  * TODO(P1): Add implementation
  *
  * @brief Background worker thread function that processes scheduled requests.
  * @brief 后台工作线程函数，用于处理已调度的请求。
  *
  * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
  * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
  *
  * 在 `DiskScheduler` 存在期间后台线程需要持续处理请求；也就是说，该函数在 `~DiskScheduler()` 被调用
  * 之前不应返回。到析构时需要确保该函数能够正常返回以便线程退出。
   */
  void StartWorkerThread();

  using DiskSchedulerPromise = std::promise<bool>;

  /**
   * @brief Create a Promise object. If you want to implement your own version of promise, you can change this function
   * so that our test cases can use your promise implementation.
   *
   * @return std::promise<bool>
   */
  auto CreatePromise() -> DiskSchedulerPromise { return {}; };
  /**
   * @brief 创建一个 Promise 对象。如果你想实现自定义的 promise 实现，可以修改此函数，
   * 使测试用例能够使用你的实现。
   *
   * @return std::promise<bool>
   */

  /**
   * @brief Increases the size of the database file to fit the specified number of pages.
   *
   * This function works like a dynamic array, where the capacity is doubled until all pages can fit.
   *
   * @param pages The number of pages the caller wants the file used for storage to support.
   */
   //确保磁盘文件的大小足以容纳至少 pages 个页面
  void IncreaseDiskSpace(size_t pages) { disk_manager_->IncreaseDiskSpace(pages); }

  /**
   * @brief 增加数据库文件的大小以容纳指定数量的页面。
   *
   * 该函数的行为类似于动态数组，容量会不断加倍直到足够容纳所有页面。
   *
   * @param pages 调用方希望文件支持的页面数量。
   */

  /**
   * @brief Deallocates a page on disk.
   *
   * Note: You should look at the documentation for `DeletePage` in `BufferPoolManager` before using this method.
   * Also note: This is a no-op without a more complex data structure to track deallocated pages.
   *
   * @param page_id The page ID of the page to deallocate from disk.
   */
  void DeallocatePage(page_id_t page_id) { disk_manager_->DeletePage(page_id); }

  /**
   * @brief 在磁盘上释放一个页面。
   *
   * 注意：在使用此方法之前应查看 `BufferPoolManager` 中 `DeletePage` 的文档。
   * 另外，如果没有更复杂的数据结构来跟踪已释放的页面，则此操作为无效（无效果）。
   *
   * @param page_id 要从磁盘上释放的页面 ID。
   */

 private:
  /** Pointer to the disk manager. */
  /** 指向磁盘管理器的指针。 */
  DiskManager *disk_manager_ __attribute__((__unused__));
  /** A shared queue to concurrently schedule and process requests. When the DiskScheduler's destructor is called,
   * `std::nullopt` is put into the queue to signal to the background thread to stop execution. */
  /**
   * 一个用于并发调度和处理请求的共享队列。当 `DiskScheduler` 析构时，
   * 会将 `std::nullopt` 放入队列以通知后台线程停止执行。
   */
  Channel<std::optional<DiskRequest>> request_queue_;
  /** The background thread responsible for issuing scheduled requests to the disk manager. */
  /** 负责向磁盘管理器发出已调度请求的后台线程。 */
  std::optional<std::thread> background_thread_;
};
}  // namespace bustub
