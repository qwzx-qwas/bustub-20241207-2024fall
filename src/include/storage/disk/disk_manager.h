//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_manager.h
//
// Identification: src/include/storage/disk/disk_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <filesystem>
#include <fstream>
#include <future>  // NOLINT
#include <mutex>   // NOLINT
#include <string>

#include "common/config.h"

namespace bustub {

/**
 * DiskManager takes care of the allocation and deallocation of pages within a database. It performs the reading and
 * writing of pages to and from disk, providing a logical file layer within the context of a database management system.
 */
/**
 * DiskManager 负责数据库中页面的分配和释放。它执行页面在磁盘与内存之间的读写，
 * 在数据库管理系统中提供一个逻辑文件层。
 */
class DiskManager {
 public:
  /**
   * Creates a new disk manager that writes to the specified database file.
   * @param db_file the file name of the database file to write to
   */
  explicit DiskManager(const std::filesystem::path &db_file);

  /**
   * 创建一个新的 DiskManager，向指定的数据库文件写入数据。
   * @param db_file 要写入的数据库文件路径
   */

  /** FOR TEST / LEADERBOARD ONLY, used by DiskManagerMemory */
  /** 仅用于测试/排行榜，由 DiskManagerMemory 使用 */
  DiskManager() = default;

  virtual ~DiskManager() = default;

  /**
   * Shut down the disk manager and close all the file resources.
   */
  void ShutDown();

  /**
   * 关闭磁盘管理器并释放所有文件资源。
   */

  /**
   * @brief Increases the size of the database file.
   *
   * This function works like a dynamic array, where the capacity is doubled until all pages can fit.
   *
   * @param pages The number of pages the caller wants the file used for storage to support.
   */
  virtual void IncreaseDiskSpace(size_t pages);

  /**
   * 增加数据库文件的大小。
   *
   * 该函数类似动态数组的行为，文件容量会翻倍，直到能够容纳所有页面为止。
   *
   * @param pages 调用方希望文件支持的页面数量。
   */

  /**
   * Write a page to the database file.
   * @param page_id id of the page
   * @param page_data raw page data
   */
  virtual void WritePage(page_id_t page_id, const char *page_data);

  /**
   * 将一个页面写入数据库文件。
   * @param page_id 页面 ID
   * @param page_data 页面原始数据
   */

  /**
   * Read a page from the database file.
   * @param page_id id of the page
   * @param[out] page_data output buffer
   */
  virtual void ReadPage(page_id_t page_id, char *page_data);

  /**
   * 从数据库文件中读取一个页面。
   * @param page_id 页面 ID
   * @param[out] page_data 输出缓冲区
   */

  /**
   * Delete a page from the database file. Reclaim the disk space.
   * @param page_id id of the page
   */
  virtual void DeletePage(page_id_t page_id);

  /**
   * 从数据库文件中删除一个页面，回收磁盘空间。
   * @param page_id 页面 ID
   */

  /**
   * Flush the entire log buffer into disk.
   * @param log_data raw log data
   * @param size size of log entry
   */
  void WriteLog(char *log_data, int size);

  /**
   * 将整个日志缓冲区刷入磁盘。
   * @param log_data 日志原始数据
   * @param size 日志条目的大小
   */

  /**
   * Read a log entry from the log file.
   * @param[out] log_data output buffer
   * @param size size of the log entry
   * @param offset offset of the log entry in the file
   * @return true if the read was successful, false otherwise
   */
  auto ReadLog(char *log_data, int size, int offset) -> bool;

  /**
   * 从日志文件中读取一条日志记录。
   * @param[out] log_data 输出缓冲区
   * @param size 日志条目的大小
   * @param offset 日志条目在文件中的偏移量
   * @return 如果读取成功返回 true，否则返回 false
   */

  /** @return the number of disk flushes */
  /** @return 磁盘 flush 的次数 */
  auto GetNumFlushes() const -> int;

  /** @return true iff the in-memory content has not been flushed yet */
  /** @return 如果内存内容尚未被刷写返回 true */
  auto GetFlushState() const -> bool;

  /** @return the number of disk writes */
  /** @return 磁盘写入的次数 */
  auto GetNumWrites() const -> int;

  /** @return the number of deletions */
  /** @return 删除操作的次数 */
  auto GetNumDeletes() const -> int;

  /**
   * Sets the future which is used to check for non-blocking flushes.
   * @param f the non-blocking flush check
   */
  inline void SetFlushLogFuture(std::future<void> *f) { flush_log_f_ = f; }

  /**
   * 设置用于检查非阻塞刷写的 future。
   * @param f 非阻塞刷写检查的 future
   */

  /** Checks if the non-blocking flush future was set. */
  /** 检查是否已设置非阻塞刷写的 future。 */
  inline auto HasFlushLogFuture() -> bool { return flush_log_f_ != nullptr; }

  /** @brief returns the log file name */
  inline auto GetLogFileName() const -> std::filesystem::path { return log_name_; }
  /** @brief 返回日志文件名 */

 protected:
  auto GetFileSize(const std::string &file_name) -> int;
  // stream to write log file
  // 用于写入日志文件的流
  std::fstream log_io_;
  std::filesystem::path log_name_;
  // stream to write db file
  // 用于写入数据库文件的流
  std::fstream db_io_;
  std::filesystem::path file_name_;
  int num_flushes_{0};
  int num_writes_{0};
  int num_deletes_{0};
  bool flush_log_{false};
  std::future<void> *flush_log_f_{nullptr};
  // With multiple buffer pool instances, need to protect file access
  // 当有多个 buffer pool 实例时，需要保护文件访问
  std::mutex db_io_latch_;

  /** @brief The number of pages allocated to the DBMS on disk. */
  /** @brief 分配给 DBMS 的磁盘页面数量。 */
  size_t pages_{0};
  /** @brief The capacity of the file used for storage on disk. */
  /** @brief 用于存储的磁盘文件的容量。 */
  size_t page_capacity_{DEFAULT_DB_IO_SIZE};
};

}  // namespace bustub
