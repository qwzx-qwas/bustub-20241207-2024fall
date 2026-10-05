// ===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
// ===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// Array-resident page cache. No lock spans backend IO or content waits.
//===----------------------------------------------------------------------===//
#pragma once
#include <cstddef>
#include <memory>
#include <optional>
#include "buffer/lru_k_replacer.h"
#include "buffer/page_storage.h"
#include "common/config.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page_guard.h"
namespace bustub {
class DiskManager;
class LogManager;
struct BufferPoolState;
struct BufferPoolOptions {
  size_t directory_bytes_;
  size_t arena_bytes_;
  size_t max_inflight_pages_;
  bool advise_huge_pages_;
  size_t prefetch_pages_;  // Explicit quota/window; zero disables speculative IO.
};
class BufferPoolManager {
 public:
  using ReadGuard = ReadPageGuard;
  using WriteGuard = WritePageGuard;
  BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist = LRUK_REPLACER_K,
                    LogManager *log_manager = nullptr);
  BufferPoolManager(size_t num_frames, std::shared_ptr<PageStorage> storage, BufferPoolOptions options,
                    size_t k_dist = LRUK_REPLACER_K);
  ~BufferPoolManager();
  auto Size() const -> size_t;
  auto NewPage() -> page_id_t;
  void SetNextPageIdForRecovery(page_id_t next_page_id);
  auto DeletePage(page_id_t page_id) -> bool;
  auto SupportsPageRetirement() const -> bool;
  /** Caller has removed record/index/undo references. False means another pin
   * or IO still owns the page. On success consumes victim and publishes the
   * durable replacement image into link. Failure fences this cache until reopen. */
  auto RetirePage(ReadPageGuard &victim, WritePageGuard &link, const std::array<char, BUSTUB_PAGE_SIZE> &replacement)
      -> bool;
  void ReclaimRetiredPages();
  auto CheckedWritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown)
      -> std::optional<WritePageGuard>;
  auto CheckedReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> std::optional<ReadPageGuard>;
  auto WritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> WritePageGuard;
  auto ReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> ReadPageGuard;
  auto FlushPage(page_id_t page_id) -> bool;
  void FlushAllPages();
  /** Bounded hints from an existing query RID window. Never waits for device IO. */
  void PrefetchPages(const std::vector<page_id_t> &pages);
  auto PrefetchWindow() const -> size_t;
  auto GetPinCount(page_id_t page_id) -> std::optional<size_t>;
  /** Stop new calls and drain accepted work. Existing guards retain the old
   * arena; no automatic flush/checkpoint. Owner must not destroy during a call. */
  void Close();

 private:
  std::shared_ptr<BufferPoolState> state_;
};
}  // namespace bustub
