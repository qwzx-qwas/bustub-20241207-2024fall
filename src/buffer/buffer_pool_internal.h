#pragma once
#include <condition_variable>
#include <list>
#include <mutex>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "buffer/frame_arena.h"
#include "buffer/translation_directory.h"
namespace bustub {
struct PageTask {
  std::mutex mutex_;
  std::condition_variable cv_;
  bool done_{false};
  std::exception_ptr error_;
  void Finish(std::exception_ptr error = nullptr) {
    std::lock_guard<std::mutex> lock(mutex_);
    error_ = error;
    done_ = true;
    cv_.notify_all();
  }
  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&] { return done_; });
    if (error_) {
      std::rethrow_exception(error_);
    }
  }
};
enum class FramePhase { Free, Loading, Resident, Evicting };
struct FrameHeader {
  FrameHeader(frame_id_t id, FrameMemory memory) : id_(id), memory_(memory) {}
  std::mutex mutex_;
  std::condition_variable changed_;
  frame_id_t id_;
  FrameMemory memory_;
  page_id_t page_{INVALID_PAGE_ID};
  uint64_t generation_{0};
  uint64_t dirty_version_{0}, clean_version_{0};
  size_t pins_{0}, readers_{0};
  bool writer_{false}, io_{false};
  FramePhase phase_{FramePhase::Free};
  std::shared_ptr<PageTask> task_;
};
struct BufferPoolState : std::enable_shared_from_this<BufferPoolState> {
  BufferPoolState(size_t frames, std::shared_ptr<PageStorage> storage, BufferPoolOptions options, size_t k);
  // Lifecycle, free-frame list and task budget only. Residency and unpin use
  // entry -> per-frame synchronization, never this lock across IO/content waits.
  std::mutex mutex_;
  std::condition_variable changed_;
  bool closed_{false};
  size_t calls_{0}, tasks_{0};
  uint64_t next_page_{0};
  const size_t max_tasks_;
  std::shared_ptr<PageStorage> storage_;
  FrameArena arena_;
  TranslationDirectory directory_;
  LRUKReplacer replacer_;
  std::vector<std::unique_ptr<FrameHeader>> frames_;
  std::list<frame_id_t> free_;
  struct Slot {
    explicit Slot(BufferPoolState &s) : state_(s) {}
    ~Slot();
    BufferPoolState &state_;
  };
  auto TrySlot() -> std::unique_ptr<Slot>;
  void Free(frame_id_t frame);
  void Unpin(FrameHeader &f);
  auto Buffer(FrameHeader &f) -> PageBuffer;
  auto Fetch(page_id_t page, bool write, AccessType type) -> FrameHeader *;
  void Flush(const std::vector<std::pair<page_id_t, uint64_t>> &pages);
};
}  // namespace bustub
