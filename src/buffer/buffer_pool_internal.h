#pragma once
#include <condition_variable>
#include <atomic>
#include <array>
#include <mutex>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "buffer/clock_pro_replacer.h"
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
enum class FramePhase { Free, Loading, Resident, Evicting, Failed };
struct IndexBody {
  FrameMemory memory_{};
  page_id_t page_{INVALID_PAGE_ID};
};
struct alignas(64) ShortReader {
  explicit ShortReader(ResourceCharge charge) : charge_(std::move(charge)) {}
  ResourceCharge charge_;
  std::atomic<bool> claimed_{true};
  std::atomic<IndexBody *> body_{nullptr};
  ShortReader *next_{nullptr};
};
struct FrameHeader {
  FrameHeader(frame_id_t id, FrameMemory memory) : id_(id), memory_(memory) { bodies_[0].memory_ = memory; }
  std::mutex mutex_;
  std::condition_variable changed_;
  std::array<IndexBody, 2> bodies_;
  std::unique_ptr<FrameArena> spare_;
  unsigned body_slot_{0};
  bool index_page_{false};
  std::atomic<IndexBody *> published_{nullptr};
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
  BufferPoolState(size_t frames, std::shared_ptr<PageStorage> storage, BufferPoolOptions options);
  // Lifecycle, free-frame list and task budget only. Residency and unpin use
  // entry -> per-frame synchronization, never this lock across IO/content waits.
  std::mutex mutex_;
  std::condition_variable changed_;
  bool closed_{false};
  size_t calls_{0}, tasks_{0};
  uint64_t next_page_{0};
  const size_t max_tasks_;
  const size_t max_prefetch_;
  size_t prefetch_{0};
  std::shared_ptr<PageStorage> storage_;
  std::shared_ptr<ResourceAccount> capture_memory_;
  FrameArena arena_;
  TranslationDirectory directory_;
  ClockProReplacer replacer_;
  ResourceCharge short_metadata_;
  std::vector<std::unique_ptr<FrameHeader>> frames_;
  std::vector<frame_id_t> free_;  // Reserved once; retiring a frame cannot allocate.
  struct Slot {
    explicit Slot(BufferPoolState &s, bool prefetch) : state_(s), prefetch_(prefetch) {}
    ~Slot();
    BufferPoolState &state_;
    bool prefetch_;
  };
  const uint64_t short_identity_;
  std::atomic<bool> short_closed_{false};
  std::atomic<ShortReader *> short_readers_{nullptr};
  std::mutex short_mutex_;
  std::vector<std::unique_ptr<ShortReader>> short_owned_;
  FrameArenaOptions spare_options_;
  size_t spare_bytes_{0};
  const size_t spare_limit_;
  auto ClaimShortReader() -> ShortReader *;
  auto BodyHeld(IndexBody *body) const -> bool;
  void PrivateBody(FrameHeader &frame);
  void DrainBody(FrameHeader &frame, bool release_spare = true);
  void PublishBody(FrameHeader &frame);
  auto TrySlot(bool prefetch) -> std::unique_ptr<Slot>;
  struct FramePreparation {
    FrameHeader *frame_;
    bool retry_;
  };
  auto PrepareFrame(page_id_t page, AccessType type, bool prefetch, const std::shared_ptr<PageTask> &loading)
      -> FramePreparation;
  void CompletePrefetch(FrameHeader &frame, const std::shared_ptr<PageTask> &task, std::exception_ptr error);
  void AbandonPrefetch(FrameHeader &frame, const std::shared_ptr<PageTask> &task);
  void Free(frame_id_t frame);
  void Unpin(FrameHeader &f);
  auto Buffer(FrameHeader &f) -> PageBuffer;
  auto Fetch(page_id_t page, bool write, AccessType type) -> FrameHeader *;
  void Flush(const std::vector<std::pair<page_id_t, uint64_t>> &pages);
};
}  // namespace bustub
