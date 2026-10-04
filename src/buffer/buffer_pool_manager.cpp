// ===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
// ===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <algorithm>
#include <limits>
#include <stdexcept>
#include "buffer_pool_internal.h"
#include "common/exception.h"
namespace bustub {
namespace {
class Call {
 public:
  explicit Call(BufferPoolState &s) : s_(s) {
    std::lock_guard<std::mutex> lock(s_.mutex_);
    if (s_.closed_) {
      throw std::runtime_error("buffer pool is closed");
    }
    ++s_.calls_;
  }
  ~Call() {
    std::lock_guard<std::mutex> lock(s_.mutex_);
    --s_.calls_;
    s_.changed_.notify_all();
  }

 private:
  BufferPoolState &s_;
};
void PageNumber(page_id_t p) {
  if (p < 0) {
    throw std::invalid_argument("negative database page");
  }
}
// Compatibility deployment budget; object deployments supply explicit budgets.
auto FileOptions(size_t frames) -> BufferPoolOptions {
  return {std::max<size_t>(4U << 20U, frames * 8192), frames * BUSTUB_PAGE_SIZE + (2U << 20U), frames, false};
}
}  // namespace
BufferPoolState::BufferPoolState(size_t count, std::shared_ptr<PageStorage> storage, BufferPoolOptions options,
                                 size_t k)
    : max_tasks_(options.max_inflight_pages_),
      storage_(std::move(storage)),
      arena_({count, BUSTUB_PAGE_SIZE, storage_->MemoryAlignment(), options.arena_bytes_, options.advise_huge_pages_}),
      directory_({options.directory_bytes_}),
      replacer_(count, k) {
  if (max_tasks_ == 0 || storage_->MaxBatchPages() == 0) {
    throw std::invalid_argument("no page task capacity");
  }
  frames_.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    frames_.push_back(std::make_unique<FrameHeader>(static_cast<frame_id_t>(i), arena_.Frame(i)));
    free_.push_back(i);
  }
}
void BufferPoolState::Unpin(FrameHeader &f) {
  if (f.pins_ == 0) {
    std::terminate();
  }  // Internal invariant, never hide double release.
  --f.pins_;
  if (f.pins_ == 0 && f.phase_ == FramePhase::Resident) {
    replacer_.SetEvictable(f.id_, true);
  }
  f.changed_.notify_all();
}
auto BufferPoolState::Buffer(FrameHeader &f) -> PageBuffer {
  return {f.page_, f.memory_.data_, f.memory_.capacity_, shared_from_this()};
}
BufferPoolState::Slot::~Slot() {
  std::lock_guard<std::mutex> lock(state_.mutex_);
  --state_.tasks_;
  state_.changed_.notify_all();
}
auto BufferPoolState::TrySlot() -> std::unique_ptr<Slot> {
  std::lock_guard<std::mutex> lock(mutex_);
  if (tasks_ == max_tasks_) {
    return nullptr;
  }
  auto slot = std::make_unique<Slot>(*this);
  ++tasks_;
  return slot;
}
void BufferPoolState::Free(frame_id_t frame) {
  std::lock_guard<std::mutex> lock(mutex_);
  free_.push_back(frame);
}
auto BufferPoolState::Fetch(page_id_t page, bool write, AccessType type) -> FrameHeader * {
  PageNumber(page);
  for (;;) {
    FrameHeader *hit = nullptr;
    std::shared_ptr<PageTask> waiting;
    std::unique_lock<std::mutex> content;
    {
      auto entry = directory_.Access(page);
      if (auto id = entry.Frame()) {
        hit = frames_[*id].get();
        content = std::unique_lock<std::mutex>(hit->mutex_);
        if (hit->phase_ == FramePhase::Loading || hit->phase_ == FramePhase::Evicting) {
          waiting = hit->task_;
        } else {
          ++hit->pins_;
          replacer_.RecordAccess(hit->id_, type);
          replacer_.SetEvictable(hit->id_, false);
        }
      }
    }
    if (waiting) {
      content.unlock();
      waiting->Wait();
      continue;
    }
    if (hit) {
      hit->changed_.wait(content, [&] { return !hit->writer_ && (!write || (hit->readers_ == 0 && !hit->io_)); });
      if (write) {
        hit->writer_ = true;
      } else {
        ++hit->readers_;
      }
      return hit;
    }
    auto slot = TrySlot();
    if (!slot) {
      return nullptr;
    }
    auto loading = std::make_shared<PageTask>();
    auto evicting = std::make_shared<PageTask>();
    std::optional<frame_id_t> selected;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!free_.empty()) {
        selected = free_.front();
        free_.pop_front();
      }
    }
    bool victim = false;
    if (!selected) {
      selected = replacer_.Candidate();
      victim = selected.has_value();
    }
    if (!selected) {
      return nullptr;
    }
    auto &f = *frames_[*selected];
    if (victim) {
      page_id_t previous;
      {
        std::lock_guard<std::mutex> lock(f.mutex_);
        previous = f.page_;
      }
      if (previous == INVALID_PAGE_ID) {
        continue;
      }  // Another selector retired this candidate.
      bool dirty;
      {
        auto entry = directory_.Access(previous);
        std::lock_guard<std::mutex> lock(f.mutex_);
        if (entry.Frame() != selected || f.phase_ != FramePhase::Resident || f.pins_ != 0) {
          continue;
        }
        replacer_.Remove(f.id_);
        f.phase_ = FramePhase::Evicting;
        f.io_ = true;
        f.task_ = evicting;
        dirty = f.dirty_version_ != f.clean_version_;
      }
      try {
        if (dirty) {
          storage_->Write({Buffer(f)});
        }
      } catch (...) {
        {
          std::lock_guard<std::mutex> lock(f.mutex_);
          f.phase_ = FramePhase::Resident;
          f.io_ = false;
          f.task_.reset();
          replacer_.RecordAccess(f.id_, type);
          replacer_.SetEvictable(f.id_, true);
          f.changed_.notify_all();
        }
        evicting->Finish(std::current_exception());
        throw;
      }
      {
        auto entry = directory_.Access(previous);
        std::lock_guard<std::mutex> lock(f.mutex_);
        entry.SetFrame(std::nullopt);
        f.page_ = INVALID_PAGE_ID;
        f.phase_ = FramePhase::Free;
        f.io_ = false;
        f.task_.reset();
        f.changed_.notify_all();
      }
      evicting->Finish();
    }
    if (f.generation_ == std::numeric_limits<uint64_t>::max()) {
      Free(f.id_);
      std::lock_guard<std::mutex> lock(mutex_);
      closed_ = true;
      throw std::overflow_error("frame reuse identity exhausted");
    }
    bool lost;
    {
      auto entry = directory_.Access(page);
      std::lock_guard<std::mutex> lock(f.mutex_);
      lost = entry.Frame().has_value();
      if (!lost) {
        f.page_ = page;
        ++f.generation_;
        f.dirty_version_ = f.clean_version_ = 0;
        f.phase_ = FramePhase::Loading;
        f.io_ = true;
        f.task_ = loading;
        entry.SetFrame(f.id_);
      }
    }
    if (lost) {
      Free(f.id_);
      continue;
    }
    std::exception_ptr error;
    try {
      storage_->Read(Buffer(f));
    } catch (...) {
      error = std::current_exception();
    }
    if (error) {
      {
        auto entry = directory_.Access(page);
        std::lock_guard<std::mutex> lock(f.mutex_);
        entry.SetFrame(std::nullopt);
        f.phase_ = FramePhase::Free;
        f.page_ = INVALID_PAGE_ID;
        f.io_ = false;
        f.task_.reset();
        f.changed_.notify_all();
      }
      Free(f.id_);
      loading->Finish(error);
      std::rethrow_exception(error);
    }
    {
      std::lock_guard<std::mutex> lock(f.mutex_);
      f.phase_ = FramePhase::Resident;
      f.pins_ = 1;
      f.io_ = false;
      f.task_.reset();
      if (write) {
        f.writer_ = true;
      } else {
        f.readers_ = 1;
      }
      replacer_.RecordAccess(f.id_, type);
      replacer_.SetEvictable(f.id_, false);
      f.changed_.notify_all();
    }
    loading->Finish();
    return &f;
  }
}
BufferPoolManager::BufferPoolManager(size_t n, DiskManager *disk, size_t k, LogManager *log)
    : BufferPoolManager(n, FilePageStorage(disk), FileOptions(n), k) {
  (void)log;
}
BufferPoolManager::BufferPoolManager(size_t n, std::shared_ptr<PageStorage> storage, BufferPoolOptions options,
                                     size_t k)
    : state_(std::make_shared<BufferPoolState>(n, std::move(storage), options, k)) {}
BufferPoolManager::~BufferPoolManager() { Close(); }
auto BufferPoolManager::Size() const -> size_t { return state_->frames_.size(); }
void BufferPoolManager::Close() {
  std::unique_lock<std::mutex> lock(state_->mutex_);
  state_->closed_ = true;
  state_->changed_.wait(lock, [&] { return state_->calls_ == 0; });
}
auto BufferPoolManager::NewPage() -> page_id_t {
  Call call(*state_);
  uint64_t page;
  {
    std::lock_guard<std::mutex> lock(state_->mutex_);
    page = state_->next_page_;
    if (page > static_cast<uint64_t>(std::numeric_limits<page_id_t>::max())) {
      throw std::overflow_error("database page identity exhausted");
    }
    ++state_->next_page_;
  }
  state_->storage_->EnsurePages(page + 1);
  return page;
}
void BufferPoolManager::SetNextPageIdForRecovery(page_id_t next) {
  PageNumber(next);
  Call call(*state_);
  uint64_t count;
  {
    std::lock_guard<std::mutex> lock(state_->mutex_);
    state_->next_page_ = std::max(state_->next_page_, static_cast<uint64_t>(next));
    count = state_->next_page_;
  }
  state_->storage_->EnsurePages(count);
}
auto BufferPoolManager::CheckedReadPage(page_id_t page, AccessType type) -> std::optional<ReadPageGuard> {
  Call call(*state_);
  auto *f = state_->Fetch(page, false, type);
  if (!f) {
    return std::nullopt;
  }
  return ReadPageGuard(state_, f);
}
auto BufferPoolManager::CheckedWritePage(page_id_t page, AccessType type) -> std::optional<WritePageGuard> {
  Call call(*state_);
  auto *f = state_->Fetch(page, true, type);
  if (!f) {
    return std::nullopt;
  }
  return WritePageGuard(state_, f);
}
auto BufferPoolManager::ReadPage(page_id_t page, AccessType type) -> ReadPageGuard {
  auto guard = CheckedReadPage(page, type);
  if (!guard) {
    throw Exception("buffer pool has no available frame/task");
  }
  return std::move(*guard);
}
auto BufferPoolManager::WritePage(page_id_t page, AccessType type) -> WritePageGuard {
  auto guard = CheckedWritePage(page, type);
  if (!guard) {
    throw Exception("buffer pool has no available frame/task");
  }
  return std::move(*guard);
}
auto BufferPoolManager::GetPinCount(page_id_t page) -> std::optional<size_t> {
  PageNumber(page);
  Call call(*state_);
  auto e = state_->directory_.Access(page);
  if (auto id = e.Frame()) {
    auto &f = *state_->frames_[*id];
    std::lock_guard<std::mutex> lock(f.mutex_);
    return f.pins_;
  }
  return std::nullopt;
}
auto BufferPoolManager::DeletePage(page_id_t page) -> bool {
  PageNumber(page);
  Call call(*state_);
  std::optional<frame_id_t> freed;
  {
    auto e = state_->directory_.Access(page);
    if (auto id = e.Frame()) {
      auto &f = *state_->frames_[*id];
      std::lock_guard<std::mutex> lock(f.mutex_);
      if (f.pins_ || f.phase_ != FramePhase::Resident) {
        return false;
      }
      state_->replacer_.Remove(*id);
      e.SetFrame(std::nullopt);
      f.phase_ = FramePhase::Free;
      f.page_ = INVALID_PAGE_ID;
      freed = id;
      f.changed_.notify_all();
    }
  }
  if (freed) {
    state_->Free(*freed);
  }
  state_->storage_->Delete(page);
  return true;
}
void BufferPoolState::Flush(const std::vector<std::pair<page_id_t, uint64_t>> &pages) {
  size_t cursor = 0;
  while (cursor < pages.size()) {
    std::vector<FrameHeader *> held;
    std::vector<PageBuffer> buffers;
    std::vector<std::unique_ptr<Slot>> slots;
    std::vector<uint64_t> domains;
    const auto capacity = std::min(storage_->MaxBatchPages(), pages.size() - cursor);
    held.reserve(capacity);
    buffers.reserve(capacity);
    slots.reserve(capacity);
    domains.reserve(capacity);
    while (cursor < pages.size() && held.size() < storage_->MaxBatchPages()) {
      const auto page = pages[cursor].first;
      const auto generation = pages[cursor].second;
      auto slot = TrySlot();
      if (!slot) {
        if (!held.empty()) {
          break;
        }
        std::unique_lock<std::mutex> lock(mutex_);
        changed_.wait(lock, [&] { return tasks_ < max_tasks_; });
        continue;
      }
      FrameHeader *f = nullptr;
      std::unique_lock<std::mutex> content;
      {
        auto e = directory_.Access(page);
        if (auto id = e.Frame()) {
          f = frames_[*id].get();
          content = std::unique_lock<std::mutex>(f->mutex_);
        }
      }
      if (!f || f->generation_ != generation || f->page_ != page) {
        ++cursor;
        continue;
      }
      if (f->phase_ != FramePhase::Resident || f->writer_ || f->io_) {
        if (!held.empty()) {
          break;
        }
        const auto task = f->task_;
        content.unlock();
        slot.reset();
        if (task) {
          task->Wait();
        } else {
          content.lock();
          f->changed_.wait(
              content, [&] { return f->page_ != page || f->generation_ != generation || (!f->writer_ && !f->io_); });
        }
        continue;
      }
      const auto domain = storage_->WriteDomain(page);
      if (std::find(domains.begin(), domains.end(), domain) != domains.end()) {
        break;
      }
      domains.push_back(domain);
      ++cursor;
      ++f->pins_;
      f->io_ = true;
      replacer_.SetEvictable(f->id_, false);
      held.push_back(f);
      buffers.push_back(Buffer(*f));
      slots.push_back(std::move(slot));
    }
    std::exception_ptr error;
    try {
      if (!buffers.empty()) {
        storage_->Write(buffers);
      }
    } catch (...) {
      error = std::current_exception();
    }
    for (auto *f : held) {
      std::lock_guard<std::mutex> lock(f->mutex_);
      if (!error) {
        f->clean_version_ = f->dirty_version_;
      }
      f->io_ = false;
      Unpin(*f);
    }
    slots.clear();
    if (error) {
      std::rethrow_exception(error);
    }
  }
}
auto BufferPoolManager::FlushPage(page_id_t page) -> bool {
  PageNumber(page);
  Call call(*state_);
  uint64_t generation;
  {
    auto e = state_->directory_.Access(page);
    auto id = e.Frame();
    if (!id) {
      return false;
    }
    auto &f = *state_->frames_[*id];
    std::lock_guard<std::mutex> lock(f.mutex_);
    generation = f.generation_;
  }
  state_->Flush({{page, generation}});
  return true;
}
void BufferPoolManager::FlushAllPages() {
  Call call(*state_);
  std::vector<std::pair<page_id_t, uint64_t>> pages;
  for (const auto &f : state_->frames_) {
    std::lock_guard<std::mutex> lock(f->mutex_);
    if (f->phase_ != FramePhase::Free) {
      pages.emplace_back(f->page_, f->generation_);
    }
  }
  state_->Flush(pages);
}
}  // namespace bustub
