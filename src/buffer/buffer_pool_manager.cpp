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
#include <cstring>
#include <limits>
#include <stdexcept>
#include "buffer_pool_internal.h"
#include "common/exception.h"
namespace bustub {
namespace {
class Call {
 public:
  explicit Call(BufferPoolState &s) : s_(s) {
    {
      std::lock_guard<std::mutex> lock(s_.mutex_);
      if (s_.closed_) {
        throw std::runtime_error("buffer pool is closed");
      }
      ++s_.calls_;
    }
    try {
      s_.directory_.Maintain();
    } catch (...) {
      Finish();
      throw;
    }
  }
  ~Call() { Finish(); }

 private:
  void Finish() {
    std::lock_guard<std::mutex> lock(s_.mutex_);
    --s_.calls_;
    s_.changed_.notify_all();
  }

  BufferPoolState &s_;
};
void PageNumber(page_id_t p) {
  if (p < 0) {
    throw std::invalid_argument("negative database page");
  }
}
// Compatibility deployment budget; object deployments supply explicit budgets.
auto FileOptions(size_t frames) -> BufferPoolOptions {
  return {std::max<size_t>(4U << 20U, frames * 8192), frames * BUSTUB_PAGE_SIZE + (2U << 20U), frames, false, 0};
}
}  // namespace
BufferPoolState::BufferPoolState(size_t count, std::shared_ptr<PageStorage> storage, BufferPoolOptions options,
                                 size_t k)
    : max_tasks_(options.max_inflight_pages_),
      max_prefetch_(options.prefetch_pages_),
      storage_(std::move(storage)),
      arena_({count, BUSTUB_PAGE_SIZE, storage_->MemoryAlignment(), options.arena_bytes_, options.advise_huge_pages_}),
      directory_({options.directory_bytes_}),
      replacer_(count, k) {
  if (max_tasks_ == 0 || storage_->MaxBatchPages() == 0 || max_prefetch_ >= count || max_prefetch_ >= max_tasks_) {
    throw std::invalid_argument("no page task capacity");
  }
  frames_.reserve(count);
  free_.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    frames_.push_back(std::make_unique<FrameHeader>(static_cast<frame_id_t>(i), arena_.Frame(i)));
    free_.push_back(count - i - 1);
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
  if (prefetch_) {
    --state_.prefetch_;
  }
  state_.changed_.notify_all();
}
auto BufferPoolState::TrySlot(bool prefetch) -> std::unique_ptr<Slot> {
  std::lock_guard<std::mutex> lock(mutex_);
  if (tasks_ == max_tasks_ || (prefetch && (prefetch_ == max_prefetch_ || tasks_ + 1 == max_tasks_))) {
    return nullptr;
  }
  auto slot = std::make_unique<Slot>(*this, prefetch);
  ++tasks_;
  if (prefetch) {
    ++prefetch_;
  }
  return slot;
}
void BufferPoolState::Free(frame_id_t frame) {
  std::lock_guard<std::mutex> lock(mutex_);
  free_.push_back(frame);
}
auto BufferPoolState::PrepareFrame(page_id_t page, AccessType type, bool prefetch,
                                   const std::shared_ptr<PageTask> &loading) -> FrameHeader * {
  for (;;) {
    auto evicting = std::make_shared<PageTask>();
    std::optional<frame_id_t> selected;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!free_.empty()) {
        selected = free_.back();
        free_.pop_back();
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
        if (prefetch) {
          return nullptr;
        }
        continue;
      }  // Another selector retired this candidate.
      bool dirty;
      {
        auto entry = directory_.Access(previous);
        std::lock_guard<std::mutex> lock(f.mutex_);
        if (entry.Frame() != selected || (f.phase_ != FramePhase::Resident && f.phase_ != FramePhase::Failed) ||
            f.pins_ != 0) {
          if (prefetch) {
            return nullptr;
          }
          continue;
        }
        if (prefetch && (f.dirty_version_ != f.clean_version_ || f.io_)) {
          return nullptr;
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
    try {
      auto entry = directory_.Access(page);
      std::lock_guard<std::mutex> lock(f.mutex_);
      lost = entry.Frame().has_value();
      if (!lost) {
        // Register replacement history before accepting IO: completion must not allocate.
        replacer_.RecordAccess(f.id_, type);
        replacer_.SetEvictable(f.id_, false);
        f.page_ = page;
        ++f.generation_;
        f.dirty_version_ = f.clean_version_ = 0;
        f.phase_ = FramePhase::Loading;
        f.io_ = true;
        f.task_ = loading;
        entry.SetFrame(f.id_);
      }
    } catch (...) {
      // A group may need to be rematerialized after the initial miss. Failure
      // before registration must return this privately held, now-free frame.
      Free(f.id_);
      throw;
    }
    if (lost) {
      Free(f.id_);
      // Fetch must rejoin the existing load rather than evict more frames for
      // a page whose loading/resident identity is already installed.
      return nullptr;
    }
    return &f;
  }
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
        if (hit->phase_ == FramePhase::Loading || hit->phase_ == FramePhase::Evicting ||
            hit->phase_ == FramePhase::Failed) {
          waiting = hit->task_;
        } else {
          replacer_.RecordPinnedAccess(hit->id_, type, hit->pins_ == 0);
          ++hit->pins_;
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
    auto slot = TrySlot(false);
    if (!slot) {
      return nullptr;
    }
    auto loading = std::make_shared<PageTask>();
    auto *prepared = PrepareFrame(page, type, false, loading);
    if (!prepared) {
      auto entry = directory_.Access(page);
      if (entry.Frame()) {
        continue;
      }
      return nullptr;
    }
    auto &f = *prepared;
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
        replacer_.SetEvictable(f.id_, true);
        replacer_.Remove(f.id_);
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
  state_->changed_.wait(lock, [&] { return state_->calls_ == 0 && state_->tasks_ == 0; });
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
      if (f.pins_ || (f.phase_ != FramePhase::Resident && f.phase_ != FramePhase::Failed)) {
        return false;
      }
      state_->replacer_.Remove(*id);
      e.SetFrame(std::nullopt);
      f.phase_ = FramePhase::Free;
      f.page_ = INVALID_PAGE_ID;
      f.task_.reset();
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
auto BufferPoolManager::SupportsPageRetirement() const -> bool { return state_->storage_->SupportsRetirement(); }
auto BufferPoolManager::RetirePage(ReadPageGuard &victim, WritePageGuard &link,
                                   const std::array<char, BUSTUB_PAGE_SIZE> &replacement) -> bool {
  Call call(*state_);
  if (!SupportsPageRetirement() || victim.state_ != state_ || link.state_ != state_ || !victim.frame_ || !link.frame_ ||
      victim.frame_ == link.frame_) {
    throw std::invalid_argument("retirement requires two distinct owned pages and a supported backend");
  }
  auto &f = *victim.frame_;
  const auto page = f.page_;
  auto task = std::make_shared<PageTask>();
  {
    auto entry = state_->directory_.Access(page);
    std::lock_guard<std::mutex> lock(f.mutex_);
    if (f.pins_ != 1 || f.io_ || f.phase_ != FramePhase::Resident) {
      return false;
    }
    f.phase_ = FramePhase::Evicting;
    f.task_ = task;  // Later lookups join; no entry/frame mutex spans the commit.
  }
  try {
    state_->storage_->RetirePage(page, link.GetPageId(), replacement);
  } catch (...) {
    const auto error = std::current_exception();
    {
      std::lock_guard<std::mutex> lock(state_->mutex_);
      state_->closed_ = true;
    }
    {
      std::lock_guard<std::mutex> lock(f.mutex_);
      f.phase_ = FramePhase::Failed;
      f.changed_.notify_all();
    }
    task->Finish(error);
    std::rethrow_exception(error);
  }
  // No throwing allocation after the durable handoff. Link's write permission
  // excludes readers/writers/flush; victim's entry excludes a new pin.
  std::memcpy(link.GetDataMut(), replacement.data(), replacement.size());
  {
    std::lock_guard<std::mutex> lock(link.frame_->mutex_);
    link.frame_->clean_version_ = link.frame_->dirty_version_;
    link.mutated_ = false;
  }
  {
    auto entry = state_->directory_.Access(page);
    std::lock_guard<std::mutex> lock(f.mutex_);
    state_->replacer_.SetEvictable(f.id_, true);
    state_->replacer_.Remove(f.id_);
    entry.SetFrame(std::nullopt);
    f.pins_ = f.readers_ = 0;
    f.page_ = INVALID_PAGE_ID;
    f.phase_ = FramePhase::Free;
    f.task_.reset();
    f.changed_.notify_all();
    victim.frame_ = nullptr;
    victim.state_.reset();
  }
  state_->Free(f.id_);
  // Flush snapshots can now skip the vanished frame; demand readers recheck
  // residency and the backend rejects this durably retired identity.
  task->Finish();
  return true;
}
void BufferPoolManager::ReclaimRetiredPages() {
  Call call(*state_);
  state_->storage_->ReclaimRetiredPages();
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
    std::exception_ptr error;
    try {
      // Preparation can fail after earlier frames have been retained (for
      // example, rematerializing an evicted page under directory pressure).
      // Release the whole retained batch on either preparation or IO failure.
      while (cursor < pages.size() && held.size() < storage_->MaxBatchPages()) {
        const auto page = pages[cursor].first;
        const auto generation = pages[cursor].second;
        auto slot = TrySlot(false);
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
        if (!f || f->generation_ != generation || f->page_ != page || f->phase_ == FramePhase::Failed) {
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

void BufferPoolState::CompletePrefetch(FrameHeader &f, const std::shared_ptr<PageTask> &task,
                                       std::exception_ptr error) {
  {
    std::lock_guard<std::mutex> lock(f.mutex_);
    f.phase_ = error ? FramePhase::Failed : FramePhase::Resident;
    f.io_ = false;
    if (!error) {
      f.task_.reset();
    }
    replacer_.SetEvictable(f.id_, true);
    f.changed_.notify_all();
  }
  task->Finish(error);
}
void BufferPoolState::AbandonPrefetch(FrameHeader &f, const std::shared_ptr<PageTask> &task) {
  {
    auto entry = directory_.Access(f.page_);
    std::lock_guard<std::mutex> lock(f.mutex_);
    entry.SetFrame(std::nullopt);
    replacer_.SetEvictable(f.id_, true);
    replacer_.Remove(f.id_);
    f.phase_ = FramePhase::Free;
    f.page_ = INVALID_PAGE_ID;
    f.io_ = false;
    f.task_.reset();
    f.changed_.notify_all();
  }
  Free(f.id_);
  task->Finish();  // Joined demand readers retry; rejection is not a read error.
}
auto BufferPoolManager::PrefetchWindow() const -> size_t { return state_->max_prefetch_; }
void BufferPoolManager::PrefetchPages(const std::vector<page_id_t> &pages) {
  Call call(*state_);
  const auto count = std::min(pages.size(), state_->max_prefetch_);
  state_->directory_.Prefetch(pages.data(), count);
  for (size_t i = 0; i < count; ++i) {
    const auto page = pages[i];
    PageNumber(page);
    if (std::find(pages.begin(), pages.begin() + i, page) != pages.begin() + i) {
      continue;
    }
    std::shared_ptr<BufferPoolState::Slot> slot = state_->TrySlot(true);
    if (!slot) {
      break;
    }
    auto task = std::make_shared<PageTask>();
    FrameHeader *f = nullptr;
    try {
      {
        auto entry = state_->directory_.Access(page);
        if (auto id = entry.Frame()) {
          auto &frame = *state_->frames_[*id];
          std::lock_guard<std::mutex> lock(frame.mutex_);
          if (frame.phase_ == FramePhase::Resident) {
            __builtin_prefetch(frame.memory_.data_, 0, 1);
          }
          continue;  // In-flight, failed and resident identities are all shared.
        }
      }
      f = state_->PrepareFrame(page, AccessType::Scan, true, task);
      if (!f) {
        continue;
      }
      // Close retains this raw state until the callback's Slot is destroyed.
      // Do not retain the entire node as the last owner on an executor worker.
      auto complete = [state = state_.get(), f, task, slot](std::exception_ptr error) {
        state->CompletePrefetch(*f, task, error);
      };
      if (!state_->storage_->Prefetch(state_->Buffer(*f), std::move(complete))) {
        state_->AbandonPrefetch(*f, task);
        break;
      }
    } catch (const std::bad_alloc &) {
      if (f) {
        state_->AbandonPrefetch(*f, task);
      }
      break;  // Resource refusal is retried by actual demand, never counted as success.
    } catch (...) {
      if (!f) {
        throw;
      }
      state_->CompletePrefetch(*f, task, std::current_exception());
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
void BufferPoolManager::FlushDirtyPages(size_t limit) {
  Call call(*state_);
  std::vector<std::pair<page_id_t, uint64_t>> pages;
  for (const auto &f : state_->frames_) {
    if (pages.size() == limit) break;
    std::lock_guard<std::mutex> lock(f->mutex_);
    if (f->phase_ == FramePhase::Resident && !f->writer_ && !f->io_ && f->dirty_version_ != f->clean_version_) {
      pages.emplace_back(f->page_, f->generation_);
    }
  }
  state_->Flush(pages);
}
auto BufferPoolManager::CapturePages(const std::vector<page_id_t> &selected, size_t max_bytes) -> PageCapture {
  Call call(*state_);
  PageCapture capture;
  {
    std::lock_guard<std::mutex> lock(state_->mutex_);
    capture.next_page_ = state_->next_page_;
  }
  auto pages = selected;
  std::sort(pages.begin(), pages.end());
  capture.dirty_.reserve(std::min({pages.size(), state_->frames_.size(), max_bytes / sizeof(CapturedPage)}));
  for (const auto &f : state_->frames_) {
    std::lock_guard<std::mutex> lock(f->mutex_);
    if (!std::binary_search(pages.begin(), pages.end(), f->page_) || f->dirty_version_ == f->clean_version_) continue;
    if (f->writer_ || f->phase_ != FramePhase::Resident) {
      throw std::logic_error("page capture requires a complete business boundary");
    }
    if (capture.dirty_.size() >= max_bytes / sizeof(CapturedPage))
      throw std::runtime_error("checkpoint page budget exceeded");
    CapturedPage copy;
    copy.page_ = f->page_;
    std::memcpy(copy.bytes_.data(), f->memory_.data_, copy.bytes_.size());
    capture.dirty_.push_back(std::move(copy));
  }
  return capture;
}
}  // namespace bustub
