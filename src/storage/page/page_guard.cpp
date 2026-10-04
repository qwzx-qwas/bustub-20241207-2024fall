// ===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2024-2024, Carnegie Mellon University Database Group
//
// ===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <stdexcept>
#include <utility>
#include "../../buffer/buffer_pool_internal.h"
namespace bustub {
namespace {
void Valid(const FrameHeader *frame) {
  if (!frame) {
    throw std::logic_error("invalid page guard");
  }
}
}  // namespace
ReadPageGuard::ReadPageGuard(std::shared_ptr<BufferPoolState> state, FrameHeader *frame)
    : state_(std::move(state)), frame_(frame) {}
ReadPageGuard::ReadPageGuard(ReadPageGuard &&other) noexcept
    : state_(std::move(other.state_)), frame_(std::exchange(other.frame_, nullptr)) {}
auto ReadPageGuard::operator=(ReadPageGuard &&other) noexcept -> ReadPageGuard & {
  if (this != &other) {
    Drop();
    state_ = std::move(other.state_);
    frame_ = std::exchange(other.frame_, nullptr);
  }
  return *this;
}
ReadPageGuard::~ReadPageGuard() { Drop(); }
auto ReadPageGuard::GetPageId() const -> page_id_t {
  Valid(frame_);
  return frame_->page_;
}
auto ReadPageGuard::GetData() const -> const char * {
  Valid(frame_);
  return frame_->memory_.data_;
}
auto ReadPageGuard::IsDirty() const -> bool {
  Valid(frame_);
  std::lock_guard<std::mutex> lock(frame_->mutex_);
  return frame_->dirty_version_ != frame_->clean_version_;
}
void ReadPageGuard::Drop() {
  if (!frame_) {
    return;
  }
  {
    std::lock_guard<std::mutex> lock(frame_->mutex_);
    --frame_->readers_;
    state_->Unpin(*frame_);
    frame_ = nullptr;
  }
  state_.reset();
}
WritePageGuard::WritePageGuard(std::shared_ptr<BufferPoolState> state, FrameHeader *frame)
    : state_(std::move(state)), frame_(frame) {}
WritePageGuard::WritePageGuard(WritePageGuard &&other) noexcept
    : state_(std::move(other.state_)), frame_(std::exchange(other.frame_, nullptr)), mutated_(other.mutated_) {}
auto WritePageGuard::operator=(WritePageGuard &&other) noexcept -> WritePageGuard & {
  if (this != &other) {
    Drop();
    state_ = std::move(other.state_);
    frame_ = std::exchange(other.frame_, nullptr);
    mutated_ = other.mutated_;
  }
  return *this;
}
WritePageGuard::~WritePageGuard() { Drop(); }
auto WritePageGuard::GetPageId() const -> page_id_t {
  Valid(frame_);
  return frame_->page_;
}
auto WritePageGuard::GetData() const -> const char * {
  Valid(frame_);
  return frame_->memory_.data_;
}
auto WritePageGuard::GetDataMut() -> char * {
  Valid(frame_);
  if (!mutated_) {
    std::lock_guard<std::mutex> lock(frame_->mutex_);
    ++frame_->dirty_version_;
    mutated_ = true;
  }
  return frame_->memory_.data_;
}
auto WritePageGuard::IsDirty() const -> bool {
  Valid(frame_);
  std::lock_guard<std::mutex> lock(frame_->mutex_);
  return frame_->dirty_version_ != frame_->clean_version_;
}
void WritePageGuard::Drop() {
  if (!frame_) {
    return;
  }
  {
    std::lock_guard<std::mutex> lock(frame_->mutex_);
    frame_->writer_ = false;
    state_->Unpin(*frame_);
    frame_ = nullptr;
  }
  mutated_ = false;
  state_.reset();
}
}  // namespace bustub
