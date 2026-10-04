// ===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.h
//
// Identification: src/include/storage/page/page_guard.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
// ===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// PageGuard owns a pin and logical content permission, not a thread-owned latch.
//===----------------------------------------------------------------------===//
#pragma once
#include <memory>
#include "common/config.h"
namespace bustub {
class BufferPoolManager;
struct BufferPoolState;
struct FrameHeader;
class ReadPageGuard {
 public:
  ReadPageGuard() = default;
  ReadPageGuard(const ReadPageGuard &) = delete;
  auto operator=(const ReadPageGuard &) -> ReadPageGuard & = delete;
  ReadPageGuard(ReadPageGuard &&other) noexcept;
  auto operator=(ReadPageGuard &&other) noexcept -> ReadPageGuard &;
  ~ReadPageGuard();
  auto GetPageId() const -> page_id_t;
  auto GetData() const -> const char *;
  template <class T>
  auto As() const -> const T * {
    return reinterpret_cast<const T *>(GetData());
  }
  auto IsDirty() const -> bool;
  void Drop();

 private:
  friend class BufferPoolManager;
  ReadPageGuard(std::shared_ptr<BufferPoolState> state, FrameHeader *frame);
  std::shared_ptr<BufferPoolState> state_;
  FrameHeader *frame_{nullptr};
};
class WritePageGuard {
 public:
  WritePageGuard() = default;
  WritePageGuard(const WritePageGuard &) = delete;
  auto operator=(const WritePageGuard &) -> WritePageGuard & = delete;
  WritePageGuard(WritePageGuard &&other) noexcept;
  auto operator=(WritePageGuard &&other) noexcept -> WritePageGuard &;
  ~WritePageGuard();
  auto GetPageId() const -> page_id_t;
  auto GetData() const -> const char *;
  auto GetDataMut() -> char *;
  template <class T>
  auto As() const -> const T * {
    return reinterpret_cast<const T *>(GetData());
  }
  template <class T>
  auto AsMut() -> T * {
    return reinterpret_cast<T *>(GetDataMut());
  }
  auto IsDirty() const -> bool;
  void Drop();

 private:
  friend class BufferPoolManager;
  WritePageGuard(std::shared_ptr<BufferPoolState> state, FrameHeader *frame);
  std::shared_ptr<BufferPoolState> state_;
  FrameHeader *frame_{nullptr};
  bool mutated_{false};
};
}  // namespace bustub
