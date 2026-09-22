//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// block_device.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <system_error>  // NOLINT(build/c++11)

#include "storage/byte_range.h"

namespace bustub {

enum class DeviceAccessMode { Direct, Buffered };

struct BlockDeviceOptions {
  DeviceAccessMode access_mode_{DeviceAccessMode::Direct};
};

struct BlockDeviceInfo {
  uint64_t capacity_;
  uint32_t memory_alignment_;
  uint32_t offset_alignment_;
  DeviceAccessMode access_mode_;
  bool is_block_device_;
};

/**
 * Failure of an exact range operation. CompletedBytes counts earlier positive
 * syscall returns only. A failing write may ALSO have changed its attempted
 * range; neither zero nor a nonzero count establishes rollback or atomicity.
 */
class BlockDeviceIOError : public std::system_error {
 public:
  BlockDeviceIOError(int error, const std::string &operation, StorageByteRange range, uint64_t completed_bytes);
  auto Range() const -> StorageByteRange { return range_; }
  auto CompletedBytes() const -> uint64_t { return completed_bytes_; }

 private:
  StorageByteRange range_;
  uint64_t completed_bytes_;
};

/**
 * Linux fixed-capacity positional IO. Opens an existing file or block device;
 * does not create, truncate, grow, format, cache, pad, or allocate data buffers.
 *
 * Independent ReadAt/WriteAt operations may execute concurrently. The caller
 * orders conflicting ranges, keeps buffers alive through return/exception,
 * and drains all calls before Close/destruction. No asynchronous cancellation.
 * File size and identity must not be changed externally during its lifetime.
 * Linux implementation is built only on Linux; this is not a portable stub.
 */
class BlockDevice {
 public:
  BlockDevice(std::filesystem::path path, const BlockDeviceOptions &options);
  ~BlockDevice();
  BlockDevice(const BlockDevice &) = delete;
  auto operator=(const BlockDevice &) -> BlockDevice & = delete;
  BlockDevice(BlockDevice &&) = delete;
  auto operator=(BlockDevice &&) -> BlockDevice & = delete;

  auto Info() const -> const BlockDeviceInfo & { return info_; }

  /** Exact IO; buffer_size is the caller's available capacity, not a requested short read. */
  void ReadAt(StorageByteRange range, void *buffer, size_t buffer_size) const;
  void WriteAt(StorageByteRange range, const void *buffer, size_t buffer_size) const;

  /**
   * fdatasync on this open instance. The caller must establish that all writes
   * it wants covered have returned successfully before calling Flush. Queued
   * or concurrent writes are not covered by this contract. Errors propagate.
   * Success relies on the underlying OS/device persistence contract, not on
   * O_DIRECT, alignment, or any claim of whole-range atomicity.
   */
  void Flush() const;

  /** Requires quiescence; no implicit Flush. Explicit close errors propagate. */
  void Close();

 private:
  void Validate(StorageByteRange range, const void *buffer, size_t buffer_size) const;
  void ValidateRemaining(StorageByteRange range, const void *buffer, uint64_t completed) const;

  std::filesystem::path path_;
  int fd_{-1};
  BlockDeviceInfo info_{};
};

}  // namespace bustub
