//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// block_device.cpp
//
//===----------------------------------------------------------------------===//

#include "storage/disk/block_device.h"

#include <fcntl.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <limits>
#include <utility>

namespace bustub {
namespace {

[[noreturn]] void Fail(int error, const char *operation, const std::filesystem::path &path) {
  throw std::system_error(error, std::generic_category(), std::string(operation) + ": " + path.string());
}

auto OpenRetry(const std::filesystem::path &path, int flags) -> int {
  int fd;
  do {
    fd = open(path.c_str(), flags);
  } while (fd < 0 && errno == EINTR);
  return fd;
}

}  // namespace

BlockDeviceIOError::BlockDeviceIOError(int error, const std::string &operation, StorageByteRange range,
                                       uint64_t completed_bytes)
    : std::system_error(error, std::generic_category(),
                        operation + " offset=" + std::to_string(range.Offset()) +
                            " size=" + std::to_string(range.Size()) + " completed=" + std::to_string(completed_bytes)),
      range_(range),
      completed_bytes_(completed_bytes) {}

BlockDevice::BlockDevice(std::filesystem::path path, const BlockDeviceOptions &options) : path_(std::move(path)) {
  if (options.access_mode_ != DeviceAccessMode::Direct && options.access_mode_ != DeviceAccessMode::Buffered) {
    Fail(EINVAL, "invalid device access mode", path_);
  }
  struct stat before {};
  if (stat(path_.c_str(), &before) != 0) {
    Fail(errno, "stat device", path_);
  }
  if (!S_ISREG(before.st_mode) && !S_ISBLK(before.st_mode)) {
    Fail(ENOTSUP, "expected regular file or block device", path_);
  }
  int flags = O_RDWR | O_CLOEXEC | O_NONBLOCK;
  if (options.access_mode_ == DeviceAccessMode::Direct) {
    flags |= O_DIRECT;
  }
  if (S_ISBLK(before.st_mode)) {
    flags |= O_EXCL;
  }
  fd_ = OpenRetry(path_, flags);
  if (fd_ < 0) {
    Fail(errno, "open device", path_);
  }
  try {
    struct stat opened {};
    if (fstat(fd_, &opened) != 0) {
      Fail(errno, "fstat device", path_);
    }
    // In particular, do not accidentally open a substituted block device
    // without O_EXCL after initially observing a regular file.
    if (opened.st_dev != before.st_dev || opened.st_ino != before.st_ino ||
        (opened.st_mode & S_IFMT) != (before.st_mode & S_IFMT) || opened.st_rdev != before.st_rdev) {
      Fail(ESTALE, "device changed during open", path_);
    }
    info_.is_block_device_ = S_ISBLK(opened.st_mode);
    info_.access_mode_ = options.access_mode_;
    info_.memory_alignment_ = 1;
    info_.offset_alignment_ = 1;
    if (info_.is_block_device_) {
      if (ioctl(fd_, BLKGETSIZE64, &info_.capacity_) != 0) {
        Fail(errno, "query block device capacity", path_);
      }
    } else {
      if (opened.st_size < 0) {
        Fail(EOVERFLOW, "negative file capacity", path_);
      }
      info_.capacity_ = static_cast<uint64_t>(opened.st_size);
    }
    if (options.access_mode_ == DeviceAccessMode::Direct) {
#if defined(STATX_DIOALIGN)
      struct statx alignment {};
      int result;
      do {
        result = statx(fd_, "", AT_EMPTY_PATH, STATX_DIOALIGN, &alignment);
      } while (result != 0 && errno == EINTR);
      if (result != 0) {
        Fail(errno, "query direct IO alignment", path_);
      }
      if ((alignment.stx_mask & STATX_DIOALIGN) == 0 || alignment.stx_dio_mem_align == 0 ||
          alignment.stx_dio_offset_align == 0) {
        Fail(ENOTSUP, "direct IO alignment unavailable", path_);
      }
      info_.memory_alignment_ = alignment.stx_dio_mem_align;
      info_.offset_alignment_ = alignment.stx_dio_offset_align;
#else
      Fail(ENOTSUP, "build lacks STATX_DIOALIGN", path_);
#endif
    }
  } catch (...) {
    close(std::exchange(fd_, -1));
    throw;
  }
}

BlockDevice::~BlockDevice() {
  if (fd_ >= 0) {
    close(fd_);
  }
}

void BlockDevice::Validate(StorageByteRange range, const void *buffer, size_t buffer_size) const {
  if (fd_ < 0) {
    throw BlockDeviceIOError(EBADF, "device is closed", range, 0);
  }
  // StorageByteRange has already checked uint64_t addition. These are backend
  // capacity and off_t limits, not a second implementation of range arithmetic.
  const auto end = range.Offset() + range.Size();
  if (end > info_.capacity_ || end > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
    throw BlockDeviceIOError(EINVAL, "range exceeds device or system offset limit", range, 0);
  }
  if (range.Size() > buffer_size || (range.Size() != 0 && buffer == nullptr)) {
    throw BlockDeviceIOError(EINVAL, "insufficient IO buffer", range, 0);
  }
  if (range.Size() != 0) {
    ValidateRemaining(range, buffer, 0);
  }
}

void BlockDevice::ValidateRemaining(StorageByteRange range, const void *buffer, uint64_t completed) const {
  if (reinterpret_cast<uintptr_t>(buffer) % info_.memory_alignment_ != 0 ||
      (range.Offset() + completed) % info_.offset_alignment_ != 0 ||
      (range.Size() - completed) % info_.offset_alignment_ != 0) {
    throw BlockDeviceIOError(completed == 0 ? EINVAL : EIO, "unaligned IO remainder", range, completed);
  }
}

void BlockDevice::ReadAt(StorageByteRange range, void *buffer, size_t buffer_size) const {
  Validate(range, buffer, buffer_size);
  auto *bytes = static_cast<char *>(buffer);
  uint64_t completed = 0;
  while (completed < range.Size()) {
    ValidateRemaining(range, bytes + completed, completed);
    auto count = std::min<uint64_t>(range.Size() - completed, std::numeric_limits<ssize_t>::max());
    count -= count % info_.offset_alignment_;
    if (count == 0) {
      throw BlockDeviceIOError(EOVERFLOW, "IO alignment exceeds syscall count limit", range, completed);
    }
    const auto result =
        pread(fd_, bytes + completed, static_cast<size_t>(count), static_cast<off_t>(range.Offset() + completed));
    if (result < 0) {
      const int error = errno;
      if (error == EINTR) {
        continue;
      }
      throw BlockDeviceIOError(error, "read " + path_.string(), range, completed);
    }
    if (result == 0) {
      throw BlockDeviceIOError(EIO, "unexpected device EOF", range, completed);
    }
    completed += static_cast<uint64_t>(result);
  }
}

void BlockDevice::WriteAt(StorageByteRange range, const void *buffer, size_t buffer_size) const {
  Validate(range, buffer, buffer_size);
  const auto *bytes = static_cast<const char *>(buffer);
  uint64_t completed = 0;
  while (completed < range.Size()) {
    ValidateRemaining(range, bytes + completed, completed);
    auto count = std::min<uint64_t>(range.Size() - completed, std::numeric_limits<ssize_t>::max());
    count -= count % info_.offset_alignment_;
    if (count == 0) {
      throw BlockDeviceIOError(EOVERFLOW, "IO alignment exceeds syscall count limit", range, completed);
    }
    const auto result =
        pwrite(fd_, bytes + completed, static_cast<size_t>(count), static_cast<off_t>(range.Offset() + completed));
    if (result < 0) {
      const int error = errno;
      if (error == EINTR) {
        continue;
      }
      throw BlockDeviceIOError(error, "write " + path_.string(), range, completed);
    }
    if (result == 0) {
      throw BlockDeviceIOError(EIO, "device write made no progress", range, completed);
    }
    completed += static_cast<uint64_t>(result);
  }
}

void BlockDevice::Flush() const {
  if (fd_ < 0) {
    Fail(EBADF, "flush closed device", path_);
  }
  int result;
  do {
    result = fdatasync(fd_);
  } while (result != 0 && errno == EINTR);
  if (result != 0) {
    Fail(errno, "flush device", path_);
  }
}

void BlockDevice::Close() {
  if (fd_ >= 0 && close(std::exchange(fd_, -1)) != 0) {
    // Linux releases the descriptor even on close failure. Never retry: its
    // number could already belong to another thread's newly opened file.
    Fail(errno, "close device", path_);
  }
}

}  // namespace bustub
