//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// durable_storage.cpp
//
//===----------------------------------------------------------------------===//

#include "recovery/durable_storage.h"

#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <system_error>  // NOLINT(build/c++11)

#include "common/byte_codec.h"

namespace bustub {
namespace {

[[noreturn]] void ThrowSystemError(const std::string &operation, const std::filesystem::path &path) {
  throw std::system_error(errno, std::generic_category(), operation + ": " + path.string());
}

class ScopedFd {
 public:
  explicit ScopedFd(int fd) : fd_(fd) {}
  ~ScopedFd() {
    if (fd_ >= 0) {
      close(fd_);
    }
  }
  auto Get() const -> int { return fd_; }

 private:
  int fd_;
};

}  // namespace

void PosixDurableStorage::CreateDirectories(const std::filesystem::path &path) {
  std::error_code error;
  std::filesystem::create_directories(path, error);
  if (error) {
    throw std::system_error(error, "create directories: " + path.string());
  }
}

void PosixDurableStorage::WriteFile(const std::filesystem::path &path, const std::vector<std::byte> &data) {
  ScopedFd fd(open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644));
  if (fd.Get() < 0) {
    ThrowSystemError("open for write", path);
  }
  size_t offset = 0;
  while (offset < data.size()) {
    const auto written = write(fd.Get(), data.data() + offset, data.size() - offset);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      ThrowSystemError("write", path);
    }
    offset += static_cast<size_t>(written);
  }
}

void PosixDurableStorage::AppendFile(const std::filesystem::path &path, const std::vector<std::byte> &data) {
  ScopedFd fd(open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644));
  if (fd.Get() < 0) {
    ThrowSystemError("open for append", path);
  }
  size_t offset = 0;
  while (offset < data.size()) {
    const auto written = write(fd.Get(), data.data() + offset, data.size() - offset);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      ThrowSystemError("append", path);
    }
    offset += static_cast<size_t>(written);
  }
}

void PosixDurableStorage::AppendFileDurable(const std::filesystem::path &path, const std::vector<std::byte> &data) {
  AppendFile(path, data);
  SyncFile(path);
}

auto PosixDurableStorage::ReadFile(const std::filesystem::path &path, size_t maximum_size) -> std::vector<std::byte> {
  std::error_code error;
  const auto size = std::filesystem::file_size(path, error);
  if (error) {
    throw std::system_error(error, "stat file: " + path.string());
  }
  if (size > maximum_size) {
    throw std::runtime_error("file exceeds configured maximum size: " + path.string());
  }
  ScopedFd fd(open(path.c_str(), O_RDONLY | O_CLOEXEC));
  if (fd.Get() < 0) {
    ThrowSystemError("open for read", path);
  }
  std::vector<std::byte> result(static_cast<size_t>(size));
  size_t offset = 0;
  while (offset < result.size()) {
    const auto count = read(fd.Get(), result.data() + offset, result.size() - offset);
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      ThrowSystemError("read", path);
    }
    if (count == 0) {
      throw std::runtime_error("file was truncated while reading: " + path.string());
    }
    offset += static_cast<size_t>(count);
  }
  return result;
}

auto PosixDurableStorage::ReadFileRange(const std::filesystem::path &path, uint64_t offset, size_t maximum_size)
    -> std::vector<std::byte> {
  const auto file_size = FileSize(path);
  if (offset > file_size) {
    throw std::runtime_error("file range starts beyond end of file: " + path.string());
  }
  const auto remaining = file_size - offset;
  const auto result_size = static_cast<size_t>(std::min<uint64_t>(remaining, maximum_size));
  ScopedFd fd(open(path.c_str(), O_RDONLY | O_CLOEXEC));
  if (fd.Get() < 0) {
    ThrowSystemError("open for ranged read", path);
  }
  std::vector<std::byte> result(result_size);
  size_t read_offset = 0;
  while (read_offset < result.size()) {
    const auto count = pread(fd.Get(), result.data() + read_offset, result.size() - read_offset,
                             static_cast<off_t>(offset + read_offset));
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      ThrowSystemError("ranged read", path);
    }
    if (count == 0) {
      throw std::runtime_error("file was truncated during ranged read: " + path.string());
    }
    read_offset += static_cast<size_t>(count);
  }
  return result;
}

auto PosixDurableStorage::FileSize(const std::filesystem::path &path) -> uint64_t {
  std::error_code error;
  const auto size = std::filesystem::file_size(path, error);
  if (error) {
    throw std::system_error(error, "stat file: " + path.string());
  }
  return size;
}

void PosixDurableStorage::TruncateFile(const std::filesystem::path &path, uint64_t size) {
  if (truncate(path.c_str(), static_cast<off_t>(size)) != 0) {
    ThrowSystemError("truncate", path);
  }
  SyncFile(path);
}

auto PosixDurableStorage::ChecksumFile(const std::filesystem::path &path) -> uint32_t {
  ScopedFd fd(open(path.c_str(), O_RDONLY | O_CLOEXEC));
  if (fd.Get() < 0) {
    ThrowSystemError("open for checksum", path);
  }
  std::vector<std::byte> chunk(1024U * 1024U);
  uint32_t checksum = 0;
  while (true) {
    const auto count = read(fd.Get(), chunk.data(), chunk.size());
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      ThrowSystemError("read for checksum", path);
    }
    if (count == 0) {
      break;
    }
    checksum = Crc32cExtend(checksum, chunk.data(), static_cast<size_t>(count));
  }
  return checksum;
}

void PosixDurableStorage::SyncFile(const std::filesystem::path &path) {
  ScopedFd fd(open(path.c_str(), O_RDONLY | O_CLOEXEC));
  if (fd.Get() < 0) {
    ThrowSystemError("open for sync", path);
  }
  if (fdatasync(fd.Get()) != 0) {
    ThrowSystemError("fdatasync", path);
  }
}

void PosixDurableStorage::SyncDirectory(const std::filesystem::path &path) {
  ScopedFd fd(open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
  if (fd.Get() < 0) {
    ThrowSystemError("open directory for sync", path);
  }
  if (fsync(fd.Get()) != 0) {
    ThrowSystemError("fsync directory", path);
  }
}

void PosixDurableStorage::Rename(const std::filesystem::path &from, const std::filesystem::path &to) {
  if (rename(from.c_str(), to.c_str()) != 0) {
    ThrowSystemError("rename to " + to.string(), from);
  }
}

void PosixDurableStorage::CopyFile(const std::filesystem::path &from, const std::filesystem::path &to) {
  std::error_code error;
  std::filesystem::copy_file(from, to, std::filesystem::copy_options::overwrite_existing, error);
  if (error) {
    throw std::system_error(error, "copy file from " + from.string() + " to " + to.string());
  }
}

void PosixDurableStorage::RemoveFile(const std::filesystem::path &path) {
  std::error_code error;
  const auto removed = std::filesystem::remove(path, error);
  if (error) {
    throw std::system_error(error, "remove file: " + path.string());
  }
  static_cast<void>(removed);
}

void PosixDurableStorage::RemoveTree(const std::filesystem::path &path) {
  std::error_code error;
  std::filesystem::remove_all(path, error);
  if (error) {
    throw std::system_error(error, "remove tree: " + path.string());
  }
}

auto PosixDurableStorage::Exists(const std::filesystem::path &path) const -> bool {
  std::error_code error;
  const auto exists = std::filesystem::exists(path, error);
  if (error) {
    throw std::system_error(error, "check path: " + path.string());
  }
  return exists;
}

auto PosixDurableStorage::ListDirectory(const std::filesystem::path &path) const -> std::vector<std::string> {
  std::error_code error;
  std::vector<std::string> entries;
  for (std::filesystem::directory_iterator iterator(path, error), end; !error && iterator != end;
       iterator.increment(error)) {
    entries.push_back(iterator->path().filename().string());
  }
  if (error) {
    throw std::system_error(error, "list directory: " + path.string());
  }
  std::sort(entries.begin(), entries.end());
  return entries;
}

}  // namespace bustub
