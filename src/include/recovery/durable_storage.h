//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// durable_storage.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <filesystem>
#include <string>
#include <vector>

namespace bustub {

/** Immutable byte range used to pass large payloads without materializing them in memory. */
struct DurableFileSlice {
  std::filesystem::path path_;
  uint64_t offset_{0};
  uint64_t size_{0};
};

/** Explicit durability boundary used by snapshots, logs, and StableStore. */
class DurableStorage {
 public:
  virtual ~DurableStorage() = default;

  virtual void CreateDirectories(const std::filesystem::path &path) = 0;
  virtual void WriteFile(const std::filesystem::path &path, const std::vector<std::byte> &data) = 0;
  /** Append without implying a durability barrier; callers batch then call SyncFile. */
  virtual void AppendFile(const std::filesystem::path &path, const std::vector<std::byte> &data) = 0;
  virtual void AppendFileDurable(const std::filesystem::path &path, const std::vector<std::byte> &data) = 0;
  virtual auto ReadFile(const std::filesystem::path &path, size_t maximum_size) -> std::vector<std::byte> = 0;
  virtual auto ReadFileRange(const std::filesystem::path &path, uint64_t offset, size_t maximum_size)
      -> std::vector<std::byte> = 0;
  virtual auto FileSize(const std::filesystem::path &path) -> uint64_t = 0;
  virtual void TruncateFile(const std::filesystem::path &path, uint64_t size) = 0;
  virtual auto ChecksumFile(const std::filesystem::path &path) -> uint32_t = 0;
  virtual void SyncFile(const std::filesystem::path &path) = 0;
  virtual void SyncDirectory(const std::filesystem::path &path) = 0;
  virtual void Rename(const std::filesystem::path &from, const std::filesystem::path &to) = 0;
  virtual void CopyFile(const std::filesystem::path &from, const std::filesystem::path &to) = 0;
  virtual void RemoveFile(const std::filesystem::path &path) = 0;
  virtual void RemoveTree(const std::filesystem::path &path) = 0;
  virtual auto Exists(const std::filesystem::path &path) const -> bool = 0;
  /** Returns the immediate entry names in deterministic lexical order. */
  virtual auto ListDirectory(const std::filesystem::path &path) const -> std::vector<std::string> = 0;
};

class PosixDurableStorage : public DurableStorage {
 public:
  void CreateDirectories(const std::filesystem::path &path) override;
  void WriteFile(const std::filesystem::path &path, const std::vector<std::byte> &data) override;
  void AppendFile(const std::filesystem::path &path, const std::vector<std::byte> &data) override;
  void AppendFileDurable(const std::filesystem::path &path, const std::vector<std::byte> &data) override;
  auto ReadFile(const std::filesystem::path &path, size_t maximum_size) -> std::vector<std::byte> override;
  auto ReadFileRange(const std::filesystem::path &path, uint64_t offset, size_t maximum_size)
      -> std::vector<std::byte> override;
  auto FileSize(const std::filesystem::path &path) -> uint64_t override;
  void TruncateFile(const std::filesystem::path &path, uint64_t size) override;
  auto ChecksumFile(const std::filesystem::path &path) -> uint32_t override;
  void SyncFile(const std::filesystem::path &path) override;
  void SyncDirectory(const std::filesystem::path &path) override;
  void Rename(const std::filesystem::path &from, const std::filesystem::path &to) override;
  void CopyFile(const std::filesystem::path &from, const std::filesystem::path &to) override;
  void RemoveFile(const std::filesystem::path &path) override;
  void RemoveTree(const std::filesystem::path &path) override;
  auto Exists(const std::filesystem::path &path) const -> bool override;
  auto ListDirectory(const std::filesystem::path &path) const -> std::vector<std::string> override;
};

}  // namespace bustub
