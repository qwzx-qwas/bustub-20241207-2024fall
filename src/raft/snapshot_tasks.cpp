#include "snapshot_tasks.h"

#include <stdexcept>
#include <utility>

#include "lz4.h"

namespace bustub {

SnapshotTasks::SnapshotTasks() {
  try {
    for (size_t i = 0; i < 2; ++i) threads_.emplace_back([this] { Run(); });
  } catch (...) {
    {
      std::lock_guard lock(mutex_);
      closing_ = true;
    }
    ready_.notify_all();
    for (auto &thread : threads_) thread.join();
    throw;
  }
}
SnapshotTasks::~SnapshotTasks() {
  {
    std::lock_guard lock(mutex_);
    closing_ = true;
  }
  ready_.notify_all();
  for (auto &thread : threads_) thread.join();
}
auto SnapshotTasks::Submit(std::function<InstallSnapshotRequest()> work)
    -> std::optional<std::future<InstallSnapshotRequest>> {
  std::lock_guard lock(mutex_);
  if (closing_ || outstanding_ == 4) return std::nullopt;
  std::packaged_task<InstallSnapshotRequest()> task(std::move(work));
  auto result = task.get_future();
  queue_.push_back(std::move(task));
  ++outstanding_;
  ready_.notify_one();
  return result;
}
void SnapshotTasks::Run() {
  for (;;) {
    std::packaged_task<InstallSnapshotRequest()> task;
    {
      std::unique_lock lock(mutex_);
      ready_.wait(lock, [this] { return closing_ || !queue_.empty(); });
      if (queue_.empty()) return;
      task = std::move(queue_.front());
      queue_.pop_front();
    }
    task();     // Exceptions belong to the completion cell, never to a detached thread.
    task = {};  // Release the input lease before returning admission capacity.
    std::lock_guard lock(mutex_);
    --outstanding_;
  }
}

void CompressSnapshotChunk(InstallSnapshotRequest *request, uint64_t session) {
  if (request->reuse_ || request->data_.size() < 4096) return;
  if (request->data_.size() > 64U * 1024U) throw std::runtime_error("snapshot source exceeded block limit");
  const auto size = static_cast<int>(request->data_.size());
  std::vector<std::byte> encoded(LZ4_compressBound(size));
  const auto written = LZ4_compress_default(reinterpret_cast<const char *>(request->data_.data()),
                                            reinterpret_cast<char *>(encoded.data()), size, encoded.size());
  if (written == 0) throw std::runtime_error("snapshot LZ4 encoding failed");
  // Charge codec, raw length, session and the full transfer's delta-session field.
  constexpr size_t overhead = 1 + 4 + 8 + 8;
  if (static_cast<size_t>(written) + overhead > request->data_.size() - request->data_.size() / 8) return;
  encoded.resize(written);
  request->raw_size_ = size;
  request->encoding_ = SnapshotEncoding::Lz4;
  request->encoding_session_ = session;
  request->data_ = std::move(encoded);
}
void DecompressSnapshotChunk(InstallSnapshotRequest *request) {
  if (request->encoding_ != SnapshotEncoding::Lz4 || request->raw_size_ == 0 || request->raw_size_ > 64U * 1024U ||
      request->data_.empty() || request->data_.size() > 64U * 1024U)
    throw std::runtime_error("invalid compressed snapshot block");
  std::vector<std::byte> decoded(request->raw_size_);
  const auto written =
      LZ4_decompress_safe(reinterpret_cast<const char *>(request->data_.data()),
                          reinterpret_cast<char *>(decoded.data()), request->data_.size(), decoded.size());
  if (written != static_cast<int>(request->raw_size_)) throw std::runtime_error("damaged snapshot LZ4 block");
  request->data_ = std::move(decoded);
  request->encoding_ = SnapshotEncoding::Raw;
  request->raw_size_ = 0;
  request->encoding_session_ = 0;
}

}  // namespace bustub
