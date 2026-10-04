//===----------------------------------------------------------------------===//
// BusTub: S6 ordinary object IO tickets. No mapping publication on IO completion.
//===----------------------------------------------------------------------===//
#pragma once

#include <chrono>  // NOLINT(build/c++11)
#include <cstddef>
#include <memory>

#include "storage/disk/io_executor.h"
#include "storage/disk/metadata_engine.h"

namespace bustub {
class ObjectIO;
/** Exclusive permission to fill an existing buffer, retained through real IO.
 * Owner includes memory lifetime AND the caller's content access protection. */
struct ObjectReadTarget {
  void *data_;
  size_t capacity_;
  std::shared_ptr<void> owner_;
};
// Only a batch which fits the configured executor can report temporary pressure.
class ObjectIOBusy : public MetadataError {
 public:
  ObjectIOBusy() : MetadataError(MetadataErrorCode::ResourceUnavailable, "object IO capacity is held") {}
};
struct ObjectReadData;
struct ObjectWriteData;

/** Owns the read result, not the physical protection after completion. Dropping
 * a ticket or a WaitFor timeout does not cancel accepted IO. CopyTo exposes only
 * complete successful data; holes are zero and length is clipped at object EOF.
 */
class ObjectRead {
 public:
  ~ObjectRead();
  ObjectRead(ObjectRead &&other) noexcept;
  auto operator=(ObjectRead &&other) noexcept -> ObjectRead &;
  auto Size() const -> size_t;
  void Wait() const;
  auto WaitFor(std::chrono::milliseconds timeout) const -> bool;
  void CopyTo(void *destination, size_t capacity) const;
  /** Complete a ReadInto request; checks failures and copies only planned edges. */
  void Finish() const;

 private:
  friend class ObjectIO;
  explicit ObjectRead(std::unique_ptr<ObjectReadData> data);
  std::unique_ptr<ObjectReadData> data_;
};

/** Newly allocated data. Durable IO is necessary but does not publish mappings.
 * The reservation survives for explicit publication; a ticket from a previous
 * open instance cannot publish into a new one. No arbitrary physical writes.
 * The owner must serialize publication/move/destruction of the same ticket.
 */
class ObjectWrite {
 public:
  ~ObjectWrite();
  ObjectWrite(ObjectWrite &&other) noexcept;
  auto operator=(ObjectWrite &&other) noexcept -> ObjectWrite &;
  auto Size() const -> size_t;
  void Wait() const;
  auto WaitFor(std::chrono::milliseconds timeout) const -> bool;
  auto Result() const -> const IOBatchResult &;

 private:
  friend class ObjectIO;
  explicit ObjectWrite(std::unique_ptr<ObjectWriteData> data);
  std::unique_ptr<ObjectWriteData> data_;
};
}  // namespace bustub
