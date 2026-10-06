//===----------------------------------------------------------------------===//
// F26: page backend contracts used by the array BufferPool.
//===----------------------------------------------------------------------===//
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <vector>
#include "common/config.h"
#include "storage/disk/object_mapping.h"
#include "storage/disk/object_reference.h"

namespace bustub {
class DiskManager;
class NodeStorage;
struct PageBuffer {
  page_id_t page_;
  char *data_;
  size_t capacity_;
  // Retains the caller's frame and logical content permission through completion.
  std::shared_ptr<void> owner_;
};
/** Page facade over the existing bounded storage execution. Read/Write are
 * synchronous; optional Prefetch transfers completion instead. Operations may
 * overlap on independent pages. The caller holds pin/content rights; backends
 * retain child IO permissions through completion.
 * File compatibility completion is still file flush, not device durability.
 */
class PageStorage {
 public:
  virtual ~PageStorage() = default;
  virtual auto MemoryAlignment() const -> size_t = 0;
  virtual auto MaxBatchPages() const -> size_t = 0;
  virtual auto WriteDomain(page_id_t page) const -> uint64_t = 0;
  virtual void EnsurePages(uint64_t count) = 0;
  virtual void Read(const PageBuffer &buffer) = 0;
  /** Optional non-waiting read. True transfers the completion responsibility;
   * false means no IO. File compatibility has no async executor. */
  virtual auto Prefetch(const PageBuffer &buffer, std::function<void(std::exception_ptr)> complete) -> bool {
    return false;
  }
  virtual void Write(const std::vector<PageBuffer> &buffers) = 0;
  virtual void Delete(page_id_t page) = 0;
  /** Explicit durable retirement, separate from cache/index Delete. File backends
   * cannot atomically publish the replacement link and a recoverable retirement. */
  virtual auto SupportsRetirement() const -> bool { return false; }
  // Synchronous copy-in of a complete replacement page, not a borrowed frame lease.
  virtual void RetirePage(page_id_t page, page_id_t link, const std::array<char, BUSTUB_PAGE_SIZE> &replacement) {
    throw std::logic_error("page backend does not support durable retirement");
  }
  virtual void ReclaimRetiredPages() {}
};
auto FilePageStorage(DiskManager *disk) -> std::shared_ptr<PageStorage>;

struct ObjectPageOptions {
  uint32_t pages_per_object_;
  ObjectKey registry_;  // A dedicated control owner in the node's provisioned space.
};
class ObjectPageCapture {
 public:
  ObjectPageCapture(ObjectPageCapture &&) noexcept = default;
  auto operator=(ObjectPageCapture &&) noexcept -> ObjectPageCapture & = default;

 private:
  friend class ObjectPageStorage;
  ObjectPageCapture(ObjectMappingSnapshot view, uint64_t space, uint64_t objects, uint64_t next,
                    ObjectPageOptions options, std::optional<std::vector<page_id_t>> pages)
      : view_(std::move(view)),
        space_(space),
        objects_(objects),
        next_(next),
        options_(options),
        pages_(std::move(pages)) {}
  ObjectMappingSnapshot view_;
  uint64_t space_, objects_, next_;
  ObjectPageOptions options_;
  std::optional<std::vector<page_id_t>> pages_;
  std::vector<ObjectReadLease> leases_;
};
/** A private A working space. Descriptor encoding v1 records page size and K in
 * B. No catalog/snapshot authority is implied by these working pages. Rebuilds
 * create a fresh space; Open only accepts its explicit persistent identity. */
class ObjectPageStorage : public PageStorage {
 public:
  static auto Create(std::shared_ptr<NodeStorage> storage, ObjectPageOptions options)
      -> std::shared_ptr<ObjectPageStorage>;
  static auto Open(std::shared_ptr<NodeStorage> storage, uint64_t space, ObjectPageOptions options)
      -> std::shared_ptr<ObjectPageStorage>;
  /** Startup only, after proving no live workspace from the previous process. */
  static void RetireAbandoned(std::shared_ptr<NodeStorage> storage, ObjectPageOptions options);
  static void RetireAbandoned(std::shared_ptr<NodeStorage> storage, ObjectPageOptions options,
                              const std::vector<uint64_t> &retained);
  auto Capture(uint64_t next_page, std::optional<std::vector<page_id_t>> pages) -> ObjectPageCapture;
  static auto Clone(std::shared_ptr<NodeStorage> storage, const ObjectPageCapture &capture)
      -> std::shared_ptr<ObjectPageStorage>;
  /** Persist an immutable page-space boundary before publishing its manifest. */
  void Seal(uint64_t next_page);
  auto SealedPageCount() const -> uint64_t;
  auto Space() const -> uint64_t;
  auto MemoryAlignment() const -> size_t override;
  auto MaxBatchPages() const -> size_t override;
  auto WriteDomain(page_id_t page) const -> uint64_t override;
  void EnsurePages(uint64_t count) override;
  void Read(const PageBuffer &buffer) override;
  auto Prefetch(const PageBuffer &buffer, std::function<void(std::exception_ptr)> complete) -> bool override;
  void Write(const std::vector<PageBuffer> &buffers) override;
  void Delete(page_id_t page) override;
  auto SupportsRetirement() const -> bool override { return true; }
  void RetirePage(page_id_t page, page_id_t link, const std::array<char, BUSTUB_PAGE_SIZE> &replacement) override;
  void ReclaimRetiredPages() override;
  /** Explicitly retire an abandoned working space after guards/calls drain. */
  void Retire();

 private:
  struct Impl;
  explicit ObjectPageStorage(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;

 public:
  ~ObjectPageStorage() override;
};
}  // namespace bustub
