//===----------------------------------------------------------------------===//
// BusTub: F13 ordinary-object metadata and bounded extent lookup.
//===----------------------------------------------------------------------===//
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "storage/disk/data_allocator.h"

namespace bustub {

struct ObjectKey {
  uint64_t space_;
  uint64_t number_;
};
enum class ObjectSizeMode : uint8_t { Variable = 0, Fixed = 1 };
struct ObjectInfo {
  uint64_t size_;
  uint64_t version_;
  ObjectSizeMode mode_;
};
struct ObjectMappingOptions {
  size_t max_query_spans_;
  size_t max_update_entries_;
  uint64_t max_update_bytes_;
  uint64_t max_retired_records_;
  size_t max_active_operations_;
};
enum class ObjectMappingErrorCode { NotInitialized, AlreadyExists, NotFound, Corrupt, ResourceUnavailable, NotReady };
class ObjectMappingError : public std::runtime_error {
 public:
  ObjectMappingError(ObjectMappingErrorCode code, const std::string &message)
      : std::runtime_error(message), code_(code) {}
  auto Code() const -> ObjectMappingErrorCode { return code_; }

 private:
  ObjectMappingErrorCode code_;
};
struct ObjectDataLocation {
  uint64_t offset_;      // Data-region-relative bytes, not a device address.
  uint64_t allocation_;  // Identity is (space, object number, allocation).
};
struct ObjectSpan {
  uint64_t offset_;
  uint64_t size_;
  std::optional<ObjectDataLocation> data_;  // nullopt is a logical zero range.
};
struct ObjectReadPage {
  uint64_t next_offset_;
  bool complete_;
  std::vector<ObjectSpan> spans_;
};
struct RetiredObjectRange {
  uint64_t id_;
  uint64_t removed_version_;
  ObjectSpan span_;
};
struct RetiredRangePage {
  uint64_t next_id_;
  bool complete_;
  std::vector<RetiredObjectRange> ranges_;
};
struct ObjectMappingContext;
/** Immutable metadata view. It is NOT a physical-data pin. F14 must protect
 * actual reads before physical release is enabled. Pagination uses this same
 * view, not a fresh snapshot. Returned vectors belong to the caller.
 */
class ObjectMappingSnapshot {
 public:
  auto Describe(ObjectKey key) const -> ObjectInfo;
  auto Resolve(ObjectKey key, uint64_t offset, uint64_t length) const -> ObjectReadPage;
  /** F14 handoff, inclusive id cursor (starts at zero), also valid for tombstones. */
  auto Retired(ObjectKey key, uint64_t from_id) const -> RetiredRangePage;
  auto Control(ObjectKey owner, uint64_t item) const -> std::optional<std::vector<std::byte>>;

 private:
  friend class ObjectMappingStore;
  friend class ObjectReferenceManager;
  friend struct ObjectMappingAccess;
  ObjectMappingSnapshot(MetadataSnapshot base, std::shared_ptr<ObjectMappingContext> context);
  MetadataSnapshot base_;
  std::shared_ptr<ObjectMappingContext> context_;
};
struct ObjectSpaceCreation {
  JournalResult result_;
  // Published only for Durable. Indeterminate is not permission to retry blindly.
  std::optional<uint64_t> space_;
};
/** One lifecycle-owned store uses one existing allocator and its B. Create/Open
 * are explicit and serialized by the owner. B/allocator outlive all store calls.
 * Ordinary calls may overlap; B rejects stale bases, with no hidden retry.
 *
 * Replace consumes only a genuine pending allocation. Caller must initialize
 * and durably write its data first, retaining F12 leases through actual IO.
 * This module publishes mapping facts, not a complete WritePlanner/StorageAPI.
 * Removed ranges are persisted here; F14 owns safe physical release. No data-cache, second
 * WAL, shared snapshots or NodeStorage integration are implied.
 * Only this store and its future F14 consumer may modify its B key namespaces.
 */
class ObjectMappingStore {
 public:
  ObjectMappingStore(DataAllocator &allocator, ObjectMappingOptions options);
  ~ObjectMappingStore();
  ObjectMappingStore(const ObjectMappingStore &) = delete;
  auto operator=(const ObjectMappingStore &) -> ObjectMappingStore & = delete;
  auto Create() -> JournalResult;
  void Open();
  auto Read() const -> ObjectMappingSnapshot;
  auto CreateSpace(const ObjectMappingSnapshot &base) -> ObjectSpaceCreation;
  auto CreateObject(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t length, ObjectSizeMode mode)
      -> JournalResult;
  auto Replace(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t offset, uint64_t length,
               DataReservation &reservation) -> JournalResult;
  auto Resize(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t length) -> JournalResult;
  auto Remove(const ObjectMappingSnapshot &base, ObjectKey key) -> JournalResult;

 private:
  friend class ObjectReferenceManager;
  friend struct ObjectMappingAccess;
  auto ReferenceContext() const -> std::shared_ptr<ObjectMappingContext>;
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bustub
