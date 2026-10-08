//===----------------------------------------------------------------------===//
// BusTub: F14 bounded range protection and transactional Data reclamation.
//===----------------------------------------------------------------------===//
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "storage/disk/object_mapping.h"

namespace bustub {
struct ObjectReferenceOptions {
  size_t max_leases_;  // Busy until another retained read protection is released.
  size_t max_ranges_per_lease_;
  size_t max_scan_entries_;
  size_t max_reclaim_ranges_;
  uint64_t max_reclaim_bytes_;
  size_t max_active_operations_;  // Admission returns Busy while all call slots are in use.
};
enum class ObjectReferenceErrorCode { Busy, Stale, UnsupportedFormat };
class ObjectReferenceError : public std::runtime_error {
 public:
  ObjectReferenceError(ObjectReferenceErrorCode code, const std::string &message)
      : std::runtime_error(message), code_(code) {}
  auto Code() const -> ObjectReferenceErrorCode { return code_; }

 private:
  ObjectReferenceErrorCode code_;
};
struct ObjectReadProtection;
/** A copy keeps the SAME range protection alive. Carry a copy in the IO owner
 * (or existing F02 external-buffer release callback) until actual completion.
 * Spans describe the entire requested read, clipped at EOF; holes return zero.
 * Data IO may expand only within the protected allocation units. This is not a
 * durable snapshot, a RAM buffer permission, or a license to overwrite data.
 */
class ObjectReadLease {
 public:
  auto Spans() const -> const std::vector<ObjectSpan> &;

 private:
  friend class ObjectReferenceManager;
  explicit ObjectReadLease(std::shared_ptr<ObjectReadProtection> protection);
  std::shared_ptr<ObjectReadProtection> protection_;
};
struct ObjectReclaimResult {
  // Empty means no metadata transaction was submitted, not a synthetic Flush.
  std::optional<JournalResult> commit_;
  uint64_t released_bytes_{0};  // Nonzero only after Durable.
  uint64_t pinned_bytes_{0};    // Eligible whole units blocked by existing readers.
};
/** One lifecycle-owned manager per opened v2 ObjectMappingStore. The store, B
 * and allocator outlive this manager and its calls. Old v1 remains usable by
 * F13 but cannot enable F14; no automatic migration. No private worker/queue.
 *
 * Reclaim consumes F13's irreversible S6 retirement facts. S11 persistent
 * shared versions must extend that predicate before they are enabled. Protect
 * checks current ownership, so a metadata snapshot alone cannot resurrect a
 * freed location. Reclaim skips pinned units and never waits for readers/IO
 * while holding the range mutex. Callers schedule subsequent bounded work.
 */
class ObjectReferenceManager {
 public:
  ObjectReferenceManager(ObjectMappingStore &mapping, ObjectReferenceOptions options);
  ~ObjectReferenceManager();
  ObjectReferenceManager(const ObjectReferenceManager &) = delete;
  auto operator=(const ObjectReferenceManager &) -> ObjectReferenceManager & = delete;
  auto ProtectRead(const ObjectMappingSnapshot &view, ObjectKey object, uint64_t offset, uint64_t length)
      -> ObjectReadLease;
  auto Reclaim(const ObjectMappingSnapshot &base, ObjectKey object, uint64_t allocation) -> ObjectReclaimResult;
  /** Stop admission, drain entered calls. Retained leases remain attached to
   * the old RAM context. The lifecycle owner must drain actual Data IO before
   * reopening B/allocator/store; closing this manager is not an IO barrier.
   */
  void Close();

 private:
  friend class ObjectIO;
  auto ProtectOwned(ObjectKey owner, const ObjectSpan &span) -> ObjectReadLease;
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
}  // namespace bustub
