//===----------------------------------------------------------------------===//
// BusTub: F12 persistent Data-region allocation with volatile reservations.
//===----------------------------------------------------------------------===//
#pragma once

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "storage/disk/metadata_engine.h"

namespace bustub {

struct DataAllocatorOptions {
  uint64_t allocation_bytes_;
  uint32_t bitmap_record_bytes_;
  uint64_t max_bitmap_bytes_;
  uint64_t max_request_bytes_;
  size_t max_extents_;
  size_t max_reservations_;
  uint64_t max_search_nodes_;
};

enum class AllocationErrorCode {
  NotInitialized,
  AlreadyInitialized,
  Corrupt,
  NoSpace,
  ResourceUnavailable,  // Request/search limits; waiting alone cannot guarantee progress.
  NotReady,
  Busy  // Reservation slots are held by other operations; retry after release.
};
class AllocationError : public std::runtime_error {
 public:
  AllocationError(AllocationErrorCode code, const std::string &message) : std::runtime_error(message), code_(code) {}
  auto Code() const -> AllocationErrorCode { return code_; }

 private:
  AllocationErrorCode code_;
};

struct AllocationReservationState;
/** Keeps an uncommitted reservation unavailable until the last lease exits.
 * The caller carries this lease through actual F04/F02 IO completion. It does
 * not execute IO, prove durability, or replace F14's committed-content pins.
 */
class DataAllocationLease {
 private:
  friend class DataReservation;
  explicit DataAllocationLease(std::shared_ptr<AllocationReservationState> state);
  std::shared_ptr<AllocationReservationState> state_;
};

class DataReservation {
 public:
  DataReservation(DataReservation &&) noexcept = default;
  auto operator=(DataReservation &&) noexcept -> DataReservation & = default;
  DataReservation(const DataReservation &) = delete;
  auto operator=(const DataReservation &) -> DataReservation & = delete;
  auto Extents() const -> const std::vector<StorageByteRange> &;
  auto Lease() const -> DataAllocationLease;

 private:
  friend class DataAllocator;
  explicit DataReservation(std::shared_ptr<AllocationReservationState> state);
  std::shared_ptr<AllocationReservationState> state_;
};

/** One allocator exclusively owns B category 12. Other clients may update other
 * categories through B; they may never modify allocator records behind it.
 * The Data geometry is obtained privately from this B's existing region binding.
 * B outlives this allocator and all its calls. Create/Open are explicit and
 * lifecycle-owner serialized; reopen uses a fresh allocator after closing B.
 *
 * Reserve may overlap commits. One allocator commit role serializes C/Q changes,
 * without holding the bitmap mutex across B or IO. Companion metadata shares
 * the SAME B transaction. Stale bases reject; there is no automatic retry.
 * Accepted commit failures stop admission until a fresh recovery.
 *
 * Release requires the caller to have removed/validated persistent references
 * against the supplied base and excluded new readers and in-flight IO. F12 does
 * not invent F13/F14 ownership or GC. Partial extents must be whole units.
 * Close stops admission and drains allocator commits; external IO remains the
 * caller's responsibility. Leases may outlive Close and keep only old RAM state.
 */
class DataAllocator {
 public:
  DataAllocator(MetadataEngine &metadata, DataAllocatorOptions options);
  ~DataAllocator();
  DataAllocator(const DataAllocator &) = delete;
  auto operator=(const DataAllocator &) -> DataAllocator & = delete;

  auto Create() -> JournalResult;
  void Open();
  auto Reserve(uint64_t bytes) -> DataReservation;
  /** Bounds the companion records plus actual bitmap records before data IO. */
  void CheckCommit(const DataReservation &reservation, const std::vector<MetadataMutation> &related) const;
  auto Commit(const MetadataSnapshot &base, DataReservation &reservation, const std::vector<MetadataMutation> &related)
      -> JournalResult;
  auto Commit(const MetadataSnapshot &base, DataReservation &reservation, const std::vector<MetadataMutation> &related,
              const std::vector<MetadataPayloadMutation> &payloads) -> JournalResult;
  auto Release(const MetadataSnapshot &base, const std::vector<StorageByteRange> &ranges,
               const std::vector<MetadataMutation> &related) -> JournalResult;
  /** Explicit isolation; never automatic diagnosis/healing after an IO error.
   * A range with a live reservation cannot change quarantine state.
   */
  auto SetQuarantine(const MetadataSnapshot &base, const std::vector<StorageByteRange> &ranges, bool isolated)
      -> JournalResult;
  void Close();

 private:
  friend class ObjectMappingStore;
  auto Metadata() const -> MetadataEngine &;
  auto AllocationUnit() const -> uint64_t;
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bustub
