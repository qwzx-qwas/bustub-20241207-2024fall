#pragma once

#include <utility>
#include <vector>

#include "storage/disk/object_transaction.h"

namespace bustub {
struct ObjectChange {
  ObjectOperation operation_;
  ObjectKey object_;
  uint64_t version_;
  uint64_t offset_;
  uint64_t length_;
  ObjectSizeMode mode_;
  std::vector<StorageByteRange> extents_;
  bool deferred_{false};
  std::vector<uint32_t> checksums_{};
};
struct DeferredTarget {
  ObjectKey object_;
  uint64_t allocation_, offset_, capacity_;
  JournalPayload payload_;
};
struct ObjectGCPage {
  MetadataKey next_;
  std::vector<std::pair<ObjectKey, uint64_t>> candidates_;
};
struct ObjectUnitChecksum {
  ObjectKey owner_;
  uint64_t allocation_, offset_;
  uint32_t crc_;
};
struct ObjectScrubCandidate {
  MetadataKey next_;
  std::optional<ObjectSpan> span_;
  ObjectKey owner_{};
  bool end_{false};
  bool covered_{false};
};
struct PreparedObjectChange {
  std::vector<MetadataMutation> mutations_;
  std::vector<MetadataPayloadMutation> payloads_;
};
/** Private production bridge, not a public metadata/schema escape hatch. */
struct ObjectMappingAccess {
  static auto ScrubMetadata(ObjectMappingStore &store) -> bool;
  static auto Unit(ObjectMappingStore &store) -> uint64_t;
  static auto Checksummed(ObjectMappingStore &store) -> bool;
  static auto Checksums(const ObjectMappingSnapshot &base, ObjectKey owner, uint64_t allocation, uint64_t offset,
                        uint64_t length) -> std::vector<ObjectUnitChecksum>;
  static auto ScrubCandidate(const ObjectMappingSnapshot &base, MetadataKey cursor) -> ObjectScrubCandidate;
  static void Preflight(ObjectMappingStore &store, const ObjectMappingSnapshot &base,
                        const std::vector<ObjectChange> &operations, DataReservation *reservation,
                        const std::vector<ObjectControlMutation> &controls);
  static auto ReplaceChecked(ObjectMappingStore &store, const ObjectMappingSnapshot &base, ObjectKey object,
                             uint64_t offset, uint64_t length, DataReservation &reservation,
                             const std::vector<uint32_t> &checksums) -> JournalResult;
  static auto SupportsDeferred(ObjectMappingStore &store) -> bool;
  static auto Apply(ObjectMappingStore &store, const ObjectMappingSnapshot &base,
                    const std::vector<ObjectChange> &operations, DataReservation *reservation,
                    const std::vector<ObjectControlMutation> &controls,
                    const std::vector<std::vector<std::byte>> &payloads = {}) -> JournalResult;

 private:
  static auto Prepare(ObjectMappingStore &store, const ObjectMappingSnapshot &base,
                      const std::vector<ObjectChange> &operations, DataReservation *reservation,
                      const std::vector<ObjectControlMutation> &controls,
                      const std::vector<std::vector<std::byte>> &payloads, bool preflight) -> PreparedObjectChange;

 public:
  static auto Pending(const ObjectMappingSnapshot &base, size_t limit) -> std::vector<DeferredTarget>;
  static auto Complete(ObjectMappingStore &store, const ObjectMappingSnapshot &base, const DeferredTarget &task)
      -> JournalResult;
  static auto ReadPayload(ObjectMappingStore &store, const JournalPayload &payload, IOReadBudget &budget,
                          std::function<void(const IOBatchResult &)> complete, std::function<void()> ready)
      -> JournalPayloadRead;
  static auto Garbage(const ObjectMappingSnapshot &base, MetadataKey cursor) -> ObjectGCPage;
};
}  // namespace bustub
