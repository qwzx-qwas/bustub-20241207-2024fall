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
};
struct ObjectGCPage {
  MetadataKey next_;
  std::vector<std::pair<ObjectKey, uint64_t>> candidates_;
};
/** Private production bridge, not a public metadata/schema escape hatch. */
struct ObjectMappingAccess {
  static auto Unit(ObjectMappingStore &store) -> uint64_t;
  static auto Apply(ObjectMappingStore &store, const ObjectMappingSnapshot &base,
                    const std::vector<ObjectChange> &operations, DataReservation *reservation,
                    const std::vector<ObjectControlMutation> &controls) -> JournalResult;
  static auto Garbage(const ObjectMappingSnapshot &base, MetadataKey cursor) -> ObjectGCPage;
};
}  // namespace bustub
