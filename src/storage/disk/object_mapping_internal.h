//===----------------------------------------------------------------------===//
// Shared PRIVATE schema and binding for F13 mapping and F14 references.
//===----------------------------------------------------------------------===//
#pragma once

#include <atomic>
#include <limits>
#include <map>
#include <tuple>
#include <utility>
#include <vector>

#include "common/byte_codec.h"
#include "storage/disk/object_mapping.h"

namespace bustub {
struct ObjectMappingContext {
  ObjectMappingContext(ObjectMappingOptions options, uint64_t unit, MetadataEngine &metadata, DataAllocator &allocator)
      : options_(options), unit_(unit), metadata_(metadata), allocator_(allocator) {}
  ObjectMappingOptions options_;
  uint64_t unit_;
  MetadataEngine &metadata_;
  DataAllocator &allocator_;
  uint64_t format_{0};              // Published by the serialized lifecycle owner before ordinary operations.
  bool references_started_{false};  // One F14 context for this store lifetime.
  std::atomic<size_t> active_{0};
};
namespace object_mapping_detail {
constexpr uint64_t CONTROL = 13;
constexpr uint64_t MAGIC = 0x4253544f424a4d50ULL;  // BSTOBJMP
constexpr uint64_t FORMAT = 4;                     // v4 adds durable shared content ownership.
constexpr uint64_t SPACE_LIMIT = (uint64_t{1} << 56) - 1;
enum Kind : uint64_t {
  Space = 1,
  Descriptor = 2,
  Mapping = 3,
  Allocation = 4,
  PendingRange = 5,
  OwnedRange = 6,
  Control = 7,
  Deferred = 8,
  SharedUnit = 9
};
using Bytes = std::vector<std::byte>;
[[noreturn]] inline void Fail(ObjectMappingErrorCode code, const char *message) {
  throw ObjectMappingError(code, message);
}
inline void Require(bool valid, const char *message) {
  if (!valid) {
    Fail(ObjectMappingErrorCode::Corrupt, message);
  }
}
inline auto End(uint64_t offset, uint64_t size) -> uint64_t {
  const auto range = StorageByteRange::Create(offset, size);
  if (!range) {
    throw std::invalid_argument("object range overflows");
  }
  return offset + size;
}
inline auto Advance(uint64_t value) -> uint64_t {
  if (value == std::numeric_limits<uint64_t>::max()) {
    Fail(ObjectMappingErrorCode::ResourceUnavailable, "object sequence exhausted");
  }
  return value + 1;
}
inline auto Key(Kind kind, ObjectKey object, uint64_t item) -> MetadataKey {
  if (object.space_ == 0 || object.space_ > SPACE_LIMIT) {
    throw std::invalid_argument("object requires a valid persistent space identity");
  }
  return {(static_cast<uint64_t>(kind) << 56) | object.space_, object.number_, item};
}
inline auto SamePrefix(const MetadataKey &a, const MetadataKey &b) -> bool {
  return a.category_ == b.category_ && a.owner_ == b.owner_;
}
inline auto Encode(std::initializer_list<uint64_t> fields) -> Bytes {
  ByteWriter w;
  for (auto value : fields) {
    w.PutU64(value);
  }
  return w.Take();
}
inline auto Decode(const Bytes &bytes, size_t fields) -> std::vector<uint64_t> {
  Require(bytes.size() == fields * 8, "object metadata record length mismatch");
  ByteReader reader(bytes);
  std::vector<uint64_t> result;
  result.reserve(fields);
  for (size_t i = 0; i < fields; ++i) {
    result.push_back(reader.ReadU64());
  }
  return result;
}
struct Active {
  explicit Active(ObjectMappingContext &context) : context_(context) {
    if (context_.active_.fetch_add(1) >= context_.options_.max_active_operations_) {
      --context_.active_;
      Fail(ObjectMappingErrorCode::ResourceUnavailable, "object operation budget full");
    }
  }
  ~Active() { --context_.active_; }
  ObjectMappingContext &context_;
};
struct Description {
  bool alive_;
  ObjectInfo info_;
  uint64_t next_allocation_, next_retired_, pending_;
};
inline auto ReadDescription(const MetadataSnapshot &base, ObjectKey key, bool allow_deleted = false) -> Description {
  const auto value = base.Get(Key(Descriptor, key, 0));
  if (!value) {
    Fail(ObjectMappingErrorCode::NotFound, "object does not exist");
  }
  const auto f = Decode(*value, 7);
  Require(f[0] <= 1 && f[1] <= 1 && f[3] != 0 && f[4] != 0 && f[5] != 0 && f[6] < f[5], "invalid object descriptor");
  if (f[0] == 0 && !allow_deleted) {
    Fail(ObjectMappingErrorCode::NotFound, "object was deleted");
  }
  return {f[0] != 0, {f[2], f[3], static_cast<ObjectSizeMode>(f[1])}, f[4], f[5], f[6]};
}
inline auto EncodeDescription(const Description &d) -> Bytes {
  return Encode({d.alive_, static_cast<uint64_t>(d.info_.mode_), d.info_.size_, d.info_.version_, d.next_allocation_,
                 d.next_retired_, d.pending_});
}
inline auto ReadSpan(const MetadataEntry &entry) -> ObjectSpan {
  const auto f = Decode(entry.value_, entry.value_.size() == 40 ? 5 : 3);
  Require(f[0] != 0 && f[2] != 0 && StorageByteRange::Create(entry.key_.item_, f[0]).has_value() &&
              StorageByteRange::Create(f[1], f[0]).has_value(),
          "invalid object extent");
  return {entry.key_.item_, f[0],
          ObjectDataLocation{f[1], f[2], f.size() == 5 ? std::optional<ObjectKey>{{f[3], f[4]}} : std::nullopt}};
}
inline auto Slice(const ObjectSpan &span, uint64_t begin, uint64_t end) -> ObjectSpan {
  return {
      begin, end - begin,
      ObjectDataLocation{span.data_->offset_ + (begin - span.offset_), span.data_->allocation_, span.data_->owner_}};
}
struct KeyLess {
  auto operator()(const MetadataKey &a, const MetadataKey &b) const -> bool {
    return std::tie(a.category_, a.owner_, a.item_) < std::tie(b.category_, b.owner_, b.item_);
  }
};
struct Changes {
  explicit Changes(const ObjectMappingOptions &options) : options_(options) {}
  void Put(MetadataKey key, std::optional<Bytes> value) {
    const auto old = values_.find(key);
    const auto prior = old == values_.end() ? 0 : 32 + (old->second ? old->second->size() : 0);
    const auto added = 32 + (value ? value->size() : 0);
    if ((old == values_.end() && values_.size() == options_.max_update_entries_) ||
        added > options_.max_update_bytes_ - (bytes_ - prior)) {
      Fail(ObjectMappingErrorCode::ResourceUnavailable, "object update exceeds metadata budget");
    }
    values_.insert_or_assign(key, std::move(value));
    bytes_ = bytes_ - prior + added;
  }
  void Map(ObjectKey key, const ObjectSpan &span) {
    const auto &p = *span.data_;
    Put(Key(Mapping, key, span.offset_),
        p.owner_ ? Encode({span.size_, p.offset_, p.allocation_, p.owner_->space_, p.owner_->number_})
                 : Encode({span.size_, p.offset_, p.allocation_}));
  }
  void Share(const MetadataSnapshot &base, const ObjectSpan &span, uint64_t unit, bool acquire) {
    if (!span.data_ || !span.data_->owner_) return;
    const auto &p = *span.data_;
    const auto last = (End(p.offset_, span.size_) - 1) / unit;
    for (auto u = p.offset_ / unit; u <= last; ++u) {
      const auto key = Key(SharedUnit, *p.owner_, u);
      const auto found = values_.find(key);
      const auto value = found == values_.end() ? base.Get(key) : found->second;
      uint64_t count = 0;
      if (value) {
        const auto fields = Decode(*value, 2);
        Require(fields[0] != 0 && fields[1] == p.allocation_, "shared content identity changed");
        count = fields[0];
      }
      if (acquire)
        count = Advance(count);
      else {
        Require(count != 0, "shared content reference missing");
        --count;
      }
      Put(key, count == 0 ? std::nullopt : std::optional<Bytes>{Encode({count, p.allocation_})});
    }
  }
  void Retire(ObjectKey key, Description &d, const ObjectSpan &span) {
    if (d.pending_ >= options_.max_retired_records_) {
      Fail(ObjectMappingErrorCode::ResourceUnavailable, "object retirement backlog full");
    }
    const auto id = d.next_retired_;
    d.next_retired_ = Advance(id);
    ++d.pending_;
    Put(Key(PendingRange, key, id),
        Encode({d.info_.version_, span.offset_, span.size_, span.data_->offset_, span.data_->allocation_}));
  }
  auto Take() -> std::vector<MetadataMutation> {
    std::vector<MetadataMutation> result;
    result.reserve(values_.size());
    for (auto &[key, value] : values_) {
      result.push_back({key, std::move(value)});
    }
    return result;
  }
  const ObjectMappingOptions &options_;
  uint64_t bytes_{0};
  std::map<MetadataKey, std::optional<Bytes>, KeyLess> values_;
};
struct ObjectControl {
  uint64_t format_;
  uint64_t last_space_;
};
inline auto ReadControl(const MetadataSnapshot &base) -> ObjectControl {
  const auto value = base.Get({CONTROL, 0, 0});
  if (!value) {
    Fail(ObjectMappingErrorCode::NotInitialized, "object format missing; Open never creates");
  }
  const auto f = Decode(*value, 3);
  Require(f[0] == MAGIC && f[1] >= 1 && f[1] <= FORMAT && f[2] <= SPACE_LIMIT,
          "invalid object format or space sequence");
  return {f[1], f[2]};
}
}  // namespace object_mapping_detail
}  // namespace bustub
