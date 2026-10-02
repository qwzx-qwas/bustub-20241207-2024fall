//===----------------------------------------------------------------------===//
// BusTub: object ranges, atomic allocation publication and durable retirement.
//===----------------------------------------------------------------------===//
#include "storage/disk/object_mapping.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "object_mapping_internal.h"  // NOLINT(build/include_subdir): private sibling schema.

namespace bustub {
using namespace object_mapping_detail;  // NOLINT(build/namespaces)
namespace {
// Remove only intersections. Unchanged left/right slices retain their original
// allocation identity. The retired part is not automatically safe to free.
void Cut(const MetadataSnapshot &base, ObjectKey key, uint64_t begin, uint64_t end, Description &d, Changes &changes) {
  if (begin >= end) {
    return;
  }
  auto lower = Key(Mapping, key, begin);
  const auto floor = base.GetFloor(lower);
  if (floor && SamePrefix(floor->key_, lower)) {
    const auto span = ReadSpan(*floor);
    if (span.offset_ + span.size_ > begin) {
      lower = floor->key_;
    }
  }
  size_t affected = 0;
  for (const auto &entry : base.Scan(lower, changes.options_.max_update_entries_ + 1)) {
    if (!SamePrefix(entry.key_, lower) || entry.key_.item_ >= end) {
      break;
    }
    if (++affected > changes.options_.max_update_entries_) {
      Fail(ObjectMappingErrorCode::ResourceUnavailable, "object update touches too many mappings");
    }
    const auto span = ReadSpan(entry);
    const auto stop = span.offset_ + span.size_;
    changes.Put(entry.key_, std::nullopt);
    if (span.offset_ < begin) {
      changes.Map(key, Slice(span, span.offset_, begin));
    }
    if (stop > end) {
      changes.Map(key, Slice(span, end, stop));
    }
    changes.Retire(key, d, Slice(span, std::max(begin, span.offset_), std::min(end, stop)));
  }
}
}  // namespace

ObjectMappingSnapshot::ObjectMappingSnapshot(MetadataSnapshot base, std::shared_ptr<ObjectMappingContext> context)
    : base_(std::move(base)), context_(std::move(context)) {}
auto ObjectMappingSnapshot::Describe(ObjectKey key) const -> ObjectInfo {
  Active active(*context_);
  return ReadDescription(base_, key).info_;
}
auto ObjectMappingSnapshot::Resolve(ObjectKey key, uint64_t offset, uint64_t length) const -> ObjectReadPage {
  Active active(*context_);
  const auto requested_end = End(offset, length);
  const auto d = ReadDescription(base_, key);
  const auto end = std::min(requested_end, d.info_.size_);
  ObjectReadPage result{offset, true, {}};
  if (offset >= end) {
    return result;
  }
  auto lower = Key(Mapping, key, offset);
  const auto floor = base_.GetFloor(lower);
  if (floor && SamePrefix(floor->key_, lower)) {
    const auto span = ReadSpan(*floor);
    if (span.offset_ + span.size_ > offset) {
      lower = floor->key_;
    }
  }
  const auto entries = base_.Scan(lower, context_->options_.max_query_spans_ + 1);
  auto add = [&](ObjectSpan span) {
    if (result.spans_.size() == context_->options_.max_query_spans_) {
      return false;
    }
    result.next_offset_ = span.offset_ + span.size_;
    result.spans_.push_back(std::move(span));
    return true;
  };
  for (const auto &entry : entries) {
    if (!SamePrefix(entry.key_, lower) || entry.key_.item_ >= end) {
      break;
    }
    const auto span = ReadSpan(entry);
    if (span.offset_ > result.next_offset_ &&
        !add({result.next_offset_, span.offset_ - result.next_offset_, std::nullopt})) {
      result.complete_ = false;
      return result;
    }
    const auto stop = std::min(end, span.offset_ + span.size_);
    if (!add(Slice(span, result.next_offset_, stop))) {
      result.complete_ = false;
      return result;
    }
    if (stop == end) {
      return result;
    }
  }
  if (result.next_offset_ < end) {
    result.complete_ = add({result.next_offset_, end - result.next_offset_, std::nullopt});
  }
  return result;
}
auto ObjectMappingSnapshot::Retired(ObjectKey key, uint64_t from_id) const -> RetiredRangePage {
  Active active(*context_);
  const auto d = ReadDescription(base_, key, true);
  RetiredRangePage result{from_id, true, {}};
  const auto lower = Key(PendingRange, key, from_id);
  for (const auto &entry : base_.Scan(lower, context_->options_.max_query_spans_ + 1)) {
    if (!SamePrefix(entry.key_, lower)) {
      break;
    }
    if (result.ranges_.size() == context_->options_.max_query_spans_) {
      result.complete_ = false;
      return result;
    }
    const auto f = Decode(entry.value_, 5);
    Require(f[0] <= d.info_.version_ && f[2] != 0 && f[4] != 0 && StorageByteRange::Create(f[1], f[2]).has_value() &&
                StorageByteRange::Create(f[3], f[2]).has_value(),
            "invalid retired object range");
    result.ranges_.push_back({entry.key_.item_, f[0], {f[1], f[2], ObjectDataLocation{f[3], f[4]}}});
    result.next_id_ = Advance(entry.key_.item_);
  }
  return result;
}
struct ObjectMappingStore::Impl {
  Impl(DataAllocator &allocator, MetadataEngine &metadata, uint64_t unit, ObjectMappingOptions options)
      : allocator_(allocator),
        metadata_(metadata),
        context_(std::make_shared<ObjectMappingContext>(options, unit, metadata, allocator)) {
    if (options.max_query_spans_ == 0 || options.max_query_spans_ == std::numeric_limits<size_t>::max() ||
        options.max_update_entries_ == 0 || options.max_update_entries_ == std::numeric_limits<size_t>::max() ||
        options.max_update_bytes_ < 88 || options.max_retired_records_ == 0 || options.max_active_operations_ == 0) {
      throw std::invalid_argument("object mapping requires positive explicit budgets");
    }
  }
  void Ready() const {
    if (!ready_) {
      Fail(ObjectMappingErrorCode::NotReady, "object mapping store is not open");
    }
  }
  void Start() {
    if (started_) {
      throw std::logic_error("object startup requires a fresh store");
    }
    started_ = true;
  }
  void Check(const ObjectMappingSnapshot &base) const {
    Ready();
    if (base.context_ != context_) {
      throw MetadataError(MetadataErrorCode::Conflict, "foreign object metadata snapshot");
    }
  }
  auto Create() -> JournalResult {
    Start();
    const auto base = metadata_.Read();
    if (base.Get({CONTROL, 0, 0})) {
      Fail(ObjectMappingErrorCode::AlreadyExists, "object mapping format already exists");
    }
    auto result = metadata_.Commit(base, {{{CONTROL, 0, 0}, Encode({MAGIC, FORMAT, 0})}});
    ready_ = result.outcome_ == JournalOutcome::Durable;
    if (ready_) {
      context_->format_ = FORMAT;
    }
    return result;
  }
  void Open() {
    Start();
    context_->format_ = ReadControl(metadata_.Read()).format_;
    ready_ = true;
  }
  auto Change(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t begin, uint64_t length,
              DataReservation *reservation, bool remove) -> JournalResult {
    Active active(*context_);
    Check(base);
    auto d = ReadDescription(base.base_, key);
    d.info_.version_ = Advance(d.info_.version_);
    Changes changes(context_->options_);
    if (reservation != nullptr) {
      if (length == 0) {
        throw std::invalid_argument("replacement must contain initialized data");
      }
      auto end = End(begin, length);
      if (d.info_.mode_ == ObjectSizeMode::Fixed && end > d.info_.size_) {
        throw std::invalid_argument("replacement exceeds fixed object length");
      }
      Cut(base.base_, key, begin, end, d, changes);
      uint64_t remaining = length, cursor = begin;
      for (const auto &range : reservation->Extents()) {
        if (remaining == 0) {
          throw std::invalid_argument("only the final allocation may have unused padding");
        }
        const auto id = d.next_allocation_;
        d.next_allocation_ = Advance(id);
        const auto take = std::min(remaining, range.Size());
        if (context_->format_ == 1) {
          changes.Put(Key(Allocation, key, id), Encode({range.Offset(), range.Size(), d.info_.version_}));
        } else {
          if (range.Size() - take >= context_->unit_) {
            throw std::invalid_argument("object allocation padding must be less than one allocation unit");
          }
          changes.Put(Key(Allocation, key, id), Encode({range.Offset(), range.Size(), d.info_.version_, take}));
          changes.Put(Key(OwnedRange, key, range.Offset()), Encode({range.Size(), id}));
        }
        changes.Map(key, {cursor, take, ObjectDataLocation{range.Offset(), id}});
        cursor += take;
        remaining -= take;
      }
      if (remaining != 0) {
        throw std::invalid_argument("reservation is shorter than initialized object data");
      }
      d.info_.size_ = std::max(d.info_.size_, end);
    } else {
      if (!remove && d.info_.mode_ == ObjectSizeMode::Fixed && length != d.info_.size_) {
        throw std::invalid_argument("fixed object cannot change length");
      }
      const auto size = remove ? 0 : length;
      Cut(base.base_, key, size, d.info_.size_, d, changes);
      d.info_.size_ = size;
      d.alive_ = !remove;
    }
    changes.Put(Key(Descriptor, key, 0), EncodeDescription(d));
    const auto mutations = changes.Take();
    return reservation ? allocator_.Commit(base.base_, *reservation, mutations)
                       : metadata_.Commit(base.base_, mutations);
  }
  DataAllocator &allocator_;
  MetadataEngine &metadata_;
  std::shared_ptr<ObjectMappingContext> context_;
  bool started_{false};
  bool ready_{false};  // Written only by lifecycle-owner serialized Create/Open.
};
ObjectMappingStore::ObjectMappingStore(DataAllocator &allocator, ObjectMappingOptions options)
    : impl_(std::make_unique<Impl>(allocator, allocator.Metadata(), allocator.AllocationUnit(), options)) {}
ObjectMappingStore::~ObjectMappingStore() = default;
auto ObjectMappingStore::ReferenceContext() const -> std::shared_ptr<ObjectMappingContext> {
  impl_->Ready();
  return impl_->context_;
}
auto ObjectMappingStore::Create() -> JournalResult { return impl_->Create(); }
void ObjectMappingStore::Open() { impl_->Open(); }
auto ObjectMappingStore::Read() const -> ObjectMappingSnapshot {
  impl_->Ready();
  return ObjectMappingSnapshot(impl_->metadata_.Read(), impl_->context_);
}
auto ObjectMappingStore::CreateSpace(const ObjectMappingSnapshot &base) -> ObjectSpaceCreation {
  auto &s = *impl_;
  Active active(*s.context_);
  s.Check(base);
  const auto control = ReadControl(base.base_);
  const auto last = control.last_space_;
  if (last == SPACE_LIMIT) {
    Fail(ObjectMappingErrorCode::ResourceUnavailable, "object space identities exhausted");
  }
  const auto id = last + 1;
  Changes changes(s.context_->options_);
  changes.Put({CONTROL, 0, 0}, Encode({MAGIC, control.format_, id}));
  changes.Put(Key(Space, {id, 0}, 0), Encode({id}));
  auto result = s.metadata_.Commit(base.base_, changes.Take());
  auto published = result.outcome_ == JournalOutcome::Durable ? std::optional<uint64_t>(id) : std::nullopt;
  return {std::move(result), published};
}
auto ObjectMappingStore::CreateObject(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t length,
                                      ObjectSizeMode mode) -> JournalResult {
  auto &s = *impl_;
  Active active(*s.context_);
  s.Check(base);
  const auto space = base.base_.Get(Key(Space, {key.space_, 0}, 0));
  if (!space) {
    Fail(ObjectMappingErrorCode::NotFound, "object space does not exist");
  }
  Require(*space == Encode({key.space_}), "object space identity mismatch");
  if (mode != ObjectSizeMode::Variable && mode != ObjectSizeMode::Fixed) {
    throw std::invalid_argument("invalid object size mode");
  }
  const auto descriptor = Key(Descriptor, key, 0);
  if (base.base_.Get(descriptor)) {
    Fail(ObjectMappingErrorCode::AlreadyExists, "object number cannot be reused, including tombstones");
  }
  Changes changes(s.context_->options_);
  changes.Put(descriptor, EncodeDescription({true, {length, 1, mode}, 1, 1, 0}));
  return s.metadata_.Commit(base.base_, changes.Take());
}
auto ObjectMappingStore::Replace(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t offset, uint64_t length,
                                 DataReservation &reservation) -> JournalResult {
  return impl_->Change(base, key, offset, length, &reservation, false);
}
auto ObjectMappingStore::Resize(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t length) -> JournalResult {
  return impl_->Change(base, key, 0, length, nullptr, false);
}
auto ObjectMappingStore::Remove(const ObjectMappingSnapshot &base, ObjectKey key) -> JournalResult {
  return impl_->Change(base, key, 0, 0, nullptr, true);
}
}  // namespace bustub
