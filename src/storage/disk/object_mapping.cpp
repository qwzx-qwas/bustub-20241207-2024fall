//===----------------------------------------------------------------------===//
// BusTub: object ranges, atomic allocation publication and durable retirement.
//===----------------------------------------------------------------------===//
#include "storage/disk/object_mapping.h"

#include <algorithm>
#include <limits>
#include <set>
#include <utility>

#include "object_change_internal.h"   // NOLINT(build/include_subdir): private sibling component.
#include "object_mapping_internal.h"  // NOLINT(build/include_subdir): private sibling schema.
#include "storage/disk/object_io.h"

namespace bustub {
using namespace object_mapping_detail;  // NOLINT: shared private mapping schema, no public namespace import.
namespace {
// Remove only intersections. Unchanged left/right slices retain their original
// allocation identity. The retired part is not automatically safe to free.
void Cut(const MetadataSnapshot &base, ObjectKey key, uint64_t begin, uint64_t cut_end, Description &d,
         Changes &changes, uint64_t unit) {
  if (begin >= cut_end) {
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
    if (!SamePrefix(entry.key_, lower) || entry.key_.item_ >= cut_end) {
      break;
    }
    if (++affected > changes.options_.max_update_entries_) {
      Fail(ObjectMappingErrorCode::ResourceUnavailable, "object update touches too many mappings");
    }
    const auto span = ReadSpan(entry);
    const auto stop = span.offset_ + span.size_;
    changes.Put(entry.key_, std::nullopt);
    changes.Share(base, span, unit, false);
    if (span.offset_ < begin) {
      changes.Map(key, Slice(span, span.offset_, begin));
      changes.Share(base, Slice(span, span.offset_, begin), unit, true);
    }
    if (stop > cut_end) {
      // The surviving right slice starts at the END of the removed interval.
      changes.Map(key, Slice(span, cut_end, stop));  // NOLINT: the right slice starts at cut_end and ends at stop.
      changes.Share(base, Slice(span, cut_end, stop), unit, true);
    }
    if (!span.data_->owner_) {
      changes.Retire(key, d, Slice(span, std::max(begin, span.offset_), std::min(cut_end, stop)));
    }
  }
}

void ReplaceChanges(const MetadataSnapshot &base, ObjectKey key, uint64_t begin, uint64_t length,
                    const std::vector<StorageByteRange> &ranges, const ObjectMappingContext &context, Description &d,
                    Changes &changes) {
  if (length == 0) {
    throw std::invalid_argument("replacement must contain initialized data");
  }
  auto end = End(begin, length);
  if (d.info_.mode_ == ObjectSizeMode::Fixed && end > d.info_.size_) {
    throw std::invalid_argument("replacement exceeds fixed object length");
  }
  Cut(base, key, begin, end, d, changes, context.unit_);
  uint64_t remaining = length;
  uint64_t cursor = begin;
  for (const auto &range : ranges) {
    if (remaining == 0) {
      throw std::invalid_argument("only the final allocation may have unused padding");
    }
    const auto id = d.next_allocation_;
    d.next_allocation_ = Advance(id);
    const auto take = std::min(remaining, range.Size());
    if (context.format_ == 1) {
      changes.Put(Key(Allocation, key, id), Encode({range.Offset(), range.Size(), d.info_.version_}));
    } else {
      if (range.Size() - take >= context.unit_) {
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
    if (span.data_) {
      const auto owner = span.data_->owner_.value_or(key);
      const auto task = base_.Get(Key(Deferred, owner, span.data_->allocation_));
      if (task) {
        const auto fields = Decode(*task, 2);
        auto payload = base_.Payload({owner.space_, owner.number_, span.data_->allocation_});
        Require(payload.has_value() && span.data_->offset_ >= fields[0] &&
                    End(span.data_->offset_, span.size_) <= End(fields[0], payload->ref_.bytes_ - 4),
                "pending object source lacks its Journal body");
        span.journal_ = ObjectJournalLocation{*payload, span.data_->offset_ - fields[0]};
      }
    }
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
auto ObjectMappingSnapshot::PlanTailTrim(ObjectKey key, uint64_t maximum_bytes) const -> uint64_t {
  if (maximum_bytes == 0) {
    throw std::invalid_argument("tail trim requires a positive byte limit");
  }
  Active active(*context_);
  const auto end = ReadDescription(base_, key).info_.size_;
  if (end == 0) {
    return 0;
  }
  const auto start = end - std::min(end, maximum_bytes);
  const auto upper = Key(Mapping, key, end - 1);
  const auto entry = base_.GetFloor(upper);
  if (!entry || !SamePrefix(entry->key_, upper)) {
    return start;  // Entire object is a hole.
  }
  const auto span = ReadSpan(*entry);
  const auto mapped_end = span.offset_ + span.size_;
  Require(mapped_end <= end, "object mapping exceeds logical length");
  // A trailing hole is trimmed separately from the mapping before it.
  return std::max(start, mapped_end < end ? mapped_end : span.offset_);
}
auto ObjectMappingSnapshot::EstimateRewrite(ObjectKey key) const -> std::optional<ObjectRewriteEstimate> {
  Active active(*context_);
  ReadDescription(base_, key);
  const auto unit = context_->unit_;
  const auto limit = context_->options_.max_query_spans_;
  const auto lower = Key(Mapping, key, 0);
  std::set<uint64_t> units;
  size_t examined = 0;
  for (const auto &entry : base_.Scan(lower, limit + 1)) {
    if (!SamePrefix(entry.key_, lower)) break;
    if (++examined > limit) return std::nullopt;
    const auto span = ReadSpan(entry);
    if (span.data_->owner_) continue;
    const auto first = span.data_->offset_ / unit;
    const auto end = (End(span.data_->offset_, span.size_) - 1) / unit + 1;
    for (auto n = first; n < end; ++n) {
      if (units.size() == limit && units.count(n) == 0) return std::nullopt;
      units.insert(n);
    }
  }
  uint64_t bytes = 0;
  for (auto n : units) {
    if (!base_.Get(Key(SharedUnit, key, n))) bytes += unit;
  }
  return ObjectRewriteEstimate{unit, bytes};
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
      ReplaceChanges(base.base_, key, begin, length, reservation->Extents(), *context_, d, changes);
    } else {
      if (!remove && d.info_.mode_ == ObjectSizeMode::Fixed && length != d.info_.size_) {
        throw std::invalid_argument("fixed object cannot change length");
      }
      const auto size = remove ? 0 : length;
      Cut(base.base_, key, size, d.info_.size_, d, changes, context_->unit_);
      d.info_.size_ = size;
      d.alive_ = !remove;
    }
    changes.Put(Key(Descriptor, key, 0), EncodeDescription(d));
    const auto mutations = changes.Take();
    return reservation != nullptr ? allocator_.Commit(base.base_, *reservation, mutations)
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
  return {impl_->metadata_.Read(), impl_->context_};
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
auto ObjectMappingStore::SupportsSharing() const -> bool { return impl_->context_->format_ >= 4; }
auto ObjectMappingStore::Share(const ObjectMappingSnapshot &base, ObjectKey key, const std::vector<ObjectSpan> &spans)
    -> JournalResult {
  auto &s = *impl_;
  Active active(*s.context_);
  s.Check(base);
  if (!SupportsSharing()) throw std::runtime_error("shared versions require object format v4; no implicit migration");
  auto d = ReadDescription(base.base_, key);
  d.info_.version_ = Advance(d.info_.version_);
  Changes changes(s.context_->options_);
  for (const auto &span : spans) {
    if (span.size_ == 0 || End(span.offset_, span.size_) > d.info_.size_) {
      throw std::invalid_argument("shared mapping exceeds destination");
    }
    const auto target = base.Resolve(key, span.offset_, span.size_);
    if (!target.complete_ || std::any_of(target.spans_.begin(), target.spans_.end(),
                                         [](const auto &part) { return part.data_.has_value(); })) {
      throw std::invalid_argument("shared version construction requires an empty destination range");
    }
    if (span.data_) {
      if (!span.data_->owner_) throw std::invalid_argument("shared mapping requires original content identity");
      changes.Map(key, span);
      changes.Share(base.base_, span, s.context_->unit_, true);
    }
  }
  changes.Put(Key(Descriptor, key, 0), EncodeDescription(d));
  return s.metadata_.Commit(base.base_, changes.Take());
}
auto ObjectMappingSnapshot::Control(ObjectKey owner, uint64_t item) const -> std::optional<std::vector<std::byte>> {
  return base_.Get(Key(object_mapping_detail::Control, owner, item));
}
auto ObjectMappingSnapshot::Controls(ObjectKey owner, uint64_t from, size_t limit) const
    -> std::vector<ObjectControlEntry> {
  if (limit == 0 || limit > context_->options_.max_query_spans_) {
    throw std::invalid_argument("control scan exceeds query budget");
  }
  const auto lower = Key(object_mapping_detail::Control, owner, from);
  std::vector<ObjectControlEntry> result;
  for (const auto &entry : base_.Scan(lower, limit)) {
    if (!SamePrefix(entry.key_, lower)) {
      break;
    }
    result.push_back({entry.key_.item_, entry.value_});
  }
  return result;
}
auto ObjectMappingAccess::Unit(ObjectMappingStore &store) -> uint64_t { return store.impl_->context_->unit_; }
auto ObjectMappingAccess::Apply(ObjectMappingStore &store, const ObjectMappingSnapshot &base,
                                const std::vector<ObjectChange> &operations, DataReservation *reservation,
                                const std::vector<ObjectControlMutation> &controls,
                                const std::vector<std::vector<std::byte>> &payloads) -> JournalResult {
  auto &s = *store.impl_;
  Active active(*s.context_);
  s.Check(base);
  Changes changes(s.context_->options_);
  // Validate exact ownership partition before creating mapping facts. No caller
  // may supply a device address not covered by this actual reservation.
  size_t extent = 0;
  uint64_t used = 0;
  for (const auto &op : operations) {
    for (const auto &range : op.extents_) {
      if (reservation == nullptr || extent == reservation->Extents().size()) {
        throw std::invalid_argument("object transaction lacks its allocation");
      }
      const auto &owned = reservation->Extents()[extent];
      if (range.Offset() != owned.Offset() + used || range.Size() > owned.Size() - used) {
        throw std::invalid_argument("object transaction allocation partition differs");
      }
      used += range.Size();
      if (used == owned.Size()) {
        ++extent;
        used = 0;
      }
    }
  }
  if (reservation != nullptr && (extent != reservation->Extents().size() || used != 0)) {
    throw std::invalid_argument("object transaction leaves an unowned allocation");
  }
  std::vector<MetadataPayloadMutation> journal_payloads;
  size_t operation = 0;
  for (const auto &op : operations) {
    const auto payload_index = operation++;
    const auto key = op.object_;
    if (op.operation_ == ObjectOperation::Create) {
      if (!base.base_.Get(Key(Space, {key.space_, 0}, 0))) {
        Fail(ObjectMappingErrorCode::NotFound, "object space does not exist");
      }
      if (base.base_.Get(Key(Descriptor, key, 0))) {
        Fail(ObjectMappingErrorCode::AlreadyExists, "object identity cannot be reused");
      }
      changes.Put(Key(Descriptor, key, 0), EncodeDescription({true, {op.length_, 1, op.mode_}, 1, 1, 0}));
      continue;
    }
    auto d = ReadDescription(base.base_, key);
    if (d.info_.version_ != op.version_) {
      throw MetadataError(MetadataErrorCode::Conflict, "object changed while its data was in flight");
    }
    d.info_.version_ = Advance(d.info_.version_);
    if (op.operation_ == ObjectOperation::Write || op.operation_ == ObjectOperation::Append) {
      const auto first_allocation = d.next_allocation_;
      ReplaceChanges(base.base_, key, op.offset_, op.length_, op.extents_, *s.context_, d, changes);
      if (op.deferred_) {
        if (s.context_->format_ < 3 || payload_index >= payloads.size() ||
            payloads[payload_index].size() != op.length_) {
          throw std::invalid_argument("Deferred requires current object format and complete initialized body");
        }
        size_t cursor = 0;
        auto allocation = first_allocation;
        for (const auto &range : op.extents_) {
          const auto take = std::min<uint64_t>(range.Size(), op.length_ - cursor);
          changes.Put(Key(Deferred, key, allocation), Encode({range.Offset(), range.Size()}));
          const auto &body = payloads[payload_index];
          journal_payloads.push_back(
              {{key.space_, key.number_, allocation}, Bytes(body.begin() + cursor, body.begin() + cursor + take)});
          cursor += take;
          ++allocation;
        }
      }
    } else if (op.operation_ == ObjectOperation::Unmap) {
      Cut(base.base_, key, op.offset_, End(op.offset_, op.length_), d, changes, s.context_->unit_);
    } else {
      const bool remove = op.operation_ == ObjectOperation::Remove;
      if (!remove && d.info_.mode_ == ObjectSizeMode::Fixed && op.length_ != d.info_.size_) {
        throw std::invalid_argument("fixed object cannot change length");
      }
      const auto size = remove ? 0 : op.length_;
      Cut(base.base_, key, size, d.info_.size_, d, changes, s.context_->unit_);
      d.info_.size_ = size;
      d.alive_ = !remove;
    }
    changes.Put(Key(Descriptor, key, 0), EncodeDescription(d));
  }
  for (const auto &control : controls) {
    if (!base.base_.Get(Key(Space, {control.owner_.space_, 0}, 0))) {
      Fail(ObjectMappingErrorCode::NotFound, "control space does not exist");
    }
    changes.Put(Key(object_mapping_detail::Control, control.owner_, control.item_), control.value_);
  }
  const auto mutations = changes.Take();
  return reservation != nullptr ? s.allocator_.Commit(base.base_, *reservation, mutations, journal_payloads)
                                : s.metadata_.Commit(base.base_, mutations);
}
auto ObjectMappingAccess::SupportsDeferred(ObjectMappingStore &store) -> bool {
  return store.impl_->context_->format_ >= 3;
}
auto ObjectMappingAccess::ReadPayload(ObjectMappingStore &store, const JournalPayload &payload, IOReadBudget &budget,
                                      std::function<void(const IOBatchResult &)> complete, std::function<void()> ready)
    -> JournalPayloadRead {
  try {
    return store.impl_->metadata_.ReadPayload(payload, budget, std::move(complete), std::move(ready));
  } catch (const JournalError &error) {
    if (error.Code() == JournalErrorCode::ResourceUnavailable) throw ObjectIOBusy();
    if (error.Code() == JournalErrorCode::RequestTooLarge)
      throw MetadataError(MetadataErrorCode::ResourceUnavailable, error.what());
    throw;
  }
}
auto ObjectMappingAccess::Pending(const ObjectMappingSnapshot &base, size_t limit) -> std::vector<DeferredTarget> {
  std::vector<DeferredTarget> result;
  for (const auto &entry : base.base_.Scan({uint64_t{Deferred} << 56, 0, 0}, limit)) {
    if ((entry.key_.category_ >> 56) != Deferred) break;
    ObjectKey object{entry.key_.category_ & SPACE_LIMIT, entry.key_.owner_};
    auto payload = base.base_.Payload({object.space_, object.number_, entry.key_.item_});
    Require(payload.has_value(), "Deferred target has no recoverable body");
    const auto f = Decode(entry.value_, 2);
    Require(f[1] >= payload->ref_.bytes_ - 4 && f[0] % base.context_->unit_ == 0 && f[1] % base.context_->unit_ == 0,
            "invalid Deferred target range");
    result.push_back({object, entry.key_.item_, f[0], f[1], *payload});
  }
  return result;
}
auto ObjectMappingAccess::Complete(ObjectMappingStore &store, const ObjectMappingSnapshot &base,
                                   const DeferredTarget &task) -> JournalResult {
  auto &s = *store.impl_;
  s.Check(base);
  const auto key = Key(Deferred, task.object_, task.allocation_);
  const auto entry = base.base_.Get(key);
  Require(entry && *entry == Encode({task.offset_, task.capacity_}), "Deferred completion changed target identity");
  return s.metadata_.Commit(base.base_, {{key, std::nullopt}},
                            {{{task.object_.space_, task.object_.number_, task.allocation_}, std::nullopt}});
}
auto ObjectMappingAccess::Garbage(const ObjectMappingSnapshot &base, MetadataKey cursor) -> ObjectGCPage {
  const uint64_t category = uint64_t{PendingRange} << 56;
  if (cursor.category_ < category || cursor.category_ >= (uint64_t{OwnedRange} << 56)) {
    cursor = {category, 0, 0};
  }
  ObjectGCPage result{cursor, {}};
  const auto entries = base.base_.Scan(cursor, base.context_->options_.max_query_spans_);
  for (const auto &entry : entries) {
    if (entry.key_.category_ >= (uint64_t{OwnedRange} << 56)) {
      result.next_ = {category, 0, 0};
      return result;
    }
    const auto f = Decode(entry.value_, 5);
    result.candidates_.push_back({{entry.key_.category_ & SPACE_LIMIT, entry.key_.owner_}, f[4]});
    result.next_ = {entry.key_.category_, entry.key_.owner_, Advance(entry.key_.item_)};
  }
  if (entries.empty()) {
    result.next_ = {category, 0, 0};
  }
  return result;
}
}  // namespace bustub
