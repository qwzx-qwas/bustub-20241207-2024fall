//===----------------------------------------------------------------------===//
// F14 uses the same metadata transaction as F13/F12; no second recovery log.
//===----------------------------------------------------------------------===//
#include "storage/disk/object_reference.h"

#include <algorithm>
#include <condition_variable>  // NOLINT(build/c++11)
#include <map>
#include <mutex>  // NOLINT(build/c++11)
#include <utility>

#include "object_mapping_internal.h"  // NOLINT(build/include_subdir): private sibling schema.

namespace bustub {
namespace {
using namespace object_mapping_detail;  // NOLINT(build/namespaces)
struct Interval {
  uint64_t begin_;
  uint64_t end_;
};
struct ProtectedRange {
  ObjectKey object_;
  uint64_t allocation_;
  Interval range_;
};
auto SameObject(ObjectKey a, ObjectKey b) -> bool { return a.space_ == b.space_ && a.number_ == b.number_; }
auto Overlaps(Interval a, Interval b) -> bool { return a.begin_ < b.end_ && b.begin_ < a.end_; }
auto Conflicts(const ProtectedRange &a, const ProtectedRange &b) -> bool {
  return SameObject(a.object_, b.object_) && a.allocation_ == b.allocation_ && Overlaps(a.range_, b.range_);
}
auto Merge(std::vector<Interval> ranges) -> std::vector<Interval> {
  std::sort(ranges.begin(), ranges.end(), [](auto a, auto b) { return a.begin_ < b.begin_; });
  size_t size = 0;
  for (const auto range : ranges) {
    if (range.begin_ == range.end_) {
      continue;
    }
    if (size != 0 && ranges[size - 1].end_ >= range.begin_) {
      ranges[size - 1].end_ = std::max(ranges[size - 1].end_, range.end_);
    } else {
      ranges[size++] = range;
    }
  }
  ranges.resize(size);
  return ranges;
}
auto Subtract(Interval from, const std::vector<Interval> &removed) -> std::vector<Interval> {
  std::vector<Interval> result;
  auto cursor = from.begin_;
  for (const auto range : removed) {
    if (range.end_ <= cursor) {
      continue;
    }
    if (range.begin_ >= from.end_) {
      break;
    }
    if (range.begin_ > cursor) {
      result.push_back({cursor, range.begin_});
    }
    cursor = std::max(cursor, std::min(range.end_, from.end_));
  }
  if (cursor < from.end_) {
    result.push_back({cursor, from.end_});
  }
  return result;
}
auto BytesIn(const std::vector<Interval> &ranges) -> uint64_t {
  uint64_t bytes = 0;
  for (const auto range : ranges) {
    bytes += range.end_ - range.begin_;
  }
  return bytes;
}
[[noreturn]] void ReferenceFail(ObjectReferenceErrorCode code, const char *message) {
  throw ObjectReferenceError(code, message);
}
struct AllocationInfo {
  Interval range_;
  uint64_t initialized_;
};
auto DecodeAllocation(const Bytes &value, uint64_t unit) -> AllocationInfo {
  const auto f = Decode(value, 4);
  Require(f[1] != 0 && f[0] % unit == 0 && f[1] % unit == 0 && f[2] != 0 && f[3] != 0 && f[3] <= f[1] &&
              f[1] - f[3] < unit && StorageByteRange::Create(f[0], f[1]).has_value(),
          "invalid v2 object allocation");
  return {{f[0], f[0] + f[1]}, f[3]};
}
struct Owned {
  MetadataKey key_;
  Interval range_;
};
}  // namespace

struct ObjectReferenceState {
  explicit ObjectReferenceState(ObjectReferenceOptions options) : options_(options) {}
  ObjectReferenceOptions options_;
  std::mutex mutex_;
  std::condition_variable drained_;
  bool accepting_{true};
  size_t active_{0};
  uint64_t next_id_{1};
  std::map<uint64_t, std::vector<ProtectedRange>> pins_;
  std::map<uint64_t, std::vector<ProtectedRange>> gates_;
};
struct ObjectReadProtection {
  explicit ObjectReadProtection(std::shared_ptr<ObjectReferenceState> state) : state_(std::move(state)) {}
  ~ObjectReadProtection() {
    if (id_ != 0) {
      std::lock_guard<std::mutex> lock(state_->mutex_);
      state_->pins_.erase(id_);
    }
  }
  std::shared_ptr<ObjectReferenceState> state_;
  uint64_t id_{0};
  std::vector<ObjectSpan> spans_;
};
namespace {
struct Call {
  explicit Call(ObjectReferenceState &state) : state_(state) {
    std::lock_guard<std::mutex> lock(state_.mutex_);
    if (!state_.accepting_) {
      Fail(ObjectMappingErrorCode::NotReady, "object references are closed");
    }
    if (state_.active_ == state_.options_.max_active_operations_) {
      Fail(ObjectMappingErrorCode::ResourceUnavailable, "object reference operation budget full");
    }
    ++state_.active_;
  }
  ~Call() {
    std::lock_guard<std::mutex> lock(state_.mutex_);
    --state_.active_;
    state_.drained_.notify_all();
  }
  ObjectReferenceState &state_;
};
struct Gate {
  explicit Gate(ObjectReferenceState &state) : state_(state) {}
  ~Gate() {
    if (id_ != 0) {
      std::lock_guard<std::mutex> lock(state_.mutex_);
      state_.gates_.erase(id_);
    }
  }
  ObjectReferenceState &state_;
  uint64_t id_{0};
};
void CheckOwnership(const MetadataSnapshot &base, const ProtectedRange &p, uint64_t unit, size_t *lookups,
                    size_t limit) {
  const auto value = base.Get(Key(object_mapping_detail::Allocation, p.object_, p.allocation_));
  if (!value) {
    ReferenceFail(ObjectReferenceErrorCode::Stale, "read allocation has been released");
  }
  const auto allocation = DecodeAllocation(*value, unit);
  Require(p.range_.begin_ >= allocation.range_.begin_ && p.range_.end_ <= allocation.range_.end_,
          "read lies outside allocation");
  auto cursor = p.range_.begin_;
  while (cursor < p.range_.end_) {
    if (++*lookups > limit) {
      Fail(ObjectMappingErrorCode::ResourceUnavailable, "ownership lookup budget exhausted");
    }
    const auto upper = Key(OwnedRange, p.object_, cursor);
    const auto entry = base.GetFloor(upper);
    if (!entry || !SamePrefix(entry->key_, upper)) {
      ReferenceFail(ObjectReferenceErrorCode::Stale, "read ownership no longer exists");
    }
    const auto f = Decode(entry->value_, 2);
    const auto stop = End(entry->key_.item_, f[0]);
    if (f[1] != p.allocation_ || stop <= cursor) {
      ReferenceFail(ObjectReferenceErrorCode::Stale, "read range no longer belongs to its allocation");
    }
    cursor = std::min(stop, p.range_.end_);
  }
}
}  // namespace

struct ObjectReferenceManager::Impl {
  Impl(std::shared_ptr<ObjectMappingContext> context, ObjectReferenceOptions options)
      : context_(std::move(context)), state_(std::make_shared<ObjectReferenceState>(options)) {
    if (context_->format_ < 2) {
      ReferenceFail(ObjectReferenceErrorCode::UnsupportedFormat,
                    "F14 requires v2; v1 remains usable without reclamation");
    }
    if (options.max_leases_ == 0 || options.max_ranges_per_lease_ == 0 || options.max_scan_entries_ == 0 ||
        options.max_scan_entries_ == std::numeric_limits<size_t>::max() || options.max_reclaim_ranges_ == 0 ||
        options.max_reclaim_bytes_ < context_->unit_ || options.max_reclaim_bytes_ % context_->unit_ != 0 ||
        options.max_active_operations_ == 0) {
      throw std::invalid_argument("object references require explicit positive budgets and aligned reclaim bytes");
    }
    if (context_->references_started_) {
      throw std::logic_error("one reference manager per object store lifetime");
    }
    context_->references_started_ = true;
  }
  auto Protect(const ObjectMappingSnapshot &view, ObjectKey object, uint64_t offset, uint64_t length)
      -> std::shared_ptr<ObjectReadProtection> {
    Call call(*state_);
    auto protection = std::make_shared<ObjectReadProtection>(state_);
    const auto end = End(offset, length);
    auto cursor = offset;
    std::vector<ProtectedRange> ranges;
    for (;;) {
      const auto page = view.Resolve(object, cursor, end - cursor);
      if (page.spans_.size() > state_->options_.max_ranges_per_lease_ - protection->spans_.size()) {
        Fail(ObjectMappingErrorCode::ResourceUnavailable, "protected read exceeds range budget");
      }
      for (const auto &span : page.spans_) {
        protection->spans_.push_back(span);
        if (span.data_) {
          const auto first = span.data_->offset_;
          const auto stop = End(first, span.size_);
          const auto unit = context_->unit_;
          const auto rounded_end = End(stop, (unit - stop % unit) % unit);
          ranges.push_back(
              {span.data_->owner_.value_or(object), span.data_->allocation_, {first - first % unit, rounded_end}});
        }
      }
      if (page.complete_) {
        break;
      }
      cursor = page.next_offset_;
    }
    {
      std::lock_guard<std::mutex> lock(state_->mutex_);
      if (state_->pins_.size() == state_->options_.max_leases_) {
        Fail(ObjectMappingErrorCode::ResourceUnavailable, "object read lease budget full");
      }
      for (const auto &[id, gate] : state_->gates_) {
        for (const auto &a : gate) {
          for (const auto &b : ranges) {
            if (Conflicts(a, b)) {
              ReferenceFail(ObjectReferenceErrorCode::Busy, "read range is being retired");
            }
          }
        }
      }
      const auto id = state_->next_id_;
      state_->next_id_ = Advance(id);
      state_->pins_.emplace(id, ranges);
      protection->id_ = id;
    }
    // Register first: reclamation cannot pass between validation and pinning.
    // Current ownership is authoritative; the caller's old map is not.
    const auto current = context_->metadata_.Read();
    size_t lookups = 0;
    for (const auto &range : ranges) {
      CheckOwnership(current, range, context_->unit_, &lookups, state_->options_.max_scan_entries_);
    }
    return protection;
  }
  auto Reclaim(const MetadataSnapshot &base, ObjectKey object, uint64_t allocation_id) -> ObjectReclaimResult {
    Call call(*state_);
    context_->metadata_.Read();  // Do not continue through an isolated B, even on a no-op.
    auto d = ReadDescription(base, object, true);
    if (allocation_id == 0 || allocation_id >= d.next_allocation_) {
      throw std::invalid_argument("unknown object allocation identity");
    }
    // The persistent task owns its complete target through final IO and completion.
    if (base.Get(Key(Deferred, object, allocation_id))) return {};
    const auto allocation_key = Key(object_mapping_detail::Allocation, object, allocation_id);
    const auto value = base.Get(allocation_key);
    if (!value) {
      return {};  // Repeated old identity cannot release a new owner of that address.
    }
    const auto allocation = DecodeAllocation(*value, context_->unit_);
    const auto limit = state_->options_.max_scan_entries_;
    std::vector<Owned> owned;
    const auto owned_lower = Key(OwnedRange, object, allocation.range_.begin_);
    size_t examined = 0;
    for (const auto &entry : base.Scan(owned_lower, limit + 1)) {
      if (!SamePrefix(entry.key_, owned_lower) || entry.key_.item_ >= allocation.range_.end_) {
        break;
      }
      if (++examined > limit) {
        Fail(ObjectMappingErrorCode::ResourceUnavailable, "ownership reclaim scan exceeds budget");
      }
      const auto f = Decode(entry.value_, 2);
      if (f[1] != allocation_id) {
        continue;
      }
      const auto stop = End(entry.key_.item_, f[0]);
      Require(f[0] != 0 && entry.key_.item_ % context_->unit_ == 0 && f[0] % context_->unit_ == 0 &&
                  stop <= allocation.range_.end_,
              "invalid remaining ownership range");
      owned.push_back({entry.key_, {entry.key_.item_, stop}});
    }
    Require(!owned.empty(), "allocated identity has no remaining ownership");
    const auto pending_lower = Key(PendingRange, object, 0);
    std::vector<std::pair<MetadataKey, RetiredObjectRange>> pending;
    std::vector<Interval> dead{{allocation.range_.begin_ + allocation.initialized_, allocation.range_.end_}};
    size_t pending_count = 0;
    for (const auto &entry : base.Scan(pending_lower, limit + 1)) {
      if (!SamePrefix(entry.key_, pending_lower)) {
        break;
      }
      if (++examined > limit) {
        Fail(ObjectMappingErrorCode::ResourceUnavailable, "retirement reclaim scan exceeds budget");
      }
      ++pending_count;
      const auto f = Decode(entry.value_, 5);
      if (f[4] != allocation_id) {
        continue;
      }
      Require(f[0] <= d.info_.version_ && f[2] != 0 && f[3] >= allocation.range_.begin_ &&
                  End(f[3], f[2]) <= allocation.range_.begin_ + allocation.initialized_,
              "invalid allocation retirement range");
      pending.push_back({entry.key_, {entry.key_.item_, f[0], {f[1], f[2], ObjectDataLocation{f[3], f[4]}}}});
      dead.push_back({f[3], f[3] + f[2]});
    }
    Require(pending_count == d.pending_, "retirement count disagrees with object descriptor");
    dead = Merge(std::move(dead));
    std::vector<Interval> candidates;
    for (const auto &o : owned) {
      for (const auto range : dead) {
        auto begin = std::max(o.range_.begin_, range.begin_);
        auto end = std::min(o.range_.end_, range.end_);
        if (begin >= end) {
          continue;
        }
        const auto unit = context_->unit_;
        begin = End(begin, (unit - begin % unit) % unit);
        end -= end % unit;
        if (begin < end) {
          candidates.push_back({begin, end});
        }
      }
    }
    candidates = Merge(std::move(candidates));
    if (candidates.empty()) {
      return {};
    }
    Gate fence(*state_);
    std::vector<ProtectedRange> gates;
    for (const auto range : candidates) {
      gates.push_back({object, allocation_id, range});
    }
    std::vector<Interval> pinned;
    {
      std::lock_guard<std::mutex> lock(state_->mutex_);
      for (const auto &[id, existing] : state_->gates_) {
        for (const auto &a : existing) {
          for (const auto &b : gates) {
            if (Conflicts(a, b)) {
              ReferenceFail(ObjectReferenceErrorCode::Busy, "allocation range already has a reclaimer");
            }
          }
        }
      }
      const auto id = state_->next_id_;
      state_->next_id_ = Advance(id);
      state_->gates_.emplace(id, std::move(gates));
      fence.id_ = id;
      for (const auto &[pin_id, ranges] : state_->pins_) {
        for (const auto &range : ranges) {
          if (SameObject(range.object_, object) && range.allocation_ == allocation_id) {
            pinned.push_back(range.range_);
          }
        }
      }
    }
    // Persistent checkpoint/fork references outlive RAM readers and process restarts.
    // Shared counts use the original allocation identity, never the latest logical map.
    for (const auto range : candidates) {
      const auto unit = context_->unit_;
      const auto lower = Key(SharedUnit, object, range.begin_ / unit);
      for (const auto &entry : base.Scan(lower, limit + 1)) {
        if (!SamePrefix(entry.key_, lower) || entry.key_.item_ >= range.end_ / unit) break;
        if (++examined > limit) Fail(ObjectMappingErrorCode::ResourceUnavailable, "shared reclaim scan exceeds budget");
        const auto f = Decode(entry.value_, 2);
        Require(f[0] != 0 && f[1] == allocation_id, "shared range has wrong allocation identity");
        pinned.push_back({entry.key_.item_ * unit, (entry.key_.item_ + 1) * unit});
      }
    }
    pinned = Merge(std::move(pinned));
    std::vector<Interval> eligible;
    for (const auto range : candidates) {
      const auto parts = Subtract(range, pinned);
      eligible.insert(eligible.end(), parts.begin(), parts.end());
    }
    ObjectReclaimResult result;
    result.pinned_bytes_ = BytesIn(candidates) - BytesIn(eligible);
    std::vector<Interval> released;
    auto remaining_bytes = state_->options_.max_reclaim_bytes_;
    for (const auto range : eligible) {
      if (remaining_bytes == 0 || released.size() == state_->options_.max_reclaim_ranges_) {
        break;
      }
      const auto size = std::min(range.end_ - range.begin_, remaining_bytes);
      released.push_back({range.begin_, range.begin_ + size});
      remaining_bytes -= size;
    }
    if (released.empty()) {
      return result;
    }
    Changes changes(context_->options_);
    size_t remaining_owned = 0;
    for (const auto &o : owned) {
      const auto parts = Subtract(o.range_, released);
      remaining_owned += parts.size();
      if (parts.size() == 1 && parts[0].begin_ == o.range_.begin_ && parts[0].end_ == o.range_.end_) {
        continue;
      }
      changes.Put(o.key_, std::nullopt);
      for (const auto part : parts) {
        changes.Put(Key(OwnedRange, object, part.begin_), Encode({part.end_ - part.begin_, allocation_id}));
      }
    }
    for (const auto &[key, retired] : pending) {
      const auto &span = retired.span_;
      const Interval old{span.data_->offset_, span.data_->offset_ + span.size_};
      const auto parts = Subtract(old, released);
      if (parts.size() == 1 && parts[0].begin_ == old.begin_ && parts[0].end_ == old.end_) {
        continue;
      }
      changes.Put(key, std::nullopt);
      --d.pending_;
      bool first = true;
      for (const auto part : parts) {
        if (d.pending_ == context_->options_.max_retired_records_) {
          Fail(ObjectMappingErrorCode::ResourceUnavailable, "reclaim split exceeds retirement budget");
        }
        const auto id = first ? key.item_ : d.next_retired_;
        if (!first) {
          d.next_retired_ = Advance(id);
        }
        first = false;
        ++d.pending_;
        changes.Put(Key(PendingRange, object, id),
                    Encode({retired.removed_version_, span.offset_ + (part.begin_ - old.begin_),
                            part.end_ - part.begin_, part.begin_, allocation_id}));
      }
    }
    if (remaining_owned == 0) {
      changes.Put(allocation_key, std::nullopt);
    }
    changes.Put(Key(Descriptor, object, 0), EncodeDescription(d));
    std::vector<StorageByteRange> ranges;
    for (const auto range : released) {
      ranges.push_back(*StorageByteRange::Create(range.begin_, range.end_ - range.begin_));
    }
    const auto bytes = BytesIn(released);
    result.commit_ = context_->allocator_.Release(base, ranges, changes.Take());
    if (result.commit_->outcome_ == JournalOutcome::Durable) {
      result.released_bytes_ = bytes;
    }
    return result;
  }
  void Close() {
    std::unique_lock<std::mutex> lock(state_->mutex_);
    state_->accepting_ = false;
    state_->drained_.wait(lock, [&] { return state_->active_ == 0; });
  }
  std::shared_ptr<ObjectMappingContext> context_;
  std::shared_ptr<ObjectReferenceState> state_;
};

ObjectReadLease::ObjectReadLease(std::shared_ptr<ObjectReadProtection> protection)
    : protection_(std::move(protection)) {}
auto ObjectReadLease::Spans() const -> const std::vector<ObjectSpan> & { return protection_->spans_; }
ObjectReferenceManager::ObjectReferenceManager(ObjectMappingStore &mapping, ObjectReferenceOptions options)
    : impl_(std::make_unique<Impl>(mapping.ReferenceContext(), options)) {}
ObjectReferenceManager::~ObjectReferenceManager() { Close(); }
auto ObjectReferenceManager::ProtectRead(const ObjectMappingSnapshot &view, ObjectKey object, uint64_t offset,
                                         uint64_t length) -> ObjectReadLease {
  if (view.context_ != impl_->context_) {
    throw MetadataError(MetadataErrorCode::Conflict, "foreign object mapping view");
  }
  return ObjectReadLease(impl_->Protect(view, object, offset, length));
}
auto ObjectReferenceManager::Reclaim(const ObjectMappingSnapshot &base, ObjectKey object, uint64_t allocation)
    -> ObjectReclaimResult {
  if (base.context_ != impl_->context_) {
    throw MetadataError(MetadataErrorCode::Conflict, "foreign object mapping view");
  }
  return impl_->Reclaim(base.base_, object, allocation);
}
void ObjectReferenceManager::Close() { impl_->Close(); }
}  // namespace bustub
