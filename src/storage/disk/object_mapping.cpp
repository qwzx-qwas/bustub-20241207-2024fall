//===----------------------------------------------------------------------===//
// BusTub: object ranges, atomic allocation publication and durable retirement.
//===----------------------------------------------------------------------===//
#include "storage/disk/object_mapping.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <map>
#include <tuple>
#include <utility>

#include "common/byte_codec.h"

namespace bustub {
struct ObjectMappingContext {
  explicit ObjectMappingContext(ObjectMappingOptions options) : options_(options) {}
  ObjectMappingOptions options_;
  std::atomic<size_t> active_{0};
};
namespace {
constexpr uint64_t CONTROL = 13;
constexpr uint64_t MAGIC = 0x4253544f424a4d50ULL;  // BSTOBJMP
constexpr uint64_t FORMAT = 1;
constexpr uint64_t SPACE_LIMIT = (uint64_t{1} << 56) - 1;
enum Kind : uint64_t { Space = 1, Descriptor = 2, Mapping = 3, Allocation = 4, PendingRange = 5 };
using Bytes = std::vector<std::byte>;
[[noreturn]] void Fail(ObjectMappingErrorCode code, const char *message) { throw ObjectMappingError(code, message); }
void Require(bool valid, const char *message) {
  if (!valid) {
    Fail(ObjectMappingErrorCode::Corrupt, message);
  }
}
auto End(uint64_t offset, uint64_t size) -> uint64_t {
  const auto range = StorageByteRange::Create(offset, size);
  if (!range) {
    throw std::invalid_argument("object range overflows");
  }
  return offset + size;
}
auto Advance(uint64_t value) -> uint64_t {
  if (value == std::numeric_limits<uint64_t>::max()) {
    Fail(ObjectMappingErrorCode::ResourceUnavailable, "object sequence exhausted");
  }
  return value + 1;
}
auto Key(Kind kind, ObjectKey object, uint64_t item) -> MetadataKey {
  if (object.space_ == 0 || object.space_ > SPACE_LIMIT) {
    throw std::invalid_argument("object requires a valid persistent space identity");
  }
  return {(static_cast<uint64_t>(kind) << 56) | object.space_, object.number_, item};
}
auto SamePrefix(const MetadataKey &a, const MetadataKey &b) -> bool {
  return a.category_ == b.category_ && a.owner_ == b.owner_;
}
auto Encode(std::initializer_list<uint64_t> fields) -> Bytes {
  ByteWriter w;
  for (auto value : fields) {
    w.PutU64(value);
  }
  return w.Take();
}
auto Decode(const Bytes &bytes, size_t fields) -> std::vector<uint64_t> {
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
auto ReadDescription(const MetadataSnapshot &base, ObjectKey key, bool allow_deleted = false) -> Description {
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
auto EncodeDescription(const Description &d) -> Bytes {
  return Encode({d.alive_, static_cast<uint64_t>(d.info_.mode_), d.info_.size_, d.info_.version_, d.next_allocation_,
                 d.next_retired_, d.pending_});
}
auto ReadSpan(const MetadataEntry &entry) -> ObjectSpan {
  const auto f = Decode(entry.value_, 3);
  Require(f[0] != 0 && f[2] != 0 && StorageByteRange::Create(entry.key_.item_, f[0]).has_value() &&
              StorageByteRange::Create(f[1], f[0]).has_value(),
          "invalid object extent");
  return {entry.key_.item_, f[0], ObjectDataLocation{f[1], f[2]}};
}
auto Slice(const ObjectSpan &span, uint64_t begin, uint64_t end) -> ObjectSpan {
  return {begin, end - begin,
          ObjectDataLocation{span.data_->offset_ + (begin - span.offset_), span.data_->allocation_}};
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
    Put(Key(Mapping, key, span.offset_), Encode({span.size_, span.data_->offset_, span.data_->allocation_}));
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
auto ReadControl(const MetadataSnapshot &base) -> uint64_t {
  const auto value = base.Get({CONTROL, 0, 0});
  if (!value) {
    Fail(ObjectMappingErrorCode::NotInitialized, "object format missing; Open never creates");
  }
  const auto f = Decode(*value, 3);
  Require(f[0] == MAGIC && f[1] == FORMAT && f[2] <= SPACE_LIMIT, "invalid object format or space sequence");
  return f[2];
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
  Impl(DataAllocator &allocator, MetadataEngine &metadata, ObjectMappingOptions options)
      : allocator_(allocator), metadata_(metadata), context_(std::make_shared<ObjectMappingContext>(options)) {
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
    return result;
  }
  void Open() {
    Start();
    ReadControl(metadata_.Read());
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
        changes.Put(Key(Allocation, key, id), Encode({range.Offset(), range.Size(), d.info_.version_}));
        const auto take = std::min(remaining, range.Size());
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
    : impl_(std::make_unique<Impl>(allocator, allocator.Metadata(), options)) {}
ObjectMappingStore::~ObjectMappingStore() = default;
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
  const auto last = ReadControl(base.base_);
  if (last == SPACE_LIMIT) {
    Fail(ObjectMappingErrorCode::ResourceUnavailable, "object space identities exhausted");
  }
  const auto id = last + 1;
  Changes changes(s.context_->options_);
  changes.Put({CONTROL, 0, 0}, Encode({MAGIC, FORMAT, id}));
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
