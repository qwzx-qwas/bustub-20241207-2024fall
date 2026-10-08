#include "object_log_store.h"
#include <algorithm>
#include <map>
#include <stdexcept>
#include "common/byte_codec.h"

namespace bustub {
namespace {
constexpr uint64_t HEADER = 9;
auto Encode(std::initializer_list<uint64_t> fields) -> std::vector<std::byte> {
  ByteWriter w;
  for (auto f : fields) {
    w.PutU64(f);
  }
  return w.Take();
}
}  // namespace
struct ObjectLogStore::Cleaning {
  enum class Step { Read, Create, Write, Publish, Retire };
  ResourceCharge memory_;  // Released after all copied buffers and directory vectors.
  std::vector<std::shared_ptr<void>> leases_;
  std::vector<Segment> sources_;
  std::vector<Segment> segments_;
  std::vector<Location> index_;
  std::vector<std::byte> body_;
  uint64_t generation_, base_, object_{0};
  size_t first_;
  Step step_{Step::Read};
};
ObjectLogStore::~ObjectLogStore() = default;
namespace {
// Only choose one existing mapping; holes are already progress. A second page
// is needed when the first bounded Resolve contains only its leading hole.
auto DeadSpan(const ObjectMappingSnapshot &view, ObjectKey key, uint64_t begin, uint64_t end, uint64_t maximum)
    -> std::optional<ObjectMutation> {
  for (int page = 0; page != 2 && begin < end; ++page) {
    const auto found = view.Resolve(key, begin, end - begin);
    for (const auto &span : found.spans_) {
      if (span.data_) {
        ObjectMutation out{ObjectOperation::Unmap, key, span.offset_, ObjectSizeMode::Variable, {}};
        out.length_ = std::min(span.size_, maximum);
        return out;
      }
    }
    if (found.complete_) break;
    begin = found.next_offset_;
  }
  return std::nullopt;
}
}  // namespace
ObjectLogStore::ObjectLogStore(std::shared_ptr<RaftObjectStorage> storage, uint64_t commit,
                               std::shared_ptr<std::mutex> mutex, bool verified_rebuild)
    : commit_(commit), storage_(std::move(storage)), mutex_(std::move(mutex)) {
  const auto root = storage_->Control(1, 0);
  if (!root) {
    if (commit != 0 && !verified_rebuild) {
      throw std::runtime_error("missing committed Raft object log");
    }
    return;
  }
  ByteReader r(*root);
  if (r.ReadU64() != 1) {
    throw std::runtime_error("unknown object log manifest version");
  }
  generation_ = r.ReadU64();
  base_ = r.ReadU64();
  base_term_ = r.ReadU64();
  const auto last = r.ReadU64();
  if (!r.Empty() || last < base_ || last - base_ > storage_->options_.max_log_entries_ ||
      (!verified_rebuild && (commit < base_ || commit > last))) {
    throw std::runtime_error("invalid object log boundaries");
  }
  for (const auto &e : storage_->Scan(1)) {
    if (e.item_ == 0) {
      continue;
    }
    ByteReader in(e.value_);
    Segment s{e.item_, in.ReadU64(), in.ReadU64()};
    if (!in.Empty() || s.begin_ >= s.end_ || s.end_ > storage_->options_.segment_bytes_) {
      throw std::runtime_error("invalid object log segment");
    }
    segments_.push_back(s);
  }
  if (verified_rebuild) {
    return;
  }
  uint64_t total_bytes = 0;
  std::optional<Location> current;
  std::vector<std::byte> body;
  for (const auto &s : segments_) {
    auto lease = storage_->Lease(s.object_);
    for (auto offset = s.begin_; offset < s.end_;) {
      if (s.end_ - offset < HEADER) {
        throw std::runtime_error("truncated log fragment header");
      }
      const auto head = storage_->Read(s.object_, offset, HEADER);
      ByteReader h(head);
      const auto type = h.ReadU8();
      const auto size = h.ReadU32();
      const auto crc = h.ReadU32();
      if (size == 0 || size > s.end_ - offset - HEADER || type < 1 || type > 4 ||
          size > storage_->options_.max_log_bytes_ - total_bytes) {
        throw std::runtime_error("invalid object log fragment");
      }
      auto bytes = storage_->Read(s.object_, offset + HEADER, size);
      if (crc != Crc32cExtend(Crc32c(head.data(), 5), bytes.data(), bytes.size())) {
        throw std::runtime_error("corrupt object log fragment");
      }
      if (type == 1 || type == 2) {
        if (current) {
          throw std::runtime_error("nested log entry fragments");
        }
        current = Location{0, s.object_, offset, 0};
        body.clear();
      } else if (!current) {
        throw std::runtime_error("missing first log fragment");
      }
      total_bytes += size;
      if (body.size() + size > LogCodec::MAX_PAYLOAD_BYTES + 64) {
        throw std::runtime_error("entry too large");
      }
      body.insert(body.end(), bytes.begin(), bytes.end());
      if (type == 1 || type == 4) {
        auto decoded = LogCodec::DecodeOne(body);
        if (decoded.status_ != LogDecodeStatus::COMPLETE || decoded.bytes_consumed_ != body.size() ||
            decoded.entry_->index_ != base_ + index_.size() + 1) {
          throw std::runtime_error("invalid published log entry");
        }
        if (index_.size() >= storage_->options_.max_log_entries_) {
          throw std::runtime_error("log index budget exceeded");
        }
        current->size_ = body.size();
        current->term_ = decoded.entry_->term_;
        index_.push_back(*current);
        current.reset();
      }
      offset += HEADER + size;
    }
  }
  if (current || Last() != last) {
    throw std::runtime_error("published log history is incomplete");
  }
}
auto ObjectLogStore::Controls(const std::vector<Segment> &segments, size_t entries, uint64_t base, uint64_t term)
    -> std::vector<ObjectControlMutation> {
  std::vector<ObjectControlMutation> out;
  out.push_back(storage_->Change(1, 0, Encode({1, generation_ + 1, base, term, base + entries})));
  for (const auto &s : segments) {
    const auto old =
        std::find_if(segments_.begin(), segments_.end(), [&](const auto &o) { return o.object_ == s.object_; });
    if (old == segments_.end() || old->begin_ != s.begin_ || old->end_ != s.end_) {
      out.push_back(storage_->Change(1, s.object_, Encode({s.begin_, s.end_})));
    }
    if (old == segments_.end()) {
      out.push_back(storage_->Own(s.object_, 1, 1));
    }
  }
  for (const auto &old : segments_) {
    if (std::none_of(segments.begin(), segments.end(), [&](const auto &s) { return s.object_ == old.object_; })) {
      out.push_back(storage_->Change(1, old.object_, std::nullopt));
      out.push_back(storage_->Own(old.object_, 1, 2));
    }
  }
  return out;
}
void ObjectLogStore::Publish(std::vector<Segment> segments, std::vector<Location> index, uint64_t base, uint64_t term) {
  auto controls = Controls(segments, index.size(), base, term);
  storage_->Check(controls);
  storage_->Commit({{}, std::move(controls)});
  segments_.swap(segments);
  index_.swap(index);
  base_ = base;
  base_term_ = term;
  ++generation_;
}
void ObjectLogStore::Replace(uint64_t from, const std::vector<ReplicatedLogEntry> &entries) {
  if (from <= base_ || from <= commit_ || from > Last() + 1 || entries.size() > storage_->options_.max_batch_entries_) {
    throw std::invalid_argument("invalid object log replacement");
  }
  const size_t keep = from - base_ - 1;
  if (keep + entries.size() > storage_->options_.max_log_entries_) {
    throw std::invalid_argument("log index budget exceeded");
  }
  std::vector<Location> next(index_.begin(), index_.begin() + static_cast<ptrdiff_t>(keep));
  std::vector<Segment> segments = segments_;
  if (keep < index_.size()) {
    const auto cut = index_[keep];
    auto it = std::find_if(segments.begin(), segments.end(), [&](auto &s) { return s.object_ == cut.object_; });
    it->end_ = cut.offset_;
    if (it->begin_ == it->end_) {
      segments.erase(it, segments.end());
    } else {
      segments.erase(it + 1, segments.end());
    }
  }
  struct Pending {
    size_t segment_;
    std::vector<std::byte> data_;
    bool candidate_;
  };
  std::vector<Pending> pending;
  uint64_t batch_bytes = 0, live_bytes = 0;
  for (const auto &loc : next) {
    live_bytes += loc.size_;
  }
  uint64_t placeholder = UINT64_MAX;
  auto add_segment = [&] {
    segments.push_back({placeholder--, 0, 0});
    pending.push_back({segments.size() - 1, {}, true});
  };
  // Only append to a pristine published tail. Unpublished leftovers and suffix
  // replacements use new objects; old visible bytes remain immutable.
  if (!segments.empty() && keep == index_.size()) {
    auto &tail = segments.back();
    if (tail.end_ + HEADER < storage_->options_.segment_bytes_ &&
        storage_->storage_->Objects().Describe(storage_->Key(tail.object_)).size_ == tail.end_) {
      pending.push_back({segments.size() - 1, {}, false});
    }
  }
  for (size_t n = 0; n < entries.size(); ++n) {
    if (entries[n].index_ != from + n) {
      throw std::invalid_argument("non-contiguous Raft log input");
    }
    auto bytes = LogCodec::Encode(entries[n]);
    batch_bytes += bytes.size();
    live_bytes += bytes.size();
    if (batch_bytes > storage_->options_.max_batch_bytes_ || live_bytes > storage_->options_.max_log_bytes_) {
      throw std::invalid_argument("Raft log body budget exceeded");
    }
    size_t used = 0;
    while (used < bytes.size()) {
      if (pending.empty() || segments[pending.back().segment_].end_ + HEADER >= storage_->options_.segment_bytes_) {
        add_segment();
      }
      auto &p = pending.back();
      auto &s = segments[p.segment_];
      const auto amount = std::min<uint64_t>(bytes.size() - used, storage_->options_.segment_bytes_ - s.end_ - HEADER);
      if (used == 0) {
        next.push_back({entries[n].term_, s.object_, s.end_, bytes.size()});
      }
      const bool last = used + amount == bytes.size();
      const uint8_t type = used == 0 ? (last ? 1 : 2) : (last ? 4 : 3);
      ByteWriter h;
      h.PutU8(type);
      h.PutU32(amount);
      h.PutU32(Crc32cExtend(Crc32c(h.Data()), bytes.data() + used, amount));
      h.PutBytes(bytes.data() + used, amount);
      p.data_.insert(p.data_.end(), h.Data().begin(), h.Data().end());
      s.end_ += HEADER + amount;
      used += amount;
    }
  }
  // Placeholder identities have the same encoded size as allocated identities.
  // Preflight happens before ANY candidate body is written.
  const auto candidates =
      static_cast<size_t>(std::count_if(pending.begin(), pending.end(), [](const auto &p) { return p.candidate_; }));
  if (candidates > storage_->options_.max_owned_objects_ - storage_->owned_objects_.load()) {
    throw std::invalid_argument("Raft candidate ownership budget exceeded");
  }
  storage_->Check(Controls(segments, next.size(), base_, base_term_));
  std::map<uint64_t, uint64_t> allocated;
  try {
    for (auto &p : pending) {
      if (p.candidate_) {
        auto &s = segments[p.segment_];
        const auto id = storage_->Candidate(1);
        allocated.emplace(s.object_, id);
        s.object_ = id;
      }
    }
    for (auto &loc : next) {
      auto found = allocated.find(loc.object_);
      if (found != allocated.end()) {
        loc.object_ = found->second;
      }
    }
    for (const auto &p : pending) {
      if (!p.data_.empty()) {
        storage_->Append(segments[p.segment_].object_, p.data_);
      }
    }
    Publish(std::move(segments), std::move(next), base_, base_term_);
  } catch (...) {
    const auto error = std::current_exception();
    for (const auto &[unused, object] : allocated) {
      static_cast<void>(unused);
      try {
        storage_->Commit({{}, {storage_->Own(object, 1, 2)}});
      } catch (...) {
      }
    }
    std::rethrow_exception(error);
  }
}
auto ObjectLogStore::PlanCleaning() -> std::unique_ptr<Cleaning> {
  // Called under the log mutex; only metadata inspection/preflight, no body IO.
  if (segments_.empty() || index_.empty() || storage_->owned_objects_ >= storage_->options_.max_owned_objects_)
    return {};
  const auto view = storage_->storage_->Objects();
  uint64_t read_bytes = 0, old_bytes = 0;
  uint64_t best_gain = 0, best_cost = 1, body_bytes = 0;
  size_t best_segment = segments_.size(), best_entry = 0;
  for (size_t count = 1; count <= segments_.size() && count <= storage_->options_.max_batch_entries_; ++count) {
    const auto n = segments_.size() - count;
    const auto &segment = segments_[n];
    read_bytes += segment.end_ - segment.begin_;
    if (read_bytes > storage_->options_.io_chunk_bytes_ || read_bytes > storage_->options_.max_batch_bytes_) break;
    const auto key = storage_->Key(segment.object_);
    const auto size = view.Describe(key).size_;
    // Do cheap unmapping first; never count those dead bytes as relocation gain.
    if (DeadSpan(view, key, 0, segment.begin_, read_bytes) || DeadSpan(view, key, segment.end_, size, read_bytes))
      break;
    const auto estimate = view.EstimateRewrite(key);
    if (!estimate) break;  // Optional planning exceeded its bounded query budget.
    old_bytes += estimate->unshared_mapped_bytes_;
    const auto first =
        std::lower_bound(index_.begin(), index_.end(), std::make_pair(segment.object_, segment.begin_),
                         [](const Location &l, const auto &p) { return std::make_pair(l.object_, l.offset_) < p; });
    // A continuation alone is not a complete entry; include its preceding segment.
    if (first == index_.end() || first->object_ != segment.object_ || first->offset_ != segment.begin_) continue;
    const auto entries = static_cast<size_t>(index_.end() - first);
    if (entries > storage_->options_.max_batch_entries_) break;
    uint64_t written = 0;
    for (auto it = first; it != index_.end(); ++it) written += HEADER + it->size_;
    const auto unit = estimate->allocation_bytes_;
    const auto allocated = (written + unit - 1) / unit * unit;
    if (old_bytes <= allocated) continue;
    const auto gain = old_bytes - allocated;
    const auto cost = read_bytes + allocated;
    if (static_cast<long double>(gain) / cost <= static_cast<long double>(best_gain) / best_cost) continue;
    auto candidate = std::vector<Segment>(segments_.begin(), segments_.begin() + static_cast<ptrdiff_t>(n));
    candidate.push_back({UINT64_MAX, 0, written});
    const auto controls = Controls(candidate, index_.size(), base_, base_term_);
    try {
      // A larger tail can exceed the atomic publication budget even when its
      // body fits. Keep an earlier feasible tail instead of faulting the node.
      storage_->Check(controls);
    } catch (const std::invalid_argument &) {
      continue;  // Check only validates configured publication capacities.
    }
    best_gain = gain;
    best_cost = cost;
    best_segment = n;
    best_entry = first - index_.begin();
    body_bytes = written;
  }
  if (best_segment == segments_.size()) return {};
  auto account = ResourceAccount::Create(storage_->storage_->MemoryBudget());
  const auto bytes = 4 * body_bytes + index_.size() * sizeof(Location) + 2 * segments_.size() * sizeof(Segment) +
                     segments_.size() * sizeof(std::shared_ptr<void>);
  // Optional relocation must not hold the credits reserved for finishing
  // already committed work. Keep them available while this candidate waits.
  if (!account->Reserve(bytes, false)) return {};
  ResourceCharge charge(account, bytes, false);
  auto job = std::make_unique<Cleaning>();
  job->memory_ = std::move(charge);
  job->generation_ = generation_;
  job->base_ = base_;
  job->first_ = best_entry;
  job->sources_.assign(segments_.begin() + static_cast<ptrdiff_t>(best_segment), segments_.end());
  job->segments_.assign(segments_.begin(), segments_.begin() + static_cast<ptrdiff_t>(best_segment));
  job->segments_.push_back({UINT64_MAX, 0, body_bytes});
  job->index_ = index_;
  job->body_.reserve(body_bytes);
  for (const auto &segment : job->sources_) job->leases_.push_back(storage_->Lease(segment.object_));
  return job;
}
auto ObjectLogStore::Maintain() -> bool {
  std::unique_lock lock(*mutex_, std::try_to_lock);
  if (!lock.owns_lock()) return false;  // Yield when a foreground operation already owns the log.
  if (!cleaning_) {
    if (!segments_.empty()) {
      const auto &segment = segments_[clean_cursor_++ % segments_.size()];
      const auto key = storage_->Key(segment.object_);
      const auto view = storage_->storage_->Objects();
      auto dead = DeadSpan(view, key, 0, segment.begin_, storage_->options_.io_chunk_bytes_);
      if (!dead) dead = DeadSpan(view, key, segment.end_, view.Describe(key).size_, storage_->options_.io_chunk_bytes_);
      if (dead) {
        // Retired prefix/suffix bytes are never republished. Append reuses an object only when its
        // length equals the published end; this Unmap keeps that length intact.
        // Thus a selected dead range cannot become a future append destination.
        lock.unlock();
        ProgressWork progress(true);
        storage_->Commit({{std::move(*dead)}, {}});
        return true;
      }
    }
    cleaning_ = PlanCleaning();
    return cleaning_ != nullptr;
  }
  auto &job = *cleaning_;
  if (job.generation_ != generation_) job.step_ = Cleaning::Step::Retire;
  if (job.step_ == Cleaning::Step::Publish) {
    // Only the small final control publication serializes with foreground
    // mutation. All body reads and writes ran outside the log mutex.
    ProgressWork progress(true);
    auto controls = Controls(job.segments_, job.index_.size(), base_, base_term_);
    storage_->Check(controls);
    storage_->Commit({{}, std::move(controls)});
    segments_.swap(job.segments_);
    index_.swap(job.index_);
    ++generation_;
    cleaning_.reset();
    return true;
  }
  lock.unlock();
  switch (job.step_) {
    case Cleaning::Step::Read: {
      job.body_.clear();
      std::vector<Location> rewritten;
      rewritten.reserve(job.index_.size() - job.first_);
      for (size_t i = job.first_; i < job.index_.size(); ++i) {
        auto entry = ReadEntry(job.index_[i], job.sources_);
        if (entry.index_ != job.base_ + i + 1) throw std::runtime_error("cleaning log identity mismatch");
        auto encoded = LogCodec::Encode(entry);
        rewritten.push_back({entry.term_, UINT64_MAX, job.body_.size(), encoded.size()});
        ByteWriter header;
        header.PutU8(1);  // FULL: the entire chosen tail fits one segment.
        header.PutU32(encoded.size());
        header.PutU32(Crc32cExtend(Crc32c(header.Data()), encoded.data(), encoded.size()));
        job.body_.insert(job.body_.end(), header.Data().begin(), header.Data().end());
        job.body_.insert(job.body_.end(), encoded.begin(), encoded.end());
      }
      std::copy(rewritten.begin(), rewritten.end(), job.index_.begin() + static_cast<ptrdiff_t>(job.first_));
      job.step_ = Cleaning::Step::Create;
      break;
    }
    case Cleaning::Step::Create:
      job.object_ = storage_->Candidate(1);
      job.segments_.back().object_ = job.object_;
      for (size_t i = job.first_; i < job.index_.size(); ++i) job.index_[i].object_ = job.object_;
      job.step_ = Cleaning::Step::Write;
      break;
    case Cleaning::Step::Write:
      try {
        storage_->Append(job.object_, job.body_);  // At most one configured IO chunk.
        job.step_ = Cleaning::Step::Publish;
      } catch (const AllocationError &e) {
        if (e.Code() != AllocationErrorCode::NoSpace) throw;
        job.step_ = Cleaning::Step::Retire;  // Optional relocation lacks staging space.
      }
      break;
    case Cleaning::Step::Retire: {
      ProgressWork progress(true);
      if (job.object_ != 0) storage_->Commit({{}, {storage_->Own(job.object_, 1, 2)}});
      cleaning_.reset();
      break;
    }
    case Cleaning::Step::Publish:
      break;
  }
  return true;
}
auto ObjectLogStore::ReadEntry(const Location &loc, const std::vector<Segment> &segments) const -> ReplicatedLogEntry {
  auto it = std::find_if(segments.begin(), segments.end(), [&](const auto &s) { return s.object_ == loc.object_; });
  uint64_t offset = loc.offset_;
  std::vector<std::byte> body;
  body.reserve(loc.size_);
  bool first = true;
  while (body.size() < loc.size_) {
    if (it == segments.end()) {
      throw std::runtime_error("missing log continuation segment");
    }
    if (offset == it->end_) {
      ++it;
      if (it != segments.end()) {
        offset = it->begin_;
      }
      continue;
    }
    const auto head = storage_->Read(it->object_, offset, HEADER);
    ByteReader h(head);
    const auto type = h.ReadU8();
    const auto size = h.ReadU32();
    const auto crc = h.ReadU32();
    if (offset > it->end_ || it->end_ - offset < HEADER || size == 0 || size > it->end_ - offset - HEADER ||
        size > loc.size_ - body.size()) {
      throw std::runtime_error("invalid log fragment range");
    }
    const bool last = body.size() + size == loc.size_;
    if (type != (first ? (last ? 1 : 2) : (last ? 4 : 3))) {
      throw std::runtime_error("log fragment order mismatch");
    }
    auto bytes = storage_->Read(it->object_, offset + HEADER, size);
    if (crc != Crc32cExtend(Crc32c(head.data(), 5), bytes.data(), bytes.size())) {
      throw std::runtime_error("log fragment checksum mismatch");
    }
    body.insert(body.end(), bytes.begin(), bytes.end());
    offset += HEADER + size;
    first = false;
  }
  auto result = LogCodec::DecodeOne(body);
  if (result.status_ != LogDecodeStatus::COMPLETE || result.bytes_consumed_ != body.size() ||
      result.entry_->term_ != loc.term_) {
    throw std::runtime_error("invalid object log entry");
  }
  return std::move(*result.entry_);
}
auto ObjectLogStore::Entries(uint64_t first, uint64_t last) const -> std::vector<ReplicatedLogEntry> {
  if (first > last) {
    return {};
  }
  if (first <= base_ || last > Last()) {
    throw std::out_of_range("Raft log range unavailable");
  }
  // LogStore's existing mutation mutex fixes this directory for the whole read.
  std::vector<ReplicatedLogEntry> result;
  result.reserve(last - first + 1);
  for (auto i = first; i <= last; ++i) {
    auto entry = ReadEntry(index_[i - base_ - 1], segments_);
    if (entry.index_ != i) {
      throw std::runtime_error("Raft log identity mismatch");
    }
    result.push_back(std::move(entry));
  }
  return result;
}
auto ObjectLogStore::Term(uint64_t index) const -> std::optional<uint64_t> {
  if (index == base_) {
    return base_term_;
  }
  if (index <= base_ || index > Last()) {
    return std::nullopt;
  }
  return index_[index - base_ - 1].term_;
}
void ObjectLogStore::Advance(uint64_t index) {
  if (index < commit_ || index > Last()) {
    throw std::invalid_argument("invalid committed log boundary");
  }
  commit_ = index;
}
void ObjectLogStore::Base(uint64_t index, uint64_t term, bool retain) {
  if (index <= base_ || (retain && Term(index) != std::optional<uint64_t>(term))) {
    throw std::invalid_argument("invalid snapshot log base");
  }
  std::vector<Location> next;
  std::vector<Segment> segments;
  if (retain && index < Last()) {
    next.assign(index_.begin() + static_cast<ptrdiff_t>(index - base_), index_.end());
    auto start =
        std::find_if(segments_.begin(), segments_.end(), [&](auto &s) { return s.object_ == next.front().object_; });
    segments.assign(start, segments_.end());
    segments.front().begin_ = next.front().offset_;
  }
  Publish(std::move(segments), std::move(next), index, term);
  commit_ = std::max(commit_, index);
}
void ObjectLogStore::Rebuild(uint64_t index, uint64_t term) {
  Publish({}, {}, index, term);
  commit_ = index;
}
}  // namespace bustub
