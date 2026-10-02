//===----------------------------------------------------------------------===//
// BusTub: Data allocation facts in B, reusable summaries and reservation leases.
//===----------------------------------------------------------------------===//
#include "storage/disk/data_allocator.h"

#include <algorithm>
#include <functional>
#include <map>
#include <mutex>  // NOLINT(build/c++11)
#include <utility>

#include "common/byte_codec.h"
#include "storage/disk/region_manager.h"

namespace bustub {
namespace {
constexpr uint64_t CATEGORY = 12;
constexpr uint64_t HEADER = 0;
constexpr uint64_t COMMITTED = 1;
constexpr uint64_t QUARANTINE = 2;
constexpr uint32_t FORMAT = 1;
constexpr uint64_t MAGIC = 0x425354414c4c4f43ULL;  // BSTALLOC
using Words = std::vector<uint64_t>;

[[noreturn]] void Fail(AllocationErrorCode code, const char *message) { throw AllocationError(code, message); }
auto Ceil(uint64_t n, uint64_t d) -> uint64_t { return n / d + static_cast<uint64_t>(n % d != 0); }
auto LowMask(uint64_t n) -> uint64_t { return n == 64 ? ~uint64_t{0} : (uint64_t{1} << n) - 1; }
auto Range(uint64_t start, uint64_t length) -> StorageByteRange { return *StorageByteRange::Create(start, length); }
struct Summary {
  uint64_t count_;
  uint64_t coverage_;
  Words words_;
  auto Get(uint64_t i) const -> uint64_t { return (words_[i / 32] >> (2 * (i % 32))) & 3; }
  void Set(uint64_t i, uint64_t value) {
    auto &word = words_[i / 32];
    auto shift = 2 * (i % 32);
    word = (word & ~(uint64_t{3} << shift)) | (value << shift);
  }
};
struct AllocationContext {
  explicit AllocationContext(const RegionInfo &region, DataAllocatorOptions options)
      : options_(options), size_(region.size_), units_(region.size_ / options.allocation_bytes_) {
    const auto n = Ceil(units_, 64);
    uint64_t bytes = n * sizeof(uint64_t) * 3;
    for (auto count = n;; count = Ceil(count, 64)) {
      bytes += Ceil(count, 32) * sizeof(uint64_t);
      if (count == 1) {
        break;
      }
    }
    if (bytes > options_.max_bitmap_bytes_) {
      Fail(AllocationErrorCode::ResourceUnavailable, "allocator bitmap budget exceeded");
    }
    committed_.resize(n);
    reserved_.resize(n);
    quarantine_.resize(n);
    uint64_t coverage = 64;
    for (auto count = n;; count = Ceil(count, 64)) {
      levels_.push_back({count, coverage, Words(Ceil(count, 32))});
      if (count == 1) {
        break;
      }
      coverage = coverage > units_ / 64 ? units_ : coverage * 64;
    }
  }
  void Ready() const {
    if (!ready_ || closed_) {
      Fail(AllocationErrorCode::NotReady, "allocator is not open for allocation");
    }
  }
  auto ValidMask(uint64_t word) const -> uint64_t { return LowMask(std::min<uint64_t>(64, units_ - word * 64)); }
  auto Free(uint64_t word) const -> uint64_t {
    return ~(committed_[word] | reserved_[word] | quarantine_[word]) & ValidMask(word);
  }
  void Update(uint64_t word) {
    auto free = Free(word);
    levels_[0].Set(word, free == 0 ? 0 : (free == ValidMask(word) ? 2 : 1));
    for (size_t level = 1; level < levels_.size(); ++level) {
      auto parent = word / 64;
      bool none = true;
      bool all = true;
      const auto &children = levels_[level - 1];
      for (auto i = parent * 64; i < std::min(children.count_, parent * 64 + 64); ++i) {
        none = none && children.Get(i) == 0;
        all = all && children.Get(i) == 2;
      }
      levels_[level].Set(parent, none ? 0 : (all ? 2 : 1));
      word = parent;
    }
  }
  template <class Action>
  void EachWord(const std::vector<StorageByteRange> &ranges, Action action) const {
    for (const auto &range : ranges) {
      auto unit = range.Offset() / options_.allocation_bytes_;
      const auto end = unit + range.Size() / options_.allocation_bytes_;
      while (unit < end) {
        auto count = std::min<uint64_t>(64 - unit % 64, end - unit);
        action(unit / 64, LowMask(count) << (unit % 64));
        unit += count;
      }
    }
  }
  void Change(Words *words, const std::vector<StorageByteRange> &ranges, bool set) {
    EachWord(ranges, [&](uint64_t i, uint64_t mask) {
      (*words)[i] = set ? (*words)[i] | mask : (*words)[i] & ~mask;
      Update(i);
    });
  }
  void Validate(const std::vector<StorageByteRange> &ranges) const {
    if (ranges.empty() || ranges.size() > options_.max_extents_) {
      throw std::invalid_argument("allocator requires a bounded nonempty range list");
    }
    uint64_t total = 0;
    for (const auto &r : ranges) {
      const auto limit = units_ * options_.allocation_bytes_;
      if (r.Size() == 0 || r.Offset() % options_.allocation_bytes_ != 0 || r.Size() % options_.allocation_bytes_ != 0 ||
          r.Offset() > limit || r.Size() > limit - r.Offset()) {
        throw std::invalid_argument("allocator range is outside whole Data units");
      }
      if (r.Size() > options_.max_request_bytes_ - total) {
        Fail(AllocationErrorCode::ResourceUnavailable, "allocator operation byte budget exceeded");
      }
      total += r.Size();
    }
    // Lists are small and bounded. Reject overlap instead of double applying it.
    for (size_t i = 0; i < ranges.size(); ++i) {
      for (size_t j = 0; j < i; ++j) {
        if (ranges[i].Offset() < ranges[j].Offset() + ranges[j].Size() &&
            ranges[j].Offset() < ranges[i].Offset() + ranges[i].Size()) {
          throw std::invalid_argument("allocator ranges overlap");
        }
      }
    }
  }

  const DataAllocatorOptions options_;
  const uint64_t size_;
  const uint64_t units_;
  std::mutex mutex_;
  Words committed_, reserved_, quarantine_;
  std::vector<Summary> levels_;
  size_t tickets_{0};
  bool ready_{false};
  bool closed_{false};
};

// Excludes quarantine changes from concurrent Reserve while B is committing.
// Its lifetime ends after the final bitmap lock has been released. If B reports
// an uncertain outcome, Ready() has already been disabled before this rolls back.
struct ReservationFence {
  AllocationContext &context_;
  const std::vector<StorageByteRange> &ranges_;
  bool armed_{false};
  ~ReservationFence() {
    if (armed_) {
      std::lock_guard<std::mutex> lock(context_.mutex_);
      context_.Change(&context_.reserved_, ranges_, false);
    }
  }
};

auto Descriptor(const AllocationContext &c) -> std::vector<std::byte> {
  ByteWriter w;
  w.PutU64(MAGIC);
  w.PutU32(FORMAT);
  w.PutU64(c.size_);
  w.PutU64(c.options_.allocation_bytes_);
  w.PutU32(c.options_.bitmap_record_bytes_);
  return w.Take();
}
auto DecodeWords(const std::vector<std::byte> &bytes, uint32_t expected) -> Words {
  if (bytes.size() != expected) {
    Fail(AllocationErrorCode::Corrupt, "allocator bitmap record length mismatch");
  }
  ByteReader r(bytes);
  Words words(expected / 8);
  for (auto &word : words) {
    word = r.ReadU64();
  }
  return words;
}
}  // namespace

struct AllocationReservationState {
  enum class Phase { Pending, Committed, Unknown };
  std::shared_ptr<AllocationContext> context_;
  std::vector<StorageByteRange> ranges_;
  Phase phase_{Phase::Pending};
  bool armed_{false};
  ~AllocationReservationState() {
    if (!armed_) {
      return;
    }
    std::lock_guard<std::mutex> lock(context_->mutex_);
    if (phase_ == Phase::Pending) {
      context_->Change(&context_->reserved_, ranges_, false);
    }
    --context_->tickets_;
  }
};

DataReservation::DataReservation(std::shared_ptr<AllocationReservationState> state) : state_(std::move(state)) {}
DataAllocationLease::DataAllocationLease(std::shared_ptr<AllocationReservationState> state)
    : state_(std::move(state)) {}
auto DataReservation::Extents() const -> const std::vector<StorageByteRange> & {
  if (!state_) {
    throw std::logic_error("moved allocator reservation");
  }
  return state_->ranges_;
}
auto DataReservation::Lease() const -> DataAllocationLease {
  Extents();
  std::lock_guard<std::mutex> lock(state_->context_->mutex_);
  state_->context_->Ready();
  if (state_->phase_ != AllocationReservationState::Phase::Pending) {
    throw std::logic_error("only a pending reservation can issue a lease");
  }
  return DataAllocationLease(state_);
}

struct DataAllocator::Impl {
  Impl(MetadataEngine &metadata, const RegionInfo &region, DataAllocatorOptions options) : metadata_(metadata) {
    if (options.allocation_bytes_ == 0 || options.allocation_bytes_ % region.offset_alignment_ != 0 ||
        options.allocation_bytes_ > region.size_ || options.bitmap_record_bytes_ == 0 ||
        options.bitmap_record_bytes_ % 8 != 0 || options.max_request_bytes_ < options.allocation_bytes_ ||
        options.max_extents_ == 0 || options.max_reservations_ == 0 || options.max_search_nodes_ == 0) {
      throw std::invalid_argument("allocator requires aligned geometry and positive explicit budgets");
    }
    context_ = std::make_shared<AllocationContext>(region, options);
  }
  void Start() {
    std::lock_guard<std::mutex> lock(context_->mutex_);
    if (started_ || context_->closed_) {
      throw std::logic_error("allocator startup requires a fresh instance");
    }
    started_ = true;
  }
  auto Create() -> JournalResult {
    std::lock_guard<std::mutex> commit(commit_mutex_);
    Start();
    const auto base = metadata_.Read();
    const auto existing = base.Scan({CATEGORY, 0, 0}, 1);
    if (!existing.empty() && existing.front().key_.category_ == CATEGORY) {
      Fail(AllocationErrorCode::AlreadyInitialized, "allocator namespace is not empty");
    }
    auto result = metadata_.Commit(base, {{{CATEGORY, HEADER, 0}, Descriptor(*context_)}});
    std::lock_guard<std::mutex> lock(context_->mutex_);
    if (result.outcome_ == JournalOutcome::Durable) {
      for (size_t i = 0; i < context_->committed_.size(); ++i) {
        context_->Update(i);
      }
      context_->ready_ = !context_->closed_;
    }
    return result;
  }
  void Open() {
    std::lock_guard<std::mutex> commit(commit_mutex_);
    Start();
    auto &c = *context_;
    const auto base = metadata_.Read();
    const auto descriptor = base.Get({CATEGORY, HEADER, 0});
    if (!descriptor) {
      Fail(AllocationErrorCode::NotInitialized, "allocator descriptor is missing; Open never formats");
    }
    if (*descriptor != Descriptor(c)) {
      Fail(AllocationErrorCode::Corrupt, "allocator format or Data geometry mismatch");
    }
    const uint64_t words_per_record = c.options_.bitmap_record_bytes_ / 8;
    MetadataKey cursor{CATEGORY, HEADER, 0};
    for (;;) {
      const auto rows = base.Scan(cursor, 64);
      bool done = rows.empty();
      for (const auto &row : rows) {
        if (row.key_.category_ != CATEGORY) {
          done = true;
          break;
        }
        if (row.key_.owner_ == HEADER && row.key_.item_ == 0) {
          cursor = {CATEGORY, HEADER, 1};
          continue;
        }
        if ((row.key_.owner_ != COMMITTED && row.key_.owner_ != QUARANTINE) ||
            row.key_.item_ >= Ceil(c.committed_.size(), words_per_record)) {
          Fail(AllocationErrorCode::Corrupt, "allocator record has an unknown range or category");
        }
        auto words = DecodeWords(row.value_, c.options_.bitmap_record_bytes_);
        auto &target = row.key_.owner_ == COMMITTED ? c.committed_ : c.quarantine_;
        for (size_t i = 0; i < words.size(); ++i) {
          auto index = row.key_.item_ * words_per_record + i;
          if (index < target.size()) {
            if ((words[i] & ~c.ValidMask(index)) != 0) {
              Fail(AllocationErrorCode::Corrupt, "allocator record sets unavailable tail units");
            }
            target[index] = words[i];
          } else if (words[i] != 0) {
            Fail(AllocationErrorCode::Corrupt, "allocator record extends past Data units");
          }
        }
        cursor = {CATEGORY, row.key_.owner_, row.key_.item_ + 1};
      }
      if (done) {
        break;
      }
    }
    std::lock_guard<std::mutex> lock(c.mutex_);
    for (size_t i = 0; i < c.committed_.size(); ++i) {
      c.Update(i);
    }
    c.ready_ = !c.closed_;
  }
  auto Reserve(uint64_t bytes) -> DataReservation {
    auto &c = *context_;
    if (bytes == 0 || bytes > c.options_.max_request_bytes_) {
      throw std::invalid_argument("allocation size must be nonzero and within its byte budget");
    }
    const auto need = Ceil(bytes, c.options_.allocation_bytes_);
    if (need > c.options_.max_request_bytes_ / c.options_.allocation_bytes_) {
      Fail(AllocationErrorCode::ResourceUnavailable, "rounded allocation exceeds byte budget");
    }
    auto state = std::make_shared<AllocationReservationState>();
    state->context_ = context_;
    state->ranges_.reserve(c.options_.max_extents_);
    std::lock_guard<std::mutex> lock(c.mutex_);
    c.Ready();
    if (c.tickets_ >= c.options_.max_reservations_) {
      Fail(AllocationErrorCode::Busy, "allocator reservation budget full");
    }
    uint64_t run_start = 0;
    uint64_t run_size = 0;
    uint64_t selected = 0;
    uint64_t seen = 0;
    uint64_t visits = 0;
    bool found = false;
    bool exhausted = false;
    auto save_fragment = [&] {
      if (selected < need && run_size != 0 && state->ranges_.size() < c.options_.max_extents_) {
        const auto take = std::min(run_size, need - selected);
        state->ranges_.push_back(Range(run_start * c.options_.allocation_bytes_, take * c.options_.allocation_bytes_));
        selected += take;
      }
    };
    auto emit = [&](uint64_t start, uint64_t length) {
      seen += length;
      if (run_size != 0 && run_start + run_size != start) {
        save_fragment();
        run_size = 0;
      }
      if (run_size == 0) {
        run_start = start;
      }
      run_size += length;
      if (run_size >= need) {
        state->ranges_.clear();
        state->ranges_.push_back(Range(run_start * c.options_.allocation_bytes_, need * c.options_.allocation_bytes_));
        found = true;
      }
    };
    std::function<void(size_t, uint64_t)> visit = [&](size_t level, uint64_t index) {
      if (found || exhausted) {
        return;
      }
      if (++visits > c.options_.max_search_nodes_) {
        exhausted = true;
        return;
      }
      const auto &summary = c.levels_[level];
      const auto status = summary.Get(index);
      if (status == 0) {
        return;
      }
      if (status == 2) {
        const auto start = index * summary.coverage_;
        emit(start, std::min(summary.coverage_, c.units_ - start));
      } else if (level != 0) {
        for (auto child = index * 64; child < std::min(c.levels_[level - 1].count_, index * 64 + 64); ++child) {
          visit(level - 1, child);
        }
      } else {
        auto free = c.Free(index);
        while (free != 0 && !found) {
          auto start = static_cast<uint64_t>(__builtin_ctzll(free));
          auto remaining = free >> start;
          auto length = remaining == ~uint64_t{0} ? 64 : static_cast<uint64_t>(__builtin_ctzll(~remaining));
          emit(index * 64 + start, length);
          free &= ~(LowMask(length) << start);
        }
      }
    };
    visit(c.levels_.size() - 1, 0);
    if (!found) {
      save_fragment();
      if (selected < need) {
        Fail(!exhausted && seen < need ? AllocationErrorCode::NoSpace : AllocationErrorCode::ResourceUnavailable,
             "allocation cannot satisfy available space or search/extent budget");
      }
    }
    c.Change(&c.reserved_, state->ranges_, true);
    ++c.tickets_;
    state->armed_ = true;
    return DataReservation(std::move(state));
  }

  auto Change(const MetadataSnapshot &base, const std::vector<StorageByteRange> &ranges,
              const std::vector<MetadataMutation> &related, uint64_t owner, bool set,
              AllocationReservationState *reservation) -> JournalResult {
    std::lock_guard<std::mutex> commit(commit_mutex_);
    auto &c = *context_;
    c.Validate(ranges);
    for (const auto &mutation : related) {
      if (mutation.key_.category_ == CATEGORY) {
        throw std::invalid_argument("companion metadata cannot modify allocator records");
      }
    }
    ReservationFence fence{c, ranges};
    {
      std::lock_guard<std::mutex> lock(c.mutex_);
      c.Ready();
      if (reservation != nullptr &&
          (reservation->context_ != context_ || reservation->phase_ != AllocationReservationState::Phase::Pending)) {
        throw std::invalid_argument("reservation is foreign or no longer pending");
      }
      c.EachWord(ranges, [&](uint64_t i, uint64_t mask) {
        if (reservation != nullptr) {
          if ((c.reserved_[i] & mask) != mask || ((c.committed_[i] | c.quarantine_[i]) & mask) != 0) {
            throw std::logic_error("reservation no longer covers free Data space");
          }
        } else if ((c.reserved_[i] & mask) != 0) {
          Fail(AllocationErrorCode::ResourceUnavailable, "range still has a live reservation");
        } else if (owner == COMMITTED && (c.committed_[i] & mask) != mask) {
          throw std::invalid_argument("release includes unallocated space");
        }
      });
      if (owner == QUARANTINE) {
        c.Change(&c.reserved_, ranges, true);
        fence.armed_ = true;
      }
    }
    const auto per_record = c.options_.bitmap_record_bytes_ / 8;
    std::map<uint64_t, Words> records;
    c.EachWord(ranges, [&](uint64_t i, uint64_t mask) {
      auto id = i / per_record;
      auto entry = records.find(id);
      if (entry == records.end()) {
        const auto value = base.Get({CATEGORY, owner, id});
        auto words = value ? DecodeWords(*value, c.options_.bitmap_record_bytes_) : Words(per_record);
        entry = records.emplace(id, std::move(words)).first;
      }
      auto &word = entry->second[i % per_record];
      word = set ? word | mask : word & ~mask;
    });
    std::vector<MetadataMutation> mutations = related;
    mutations.reserve(mutations.size() + records.size());
    for (const auto &[id, words] : records) {
      ByteWriter w;
      for (auto word : words) {
        w.PutU64(word);
      }
      mutations.push_back({{CATEGORY, owner, id}, w.Take()});
    }
    // The base is deliberately not refreshed here. B owns conflict detection;
    // refreshing it without revalidating companion metadata would lose updates.
    auto result = metadata_.Commit(base, mutations);
    std::lock_guard<std::mutex> lock(c.mutex_);
    if (result.outcome_ != JournalOutcome::Durable) {
      c.ready_ = false;
      if (reservation != nullptr) {
        reservation->phase_ = AllocationReservationState::Phase::Unknown;
      }
      return result;
    }
    c.Change(owner == COMMITTED ? &c.committed_ : &c.quarantine_, ranges, set);
    if (reservation != nullptr) {
      c.Change(&c.reserved_, ranges, false);
      reservation->phase_ = AllocationReservationState::Phase::Committed;
    }
    return result;
  }
  void Close() {
    {
      std::lock_guard<std::mutex> lock(context_->mutex_);
      context_->closed_ = true;
    }
    std::lock_guard<std::mutex> commit(commit_mutex_);
  }
  MetadataEngine &metadata_;
  std::shared_ptr<AllocationContext> context_;
  std::mutex commit_mutex_;
  bool started_{false};
};

DataAllocator::DataAllocator(MetadataEngine &metadata, DataAllocatorOptions options)
    : impl_(std::make_unique<Impl>(metadata, metadata.DataRegionInfo(), options)) {}
DataAllocator::~DataAllocator() { Close(); }
auto DataAllocator::Metadata() const -> MetadataEngine & { return impl_->metadata_; }
auto DataAllocator::AllocationUnit() const -> uint64_t { return impl_->context_->options_.allocation_bytes_; }
auto DataAllocator::Create() -> JournalResult { return impl_->Create(); }
void DataAllocator::Open() { impl_->Open(); }
auto DataAllocator::Reserve(uint64_t bytes) -> DataReservation { return impl_->Reserve(bytes); }
auto DataAllocator::Commit(const MetadataSnapshot &base, DataReservation &reservation,
                           const std::vector<MetadataMutation> &related) -> JournalResult {
  return impl_->Change(base, reservation.Extents(), related, COMMITTED, true, reservation.state_.get());
}
auto DataAllocator::Release(const MetadataSnapshot &base, const std::vector<StorageByteRange> &ranges,
                            const std::vector<MetadataMutation> &related) -> JournalResult {
  return impl_->Change(base, ranges, related, COMMITTED, false, nullptr);
}
auto DataAllocator::SetQuarantine(const MetadataSnapshot &base, const std::vector<StorageByteRange> &ranges,
                                  bool isolated) -> JournalResult {
  return impl_->Change(base, ranges, {}, QUARANTINE, isolated, nullptr);
}
void DataAllocator::Close() { impl_->Close(); }

}  // namespace bustub
