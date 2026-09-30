//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// journal_service.cpp
//
//===----------------------------------------------------------------------===//

#include "storage/disk/journal_service.h"

#include <algorithm>
#include <condition_variable>  // NOLINT(build/c++11)
#include <cstring>
#include <limits>
#include <list>
#include <mutex>   // NOLINT(build/c++11)
#include <thread>  // NOLINT(build/c++11)
#include <utility>

#include "common/byte_codec.h"
#include "storage/disk/journal_backend.h"

namespace bustub {
namespace {

constexpr size_t UNIT_HEADER = 64;
constexpr size_t FRAGMENT_HEADER = 16;
constexpr size_t COMMIT_BYTES = 24;
constexpr uint32_t FULL = 1;
constexpr uint32_t FIRST = 2;
constexpr uint32_t MIDDLE = 3;
constexpr uint32_t LAST = 4;
constexpr uint32_t COMMIT = 5;
constexpr char FORMAT_MAGIC[] = "BUSTJFMT";
constexpr char UNIT_MAGIC[] = "BUSTJUNT";
constexpr char SEGMENT_MAGIC[] = "BUSTJSEG";

auto AllZero(const std::byte *data, size_t size) -> bool {
  return std::all_of(data, data + size, [](std::byte byte) { return byte == std::byte{0}; });
}

void Require(bool condition, const char *message) {
  if (!condition) {
    throw JournalError(JournalErrorCode::Corrupt, message);
  }
}

void CheckIO(const IOBatch &batch) {
  const auto &result = batch.Result();
  for (const auto &member : result.operations_) {
    if (member.error_) {
      std::rethrow_exception(member.error_);
    }
    if (member.outcome_ != IOOutcome::Succeeded) {
      throw JournalError(JournalErrorCode::ExecutorStopped, "Journal IO did not execute");
    }
  }
  if (result.flush_error_) {
    std::rethrow_exception(result.flush_error_);
  }
}

void CheckAdmission(IOAdmission admission) {
  if (admission != IOAdmission::Accepted) {
    throw JournalError(
        admission == IOAdmission::Full ? JournalErrorCode::ResourceUnavailable : JournalErrorCode::ExecutorStopped,
        "Journal cannot reserve or submit required IO");
  }
}

struct Fragment {
  uint32_t type_;
  uint32_t record_;
  uint32_t offset_;
  uint32_t size_;
};

struct UnitPlan {
  std::vector<Fragment> fragments_;
  size_t used_{0};
};

auto Plan(const JournalRecords &records, const JournalOptions &options) -> std::vector<UnitPlan> {
  if (records.empty() || records.size() > options.max_records_per_batch_) {
    throw std::invalid_argument("Journal requires a bounded nonempty batch");
  }
  uint64_t total = 0;
  for (const auto &record : records) {
    if (record.empty() || record.size() > options.max_record_bytes_ ||
        record.size() > options.max_batch_bytes_ - total) {
      throw std::invalid_argument("Journal record or batch exceeds configured limit");
    }
    total += record.size();
  }
  std::vector<UnitPlan> units;
  const auto capacity = options.unit_bytes_ - UNIT_HEADER - sizeof(uint32_t);
  const auto new_unit = [&]() {
    if (units.size() >= options.max_group_bytes_ / options.unit_bytes_) {
      throw std::invalid_argument("encoded Journal batch exceeds group limit");
    }
    units.emplace_back();
  };
  new_unit();
  for (size_t i = 0; i < records.size(); ++i) {
    uint32_t offset = 0;
    const auto length = static_cast<uint32_t>(records[i].size());
    while (offset < length) {
      if (capacity - units.back().used_ <= FRAGMENT_HEADER) {
        new_unit();
      }
      auto &unit = units.back();
      const auto count =
          static_cast<uint32_t>(std::min<size_t>(length - offset, capacity - unit.used_ - FRAGMENT_HEADER));
      const bool last = offset + count == length;
      const auto type = offset == 0 ? (last ? FULL : FIRST) : (last ? LAST : MIDDLE);
      unit.fragments_.push_back({type, static_cast<uint32_t>(i), offset, count});
      unit.used_ += FRAGMENT_HEADER + count;
      offset += count;
    }
  }
  if (capacity - units.back().used_ < FRAGMENT_HEADER + COMMIT_BYTES) {
    new_unit();
  }
  units.back().fragments_.push_back({COMMIT, static_cast<uint32_t>(records.size()), 0, COMMIT_BYTES});
  units.back().used_ += FRAGMENT_HEADER + COMMIT_BYTES;
  return units;
}

void AddChecksumAndPadding(ByteWriter *writer, uint32_t unit_bytes) {
  while (writer->Data().size() < unit_bytes - sizeof(uint32_t)) {
    writer->PutU8(0);
  }
  writer->PutU32(Crc32c(writer->Data()));
}

void CheckUnitChecksum(const std::vector<std::byte> &unit) {
  ByteReader checksum(unit.data() + unit.size() - sizeof(uint32_t), sizeof(uint32_t));
  Require(checksum.ReadU32() == Crc32c(unit.data(), unit.size() - sizeof(uint32_t)), "Journal unit checksum mismatch");
}

struct TicketBudget {
  std::mutex mutex_;
  size_t tickets_{0};
  uint64_t bytes_{0};
};

}  // namespace

struct JournalTicketData {
  JournalTicketData(std::shared_ptr<TicketBudget> budget, uint64_t begin, uint64_t bytes)
      : budget_(std::move(budget)), bytes_(bytes), result_{JournalOutcome::NotCommitted, begin, begin + bytes, {}} {}
  ~JournalTicketData() {
    if (reserved_) {
      std::lock_guard<std::mutex> lock(budget_->mutex_);
      --budget_->tickets_;
      budget_->bytes_ -= bytes_;
    }
  }
  void Complete(JournalOutcome outcome, std::exception_ptr error) {
    // IO handles must be drained and destroyed before releasing work bytes.
    {
      std::lock_guard<std::mutex> lock(budget_->mutex_);
      budget_->bytes_ -= bytes_;
      bytes_ = 0;
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      result_.outcome_ = outcome;
      result_.error_ = std::move(error);
      done_ = true;
    }
    ready_.notify_all();
  }
  std::shared_ptr<TicketBudget> budget_;
  uint64_t bytes_;
  bool reserved_{false};
  mutable std::mutex mutex_;
  mutable std::condition_variable ready_;
  bool done_{false};
  JournalResult result_;
};

JournalTicket::JournalTicket(std::shared_ptr<JournalTicketData> data) : data_(std::move(data)) {}
JournalTicket::JournalTicket(JournalTicket &&) noexcept = default;
auto JournalTicket::operator=(JournalTicket &&) noexcept -> JournalTicket & = default;
JournalTicket::~JournalTicket() = default;
void JournalTicket::Wait() const {
  if (!data_) {
    throw std::logic_error("empty Journal ticket");
  }
  std::unique_lock<std::mutex> lock(data_->mutex_);
  data_->ready_.wait(lock, [&] { return data_->done_; });
}
auto JournalTicket::WaitFor(std::chrono::milliseconds timeout) const -> bool {
  if (!data_) {
    throw std::logic_error("empty Journal ticket");
  }
  std::unique_lock<std::mutex> lock(data_->mutex_);
  return data_->ready_.wait_for(lock, timeout, [&] { return data_->done_; });
}
auto JournalTicket::Result() const -> JournalResult {
  if (!data_) {
    throw std::logic_error("empty Journal ticket");
  }
  std::lock_guard<std::mutex> lock(data_->mutex_);
  if (!data_->done_) {
    throw std::logic_error("Journal result not terminal");
  }
  return data_->result_;
}

struct JournalService::Impl {
  struct Request {
    std::shared_ptr<JournalTicketData> ticket_;
    std::optional<IOBatch> data_;
    std::optional<IOBatch> flush_;
    uint64_t bytes_;
    bool submitted_{false};
  };

  Impl(BootstrapStore &bootstrap, IOExecutor &executor, const BootstrapIdentity &storage,
       const JournalIdentity &identity, const JournalOptions &options)
      : executor_(executor),
        regions_(bootstrap),
        backend_(regions_, options.segment_bytes_),
        storage_(storage),
        identity_(identity),
        options_(options),
        capacity_(backend_.SegmentCapacity() * options.segment_bytes_),
        budget_(std::make_shared<TicketBudget>()) {
    if (std::all_of(identity.begin(), identity.end(), [](uint8_t value) { return value == 0; }) ||
        options.unit_bytes_ < 256 || options.unit_bytes_ % executor.DeviceInfo().offset_alignment_ != 0 ||
        options.segment_bytes_ % options.unit_bytes_ != 0 || backend_.SegmentCapacity() < 2 ||
        options.max_record_bytes_ == 0 || options.max_batch_bytes_ < options.max_record_bytes_ ||
        options.max_records_per_batch_ == 0 || options.max_group_bytes_ < options.unit_bytes_ ||
        options.max_group_bytes_ > std::numeric_limits<size_t>::max() || options.max_group_batches_ == 0 ||
        options.max_pending_batches_ < options.max_group_batches_ ||
        options.max_pending_bytes_ < options.max_group_bytes_) {
      throw std::invalid_argument("invalid Journal identity, geometry or resource limits");
    }
    group_.reserve(options.max_group_batches_);
  }

  auto Control() const -> std::vector<std::byte> {
    ByteWriter writer;
    writer.PutBytes(FORMAT_MAGIC, 8);
    writer.PutU32(format_);
    writer.PutU32(options_.unit_bytes_);
    writer.PutBytes(storage_.storage_.data(), storage_.storage_.size());
    writer.PutBytes(storage_.device_.data(), storage_.device_.size());
    writer.PutBytes(identity_.data(), identity_.size());
    writer.PutU64(options_.segment_bytes_);
    writer.PutU64(backend_.SegmentCapacity());
    writer.PutU64(format_ == 1 ? backend_.SegmentCapacity() - 1 : reserved_sequence_);
    writer.PutU32(options_.max_record_bytes_);
    writer.PutU64(options_.max_batch_bytes_);
    writer.PutU32(options_.max_records_per_batch_);
    if (format_ == 2) {
      writer.PutU64(retain_from_);
    }
    AddChecksumAndPadding(&writer, options_.unit_bytes_);
    return writer.Take();
  }

  auto PrepareUnit(uint64_t position, IOOperation operation, bool flush) -> IOBatch {
    auto prepared = backend_.TryPrepare(
        {{position / options_.segment_bytes_, operation, position % options_.segment_bytes_, options_.unit_bytes_}},
        flush);
    CheckAdmission(prepared.admission_);
    return std::move(*prepared.batch_);
  }

  void SubmitAndWait(IOBatch &batch) {
    CheckAdmission(executor_.TrySubmit(batch));
    batch.Wait();
    CheckIO(batch);
  }

  auto ReadUnit(uint64_t position) -> std::vector<std::byte> {
    auto batch = PrepareUnit(position, IOOperation::Read, false);
    SubmitAndWait(batch);
    const auto *data = reinterpret_cast<const std::byte *>(batch.Buffer(0));
    return {data, data + options_.unit_bytes_};
  }

  void Flush() {
    auto prepared = executor_.TryPrepareFlush();
    CheckAdmission(prepared.admission_);
    SubmitAndWait(*prepared.batch_);
  }

  void BeginLifecycle() {
    if (attempted_ || closed_) {
      throw std::logic_error("Journal lifecycle already started or closed");
    }
    attempted_ = true;
  }

  void Start(uint64_t cursor, uint64_t previous) {
    cursor_ = cursor;
    previous_ = previous;
    // Thread construction failure leaves this instance closed to submissions.
    worker_ = std::thread([this] { Run(); });
    std::lock_guard<std::mutex> lock(mutex_);
    ready_ = true;
  }

  void Create() {
    BeginLifecycle();
    for (uint64_t position = 0; position < capacity_; position += options_.unit_bytes_) {
      const auto unit = ReadUnit(position);
      if (!AllZero(unit.data(), unit.size())) {
        throw JournalError(JournalErrorCode::NotEmpty, "Journal Create refuses nonempty storage");
      }
    }
    if (backend_.SegmentCapacity() < 3 || options_.segment_bytes_ / options_.unit_bytes_ < 2 ||
        options_.max_group_bytes_ < 2ULL * options_.unit_bytes_) {
      throw std::invalid_argument("recyclable Journal needs two data slots and a header plus data per slot");
    }
    format_ = 2;
    retain_from_ = options_.segment_bytes_ + options_.unit_bytes_;
    slot_sequences_.assign(backend_.SegmentCapacity(), 0);
    ReserveSequences();
    next_sequence_ = 1;
    Start(0, 0);
  }

  auto Slot(uint64_t sequence) const -> uint64_t { return 1 + (sequence - 1) % (backend_.SegmentCapacity() - 1); }

  auto Physical(uint64_t lsn) const -> uint64_t {
    if (format_ == 1) {
      return lsn;
    }
    return Slot(lsn / options_.segment_bytes_) * options_.segment_bytes_ + lsn % options_.segment_bytes_;
  }

  auto SegmentHeader(uint64_t sequence) const -> std::vector<std::byte> {
    ByteWriter w;
    w.PutBytes(SEGMENT_MAGIC, 8);
    w.PutU32(2);
    w.PutU32(options_.unit_bytes_);
    w.PutBytes(identity_.data(), identity_.size());
    w.PutU64(sequence);
    w.PutU64(Slot(sequence));
    AddChecksumAndPadding(&w, options_.unit_bytes_);
    return w.Take();
  }

  void SaveControl() {
    const auto bytes = Control();
    auto io = PrepareUnit(0, IOOperation::Write, true);
    std::memcpy(io.Buffer(0), bytes.data(), bytes.size());
    SubmitAndWait(io);
  }

  // Reserve a whole ring's worth of identities. At reopen, unused numbers from
  // the previous reservation are skipped; modulo preserves the next slot.
  void ReserveSequences() {
    const auto count = backend_.SegmentCapacity() - 1;
    const auto limit = std::numeric_limits<uint64_t>::max() / options_.segment_bytes_ - 1;
    if (next_sequence_ > limit || count - 1 > limit - next_sequence_) {
      throw JournalError(JournalErrorCode::ResourceUnavailable, "Journal sequence space exhausted");
    }
    // A reopen can start near the end of a reservation interval. Cover one
    // complete physical ring from that next identity, not just to the interval end.
    const auto needed = next_sequence_ + count - 1;
    const auto rounds = needed / count + (needed % count != 0 ? 1 : 0);
    if (rounds > limit / count) {
      throw JournalError(JournalErrorCode::ResourceUnavailable, "Journal sequence space exhausted");
    }
    reserved_sequence_ = std::max(reserved_sequence_, rounds * count);
    SaveControl();
  }

  void LoadSegments() {
    slot_sequences_.assign(backend_.SegmentCapacity(), 0);
    segments_.clear();
    for (uint64_t slot = 1; slot < backend_.SegmentCapacity(); ++slot) {
      const auto image = ReadUnit(slot * options_.segment_bytes_);
      if (AllZero(image.data(), image.size())) {
        // A never initialized slot must not hide an already written body.
        for (uint64_t offset = options_.unit_bytes_; offset < options_.segment_bytes_; offset += options_.unit_bytes_) {
          const auto body = ReadUnit(slot * options_.segment_bytes_ + offset);
          Require(AllZero(body.data(), body.size()), "Journal data has no segment header");
        }
        continue;
      }
      CheckUnitChecksum(image);
      ByteReader r(image);
      const auto magic = r.ReadBytes(8);
      Require(
          std::memcmp(magic.data(), SEGMENT_MAGIC, 8) == 0 && r.ReadU32() == 2 && r.ReadU32() == options_.unit_bytes_,
          "Journal segment format mismatch");
      const auto identity = r.ReadBytes(16);
      Require(std::memcmp(identity.data(), identity_.data(), 16) == 0, "Journal segment identity mismatch");
      const auto sequence = r.ReadU64();
      Require(sequence != 0 && sequence <= reserved_sequence_ && Slot(sequence) == slot && r.ReadU64() == slot,
              "Journal segment ownership mismatch");
      Require(AllZero(image.data() + 48, image.size() - 48 - sizeof(uint32_t)),
              "Journal segment reserved bytes mismatch");
      slot_sequences_[slot] = sequence;
      if (sequence >= retain_from_ / options_.segment_bytes_) {
        segments_.push_back(sequence);
      }
    }
    std::sort(segments_.begin(), segments_.end());
  }

  struct AppendPlan {
    std::vector<uint64_t> positions_;
    std::vector<uint64_t> new_segments_;
    std::vector<JournalIORequest> requests_;
    uint64_t cursor_;
    uint64_t next_sequence_;
    uint64_t bytes_{0};
  };

  auto AvailableUnits() const -> uint64_t {
    uint64_t result = 0;
    for (uint64_t slot = 1; slot < slot_sequences_.size(); ++slot) {
      const auto sequence = slot_sequences_[slot];
      if (sequence != 0 && sequence == cursor_ / options_.segment_bytes_) {
        result += (options_.segment_bytes_ - cursor_ % options_.segment_bytes_) / options_.unit_bytes_;
      } else if (sequence == 0 || sequence < retain_from_ / options_.segment_bytes_) {
        result += options_.segment_bytes_ / options_.unit_bytes_ - 1;
      }
    }
    return result;
  }

  auto Layout(size_t units) const -> AppendPlan {
    AppendPlan plan{{}, {}, {}, cursor_, next_sequence_};
    plan.positions_.reserve(units);
    if (format_ == 1) {
      plan.requests_ = backend_.PlanAppend(cursor_, units * options_.unit_bytes_);
      for (size_t i = 0; i < units; ++i) {
        plan.positions_.push_back(cursor_ + i * options_.unit_bytes_);
      }
      plan.cursor_ += units * options_.unit_bytes_;
      plan.bytes_ = units * options_.unit_bytes_;
      return plan;
    }
    for (size_t i = 0; i < units; ++i) {
      if (plan.cursor_ == 0 || plan.cursor_ % options_.segment_bytes_ == 0) {
        const auto sequence = plan.next_sequence_++;
        Require(sequence <= reserved_sequence_, "Journal reservation exhausted before reclamation");
        const auto slot = Slot(sequence);
        Require(slot_sequences_[slot] == 0 || slot_sequences_[slot] < retain_from_ / options_.segment_bytes_,
                "Journal append would overwrite a retained segment");
        plan.new_segments_.push_back(sequence);
        plan.cursor_ = sequence * options_.segment_bytes_ + options_.unit_bytes_;
        plan.requests_.push_back({slot, IOOperation::Write, 0, options_.unit_bytes_});
        plan.bytes_ += options_.unit_bytes_;
      }
      const auto sequence = plan.cursor_ / options_.segment_bytes_;
      const auto slot = Slot(sequence);
      const auto offset = plan.cursor_ % options_.segment_bytes_;
      if (!plan.requests_.empty() && plan.requests_.back().segment_slot_ == slot &&
          plan.requests_.back().offset_ + plan.requests_.back().size_ == offset) {
        plan.requests_.back().size_ += options_.unit_bytes_;
      } else {
        plan.requests_.push_back({slot, IOOperation::Write, offset, options_.unit_bytes_});
      }
      plan.positions_.push_back(plan.cursor_);
      plan.cursor_ += options_.unit_bytes_;
      plan.bytes_ += options_.unit_bytes_;
    }
    return plan;
  }

  // Two passes: all structural checks precede replay side effects. The owner
  // excludes external writes to this region throughout Open and normal use.
  auto Scan(uint64_t start, const std::function<void(uint64_t, const JournalRecords &)> &replay, bool anchored)
      -> std::pair<uint64_t, uint64_t> {
    uint64_t previous = 0;
    uint64_t begin = start;
    uint64_t cursor = begin;
    uint64_t total = 0;
    uint32_t record_length = 0;
    uint32_t rolling = 0;
    bool partial = false;
    bool in_batch = false;
    bool tail = false;
    JournalRecords records;
    std::vector<std::pair<uint64_t, uint64_t>> ranges;
    if (format_ == 1) {
      ranges.emplace_back(begin, capacity_);
    } else {
      for (auto sequence : segments_) {
        if (sequence >= begin / options_.segment_bytes_) {
          ranges.emplace_back(std::max(begin, sequence * options_.segment_bytes_ + options_.unit_bytes_),
                              (sequence + 1) * options_.segment_bytes_);
        }
      }
    }
    for (const auto &range : ranges) {
      for (uint64_t position = range.first; position < range.second; position += options_.unit_bytes_) {
        const auto unit = ReadUnit(Physical(position));
        if (AllZero(unit.data(), unit.size())) {
          Require(!in_batch, "Journal has an incomplete batch before empty tail");
          tail = true;
          continue;
        }
        CheckUnitChecksum(unit);
        // A valid unit from a retired use can remain beyond this use's written
        // tail. It may never fill a required fragment or precede newer valid data.
        if (format_ == 2) {
          ByteReader stale(unit);
          const auto magic = stale.ReadBytes(8);
          const auto version = stale.ReadU32();
          stale.Skip(4);
          const auto identity = stale.ReadBytes(16);
          const auto sequence = stale.ReadU64();
          const auto offset = stale.ReadU64();
          if (std::memcmp(magic.data(), UNIT_MAGIC, 8) == 0 && version == 2 &&
              std::memcmp(identity.data(), identity_.data(), 16) == 0 && sequence != 0 &&
              sequence < retain_from_ / options_.segment_bytes_ &&
              Slot(sequence) == Slot(position / options_.segment_bytes_) &&
              offset == position % options_.segment_bytes_) {
            Require(!in_batch, "Journal old tail interrupts a live batch");
            tail = true;
            continue;
          }
        }
        Require(!tail, "Journal nonzero data follows an empty hole");
        ByteReader header(unit.data(), UNIT_HEADER);
        Require(header.ReadBytes(8) == std::vector<std::byte>(reinterpret_cast<const std::byte *>(UNIT_MAGIC),
                                                              reinterpret_cast<const std::byte *>(UNIT_MAGIC) + 8),
                "Journal unit magic mismatch");
        Require(header.ReadU32() == format_, "Journal unit version mismatch");
        const auto used = header.ReadU32();
        Require(used >= FRAGMENT_HEADER && used <= unit.size() - UNIT_HEADER - sizeof(uint32_t),
                "Journal invalid payload size");
        const auto identity = header.ReadBytes(identity_.size());
        Require(std::memcmp(identity.data(), identity_.data(), identity_.size()) == 0,
                "Journal unit identity mismatch");
        Require(header.ReadU64() == position / options_.segment_bytes_ &&
                    header.ReadU64() == position % options_.segment_bytes_,
                "Journal unit has wrong segment identity or position");
        if (!in_batch) {
          begin = position;
          in_batch = true;
        }
        const auto batch_begin = header.ReadU64();
        const auto prior = header.ReadU64();
        if (position == start && anchored) {
          Require(prior < start, "Journal checkpoint predecessor is invalid");
          previous = prior;
        }
        Require(batch_begin == begin && prior == previous, "Journal batch chain mismatch");
        Require(AllZero(unit.data() + UNIT_HEADER + used, unit.size() - UNIT_HEADER - used - sizeof(uint32_t)),
                "Journal nonzero unit padding");
        ByteReader payload(unit.data() + UNIT_HEADER, used);
        while (!payload.Empty()) {
          Require(payload.Remaining() >= FRAGMENT_HEADER, "Journal truncated fragment header");
          const auto start = payload.Offset();
          const auto type = payload.ReadU32();
          const auto length = payload.ReadU32();
          const auto ordinal = payload.ReadU32();
          const auto full_length = payload.ReadU32();
          Require(length <= payload.Remaining(), "Journal truncated fragment");
          if (type == COMMIT) {
            Require(!partial && !records.empty() && length == COMMIT_BYTES && ordinal == records.size() &&
                        full_length == 0 && payload.Remaining() == COMMIT_BYTES,
                    "Journal invalid batch commit boundary");
            Require(payload.ReadU64() == begin && payload.ReadU64() == total && payload.ReadU32() == rolling &&
                        payload.ReadU32() == 0,
                    "Journal batch commit checksum or identity mismatch");
            cursor = position + options_.unit_bytes_;
            if (replay) {
              replay(begin, records);
            }
            previous = begin;
            records.clear();
            total = 0;
            rolling = 0;
            in_batch = false;
            continue;
          }
          Require(length != 0 && full_length != 0 && full_length <= options_.max_record_bytes_ &&
                      length <= options_.max_batch_bytes_ - total,
                  "Journal record exceeds persisted limits");
          if (type == FULL || type == FIRST) {
            Require(!partial && ordinal == records.size() && records.size() < options_.max_records_per_batch_,
                    "Journal record start order mismatch");
            records.emplace_back();
            record_length = full_length;
          } else {
            Require((type == MIDDLE || type == LAST) && partial && ordinal == records.size() - 1 &&
                        full_length == record_length,
                    "Journal continuation missing or out of order");
          }
          Require(length <= record_length - records.back().size(), "Journal fragment exceeds record size");
          const auto bytes = payload.ReadBytes(length);
          records.back().insert(records.back().end(), bytes.begin(), bytes.end());
          total += length;
          rolling = Crc32cExtend(rolling, unit.data() + UNIT_HEADER + start, FRAGMENT_HEADER + length);
          partial = type == FIRST || type == MIDDLE;
          Require(partial ? records.back().size() < record_length : records.back().size() == record_length,
                  "Journal record length or final fragment mismatch");
        }
      }
    }
    Require(!in_batch, "Journal ended before batch commit");
    return {cursor, previous};
  }

  void Open(uint64_t begin, const std::function<void(uint64_t, const JournalRecords &)> &inspect,
            const std::function<void(uint64_t, const JournalRecords &)> &replay) {
    const bool anchored = begin != 0;
    BeginLifecycle();
    const auto control = ReadUnit(0);
    CheckUnitChecksum(control);
    ByteReader r(control);
    const auto magic = r.ReadBytes(8);
    format_ = r.ReadU32();
    if (std::memcmp(magic.data(), FORMAT_MAGIC, 8) != 0 || (format_ != 1 && format_ != 2)) {
      throw JournalError(JournalErrorCode::InvalidFormat, "unsupported Journal format");
    }
    if (format_ == 2) {
      Require(backend_.SegmentCapacity() >= 3 && options_.segment_bytes_ / options_.unit_bytes_ >= 2,
              "Journal ring geometry is invalid");
      r.Skip(68);
      reserved_sequence_ = r.ReadU64();
      r.Skip(16);
      retain_from_ = r.ReadU64();
      Require(reserved_sequence_ != 0 && reserved_sequence_ % (backend_.SegmentCapacity() - 1) == 0 &&
                  reserved_sequence_ < std::numeric_limits<uint64_t>::max() / options_.segment_bytes_ &&
                  retain_from_ / options_.segment_bytes_ != 0 &&
                  retain_from_ / options_.segment_bytes_ <= reserved_sequence_ &&
                  retain_from_ % options_.segment_bytes_ >= options_.unit_bytes_ &&
                  retain_from_ % options_.unit_bytes_ == 0,
              "Journal retirement control is invalid");
    }
    const auto expected = Control();
    if (std::memcmp(control.data() + 16, expected.data() + 16, 48) != 0) {
      throw JournalError(JournalErrorCode::IdentityMismatch, "Journal storage or format identity mismatch");
    }
    if (control != expected) {
      throw JournalError(JournalErrorCode::InvalidFormat, "Journal format, geometry or recovery limits mismatch");
    }
    for (uint64_t position = options_.unit_bytes_; position < options_.segment_bytes_;
         position += options_.unit_bytes_) {
      const auto unit = ReadUnit(position);
      Require(AllZero(unit.data(), unit.size()), "Journal reserved format area is nonzero");
    }
    if (format_ == 1) {
      begin = begin == 0 ? options_.segment_bytes_ : begin;
      Require(begin >= options_.segment_bytes_ && begin < capacity_ && begin % options_.unit_bytes_ == 0,
              "invalid legacy Journal recovery start");
    } else {
      begin = begin == 0 ? options_.segment_bytes_ + options_.unit_bytes_ : begin;
      Require(begin >= retain_from_ && begin % options_.unit_bytes_ == 0 &&
                  begin % options_.segment_bytes_ >= options_.unit_bytes_,
              "Journal recovery entry has retired");
      LoadSegments();
      if (!anchored && !segments_.empty()) {
        begin = segments_.front() * options_.segment_bytes_ + options_.unit_bytes_;
      }
      Require((segments_.empty() && begin == retain_from_ &&
               retain_from_ == options_.segment_bytes_ + options_.unit_bytes_) ||
                  std::binary_search(segments_.begin(), segments_.end(), begin / options_.segment_bytes_),
              "Journal recovery entry segment is missing");
    }
    const auto recovered = Scan(begin, inspect, anchored);
    Flush();
    if (replay) {
      const auto replayed = Scan(begin, replay, anchored);
      Require(replayed == recovered, "Journal changed during replay");
    }
    auto cursor = recovered.first;
    if (format_ == 2) {
      if (segments_.empty()) {
        cursor = 0;
        next_sequence_ = reserved_sequence_ + 1;
      } else {
        const auto last = segments_.back();
        cursor = std::max(cursor, last * options_.segment_bytes_ + options_.unit_bytes_);
        next_sequence_ = reserved_sequence_ + 1 + last % (backend_.SegmentCapacity() - 1);
      }
      ReserveSequences();
    }
    Start(cursor, recovered.second);
  }

  void Encode(const JournalRecords &records, const std::vector<UnitPlan> &units, const AppendPlan &plan, IOBatch &batch,
              uint64_t begin, uint64_t previous) {
    uint32_t rolling = 0;
    uint64_t total = 0;
    for (const auto &record : records) {
      total += record.size();
    }
    size_t member = 0;
    for (size_t index = 0; index < units.size(); ++index) {
      const auto position = plan.positions_[index];
      ByteWriter writer;
      writer.PutBytes(UNIT_MAGIC, 8);
      writer.PutU32(format_);
      writer.PutU32(static_cast<uint32_t>(units[index].used_));
      writer.PutBytes(identity_.data(), identity_.size());
      writer.PutU64(position / options_.segment_bytes_);
      writer.PutU64(position % options_.segment_bytes_);
      writer.PutU64(begin);
      writer.PutU64(previous);
      for (const auto &fragment : units[index].fragments_) {
        const auto start = writer.Data().size();
        writer.PutU32(fragment.type_);
        writer.PutU32(fragment.size_);
        writer.PutU32(fragment.record_);
        writer.PutU32(fragment.type_ == COMMIT ? 0 : static_cast<uint32_t>(records[fragment.record_].size()));
        if (fragment.type_ == COMMIT) {
          writer.PutU64(begin);
          writer.PutU64(total);
          writer.PutU32(rolling);
          writer.PutU32(0);
        } else {
          writer.PutBytes(records[fragment.record_].data() + fragment.offset_, fragment.size_);
          rolling = Crc32cExtend(rolling, writer.Data().data() + start, FRAGMENT_HEADER + fragment.size_);
        }
      }
      AddChecksumAndPadding(&writer, options_.unit_bytes_);
      const auto physical = Physical(position);
      while (plan.requests_[member].segment_slot_ != physical / options_.segment_bytes_) {
        ++member;
      }
      const auto within = physical % options_.segment_bytes_ - plan.requests_[member].offset_;
      std::memcpy(batch.Buffer(member) + within, writer.Data().data(), options_.unit_bytes_);
    }
  }

  auto Append(const JournalRecords &records, bool maintenance)
      -> std::pair<JournalAdmission, std::shared_ptr<JournalTicketData>> {
    // Serializes cursor selection and preparation, never device IO. Completion
    // and Close use the short queue mutex instead of waiting on encoding.
    std::lock_guard<std::mutex> admission(admission_mutex_);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (faulted_) {
        return {JournalAdmission::Faulted, {}};
      }
      if (!ready_ || stopping_) {
        return {JournalAdmission::Stopped, {}};
      }
    }
    const auto units = Plan(records, options_);
    if (maintenance && units.size() != 1) {
      throw std::invalid_argument("checkpoint must fit its reserved Journal unit");
    }
    if ((format_ == 1 && units.size() * options_.unit_bytes_ > capacity_ - cursor_) ||
        (format_ == 2 && units.size() + (maintenance ? 0 : 1) > AvailableUnits())) {
      return {JournalAdmission::NoSpace, {}};
    }
    const auto plan = Layout(units.size());
    const auto bytes = plan.bytes_;
    if (bytes > options_.max_group_bytes_) {
      throw std::invalid_argument("Journal batch including segment headers exceeds group budget");
    }
    {
      std::lock_guard<std::mutex> lock(budget_->mutex_);
      if (budget_->tickets_ == options_.max_pending_batches_ || bytes > options_.max_pending_bytes_ - budget_->bytes_) {
        return {JournalAdmission::Full, {}};
      }
    }
    auto data = backend_.TryPrepare(plan.requests_, false);
    if (data.admission_ != IOAdmission::Accepted) {
      return {data.admission_ == IOAdmission::Full ? JournalAdmission::Full : JournalAdmission::Stopped, {}};
    }
    auto flush = executor_.TryPrepareFlush();
    if (flush.admission_ != IOAdmission::Accepted) {
      return {flush.admission_ == IOAdmission::Full ? JournalAdmission::Full : JournalAdmission::Stopped, {}};
    }
    for (auto sequence : plan.new_segments_) {
      const auto header = SegmentHeader(sequence);
      const auto member = std::find_if(plan.requests_.begin(), plan.requests_.end(), [&](const JournalIORequest &r) {
        return r.segment_slot_ == Slot(sequence) && r.offset_ == 0;
      });
      std::memcpy(data.batch_->Buffer(member - plan.requests_.begin()), header.data(), header.size());
    }
    const auto begin = plan.positions_.front();
    Encode(records, units, plan, *data.batch_, begin, previous_);
    auto ticket = std::make_shared<JournalTicketData>(budget_, begin, bytes);
    ticket->result_.end_ = plan.positions_.back() + options_.unit_bytes_;
    Request request{ticket, std::move(data.batch_), std::move(flush.batch_), bytes};
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (faulted_) {
        return {JournalAdmission::Faulted, {}};
      }
      if (stopping_) {
        return {JournalAdmission::Stopped, {}};
      }
      queue_.push_back(std::move(request));  // Allocate before publishing any reservation.
      {
        std::lock_guard<std::mutex> budget_lock(budget_->mutex_);
        ++budget_->tickets_;
        budget_->bytes_ += bytes;
        ticket->reserved_ = true;
      }
      previous_ = begin;
      cursor_ = plan.cursor_;
      next_sequence_ = plan.next_sequence_;
      for (auto sequence : plan.new_segments_) {
        slot_sequences_[Slot(sequence)] = sequence;
      }
    }
    work_.notify_one();
    return {JournalAdmission::Accepted, std::move(ticket)};
  }

  void ObserveFailure(const std::exception_ptr &error) {
    // Admission closes as soon as this coordinator observes a failure. Result
    // publication still waits for every already submitted group member to drain.
    std::lock_guard<std::mutex> lock(mutex_);
    faulted_ = true;
    failure_ = error;
  }

  void Run() {
    std::unique_lock<std::mutex> lock(mutex_);
    for (;;) {
      work_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
      if (queue_.empty()) {
        return;
      }
      group_.clear();
      uint64_t bytes = 0;
      while (!queue_.empty() && group_.size() < options_.max_group_batches_ &&
             queue_.front().bytes_ <= options_.max_group_bytes_ - bytes) {
        bytes += queue_.front().bytes_;
        group_.push_back(std::move(queue_.front()));
        queue_.pop_front();
      }
      active_ = true;
      auto error = failure_;
      lock.unlock();
      if (!error) {
        // Submit all group members first: independent segment IO can overlap.
        for (auto &request : group_) {
          try {
            CheckAdmission(executor_.TrySubmit(*request.data_));
            request.submitted_ = true;
          } catch (...) {
            error = std::current_exception();
            ObserveFailure(error);
            break;
          }
        }
      }
      for (auto &request : group_) {
        if (request.submitted_) {
          request.data_->Wait();
          try {
            CheckIO(*request.data_);
          } catch (...) {
            if (!error) {
              error = std::current_exception();
              ObserveFailure(error);
            }
          }
        }
      }
      if (!error) {
        try {
          SubmitAndWait(*group_.front().flush_);
        } catch (...) {
          error = std::current_exception();
          ObserveFailure(error);
        }
      }
      // Every submitted IO has drained; buffers and terminal results can now
      // be released. Pending groups inherit failure_ without sending their IO.
      for (auto &request : group_) {
        request.data_.reset();
        request.flush_.reset();
        const auto outcome = !error
                                 ? JournalOutcome::Durable
                                 : (request.submitted_ ? JournalOutcome::Indeterminate : JournalOutcome::NotCommitted);
        request.ticket_->Complete(outcome, error);
      }
      group_.clear();
      lock.lock();
      active_ = false;
      idle_.notify_all();
    }
  }

  void RetireBefore(uint64_t checkpoint) {
    std::lock_guard<std::mutex> admission(admission_mutex_);
    std::unique_lock<std::mutex> lock(mutex_);
    if (format_ == 1) {
      return;
    }
    idle_.wait(lock, [&] { return !active_ && queue_.empty(); });
    if (faulted_ || stopping_ || !ready_) {
      throw JournalError(JournalErrorCode::ExecutorStopped, "Journal retirement requires a healthy writer");
    }
    Require(checkpoint <= previous_ && checkpoint >= retain_from_, "invalid checkpoint retirement boundary");
    if (checkpoint == retain_from_) {
      return;
    }
    lock.unlock();
    // B has published this checkpoint before calling us. No new append can
    // acquire the gate until the retirement rule and identity reservation persist.
    retain_from_ = checkpoint;
    try {
      ReserveSequences();
    } catch (...) {
      ObserveFailure(std::current_exception());
      throw;
    }
  }

  void Close() {
    std::call_once(close_, [&] {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
      }
      work_.notify_all();
      if (worker_.joinable()) {
        worker_.join();
      }
      // Wait out a concurrent unsubmitted preparation before releasing backends.
      std::lock_guard<std::mutex> admission(admission_mutex_);
      closed_ = true;
    });
  }

  IOExecutor &executor_;
  RegionManager regions_;
  JournalBackend backend_;
  BootstrapIdentity storage_;
  JournalIdentity identity_;
  JournalOptions options_;
  uint64_t capacity_;
  std::shared_ptr<TicketBudget> budget_;
  std::mutex admission_mutex_;
  std::mutex mutex_;
  std::condition_variable work_;
  std::condition_variable idle_;
  std::list<Request> queue_;
  std::vector<Request> group_;
  std::thread worker_;
  std::once_flag close_;
  uint32_t format_{1};
  uint64_t reserved_sequence_{0};
  uint64_t retain_from_{0};
  uint64_t next_sequence_{1};
  std::vector<uint64_t> slot_sequences_;
  std::vector<uint64_t> segments_;
  bool active_{false};
  uint64_t cursor_{0};
  uint64_t previous_{0};
  bool attempted_{false};
  bool ready_{false};
  bool stopping_{false};
  bool faulted_{false};
  bool closed_{false};
  std::exception_ptr failure_;
};

JournalService::JournalService(BootstrapStore &bootstrap, const JournalIdentity &identity,
                               const JournalOptions &options) {
  const auto binding = bootstrap.BindRegions();
  impl_ = std::make_unique<Impl>(bootstrap, *binding.executor_, binding.identity_, identity, options);
}
JournalService::~JournalService() { Close(); }
void JournalService::Create() { impl_->Create(); }
void JournalService::Open(const std::function<void(uint64_t, const JournalRecords &)> &replay) {
  impl_->Open(0, {}, replay);
}
void JournalService::OpenFrom(uint64_t begin, const std::function<void(uint64_t, const JournalRecords &)> &inspect,
                              const std::function<void(uint64_t, const JournalRecords &)> &replay) {
  if (begin == 0) {
    throw JournalError(JournalErrorCode::InvalidFormat, "checkpoint position must be nonzero");
  }
  impl_->Open(begin, inspect, replay);
}
auto JournalService::TryAppend(const JournalRecords &records) -> JournalSubmission {
  auto [admission, data] = impl_->Append(records, false);
  if (!data) {
    return {admission, std::nullopt};
  }
  return {admission, JournalTicket(std::move(data))};
}
auto JournalService::AppendCheckpoint(const JournalRecords &records) -> JournalSubmission {
  auto [admission, data] = impl_->Append(records, true);
  if (!data) {
    return {admission, std::nullopt};
  }
  return {admission, JournalTicket(std::move(data))};
}
auto JournalService::CheckpointRef(uint64_t lsn) const -> MetadataCheckpointRef {
  return {impl_->identity_, lsn, impl_->format_ == 2};
}
void JournalService::RetireBefore(uint64_t checkpoint_lsn) { impl_->RetireBefore(checkpoint_lsn); }
void JournalService::Close() { impl_->Close(); }

}  // namespace bustub
