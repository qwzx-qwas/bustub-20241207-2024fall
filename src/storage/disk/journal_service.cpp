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
    writer.PutU32(1);
    writer.PutU32(options_.unit_bytes_);
    writer.PutBytes(storage_.storage_.data(), storage_.storage_.size());
    writer.PutBytes(storage_.device_.data(), storage_.device_.size());
    writer.PutBytes(identity_.data(), identity_.size());
    writer.PutU64(options_.segment_bytes_);
    writer.PutU64(backend_.SegmentCapacity());
    writer.PutU64(backend_.SegmentCapacity() - 1);
    writer.PutU32(options_.max_record_bytes_);
    writer.PutU64(options_.max_batch_bytes_);
    writer.PutU32(options_.max_records_per_batch_);
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
    const auto control = Control();
    auto batch = PrepareUnit(0, IOOperation::Write, true);
    std::memcpy(batch.Buffer(0), control.data(), control.size());
    SubmitAndWait(batch);
    Start(options_.segment_bytes_, 0);
  }

  // Two passes: all structural checks precede replay side effects. The owner
  // excludes external writes to this region throughout Open and normal use.
  auto Scan(const std::function<void(uint64_t, const JournalRecords &)> &replay) -> std::pair<uint64_t, uint64_t> {
    uint64_t previous = 0;
    uint64_t begin = options_.segment_bytes_;
    uint64_t cursor = begin;
    uint64_t total = 0;
    uint32_t record_length = 0;
    uint32_t rolling = 0;
    bool partial = false;
    bool in_batch = false;
    bool tail = false;
    JournalRecords records;
    for (uint64_t position = begin; position < capacity_; position += options_.unit_bytes_) {
      const auto unit = ReadUnit(position);
      if (AllZero(unit.data(), unit.size())) {
        Require(!in_batch, "Journal has an incomplete batch before empty tail");
        tail = true;
        continue;
      }
      Require(!tail, "Journal nonzero data follows an empty hole");
      CheckUnitChecksum(unit);
      ByteReader header(unit.data(), UNIT_HEADER);
      Require(header.ReadBytes(8) == std::vector<std::byte>(reinterpret_cast<const std::byte *>(UNIT_MAGIC),
                                                            reinterpret_cast<const std::byte *>(UNIT_MAGIC) + 8),
              "Journal unit magic mismatch");
      Require(header.ReadU32() == 1, "Journal unit version mismatch");
      const auto used = header.ReadU32();
      Require(used >= FRAGMENT_HEADER && used <= unit.size() - UNIT_HEADER - sizeof(uint32_t),
              "Journal invalid payload size");
      const auto identity = header.ReadBytes(identity_.size());
      Require(std::memcmp(identity.data(), identity_.data(), identity_.size()) == 0, "Journal unit identity mismatch");
      Require(header.ReadU64() == position / options_.segment_bytes_ &&
                  header.ReadU64() == position % options_.segment_bytes_,
              "Journal unit has wrong segment identity or position");
      if (!in_batch) {
        begin = position;
        in_batch = true;
      }
      Require(header.ReadU64() == begin && header.ReadU64() == previous, "Journal batch chain mismatch");
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
    Require(!in_batch, "Journal ended before batch commit");
    return {cursor, previous};
  }

  void Open(const std::function<void(uint64_t, const JournalRecords &)> &replay) {
    BeginLifecycle();
    const auto control = ReadUnit(0);
    CheckUnitChecksum(control);
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
    const auto recovered = Scan({});
    Flush();
    if (replay) {
      const auto replayed = Scan(replay);
      Require(replayed == recovered, "Journal changed during replay");
    }
    Start(recovered.first, recovered.second);
  }

  void Encode(const JournalRecords &records, const std::vector<UnitPlan> &units,
              const std::vector<JournalIORequest> &requests, IOBatch &batch, uint64_t begin, uint64_t previous) {
    uint32_t rolling = 0;
    uint64_t total = 0;
    for (const auto &record : records) {
      total += record.size();
    }
    size_t member = 0;
    uint64_t within = 0;
    for (size_t index = 0; index < units.size(); ++index) {
      const auto position = begin + index * options_.unit_bytes_;
      ByteWriter writer;
      writer.PutBytes(UNIT_MAGIC, 8);
      writer.PutU32(1);
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
      std::memcpy(batch.Buffer(member) + within, writer.Data().data(), options_.unit_bytes_);
      within += options_.unit_bytes_;
      if (within == requests[member].size_) {
        ++member;
        within = 0;
      }
    }
  }

  auto Append(const JournalRecords &records) -> std::pair<JournalAdmission, std::shared_ptr<JournalTicketData>> {
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
    const uint64_t bytes = units.size() * static_cast<uint64_t>(options_.unit_bytes_);
    if (bytes > capacity_ - cursor_) {
      return {JournalAdmission::NoSpace, {}};
    }
    {
      std::lock_guard<std::mutex> lock(budget_->mutex_);
      if (budget_->tickets_ == options_.max_pending_batches_ || bytes > options_.max_pending_bytes_ - budget_->bytes_) {
        return {JournalAdmission::Full, {}};
      }
    }
    const auto requests = backend_.PlanAppend(cursor_, bytes);
    auto data = backend_.TryPrepare(requests, false);
    if (data.admission_ != IOAdmission::Accepted) {
      return {data.admission_ == IOAdmission::Full ? JournalAdmission::Full : JournalAdmission::Stopped, {}};
    }
    auto flush = executor_.TryPrepareFlush();
    if (flush.admission_ != IOAdmission::Accepted) {
      return {flush.admission_ == IOAdmission::Full ? JournalAdmission::Full : JournalAdmission::Stopped, {}};
    }
    Encode(records, units, requests, *data.batch_, cursor_, previous_);
    auto ticket = std::make_shared<JournalTicketData>(budget_, cursor_, bytes);
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
      previous_ = cursor_;
      cursor_ += bytes;
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
  std::list<Request> queue_;
  std::vector<Request> group_;
  std::thread worker_;
  std::once_flag close_;
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
void JournalService::Open(const std::function<void(uint64_t, const JournalRecords &)> &replay) { impl_->Open(replay); }
auto JournalService::TryAppend(const JournalRecords &records) -> JournalSubmission {
  auto [admission, data] = impl_->Append(records);
  if (!data) {
    return {admission, std::nullopt};
  }
  return {admission, JournalTicket(std::move(data))};
}
void JournalService::Close() { impl_->Close(); }

}  // namespace bustub
