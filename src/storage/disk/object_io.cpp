//===----------------------------------------------------------------------===//
// BusTub: ordinary object range IO on the existing bounded batch executor.
//===----------------------------------------------------------------------===//
#include "storage/disk/object_io.h"

#include <algorithm>
#include <array>
#include <condition_variable>  // NOLINT(build/c++11)
#include <cstring>
#include <mutex>  // NOLINT(build/c++11)
#include <sstream>
#include <utility>
#include "common/byte_codec.h"

#include "object_change_internal.h"  // NOLINT(build/include_subdir): private sibling component.
#include "object_io_internal.h"      // NOLINT(build/include_subdir): private sibling component.

namespace bustub {
struct ObjectIOState {
  std::mutex mutex_;
  std::condition_variable idle_;
  size_t active_{0};
  bool closed_{false};
  std::exception_ptr error_;
};
namespace {
struct Activity {
  explicit Activity(std::shared_ptr<ObjectIOState> state) : state_(std::move(state)) {
    std::lock_guard<std::mutex> lock(state_->mutex_);
    if (state_->closed_ || state_->error_) {
      throw MetadataError(MetadataErrorCode::NotReady, "object IO is closed or faulted");
    }
    ++state_->active_;
  }
  ~Activity() {
    read_.reset();
    write_.reset();
    std::lock_guard<std::mutex> lock(state_->mutex_);
    --state_->active_;
    state_->idle_.notify_all();
  }
  void Complete(const IOBatchResult &result) {
    auto error = result.flush_error_;
    for (const auto &member : result.operations_) {
      if (!error && member.error_) {
        error = member.error_;
      }
    }
    if (error) {
      std::lock_guard<std::mutex> lock(state_->mutex_);
      if (!state_->error_) {
        state_->error_ = error;
      }
    }
  }
  std::shared_ptr<ObjectIOState> state_;
  std::optional<ObjectReadLease> read_;
  std::optional<DataAllocationLease> write_;
};
void Accepted(IOAdmission admission) {
  if (admission == IOAdmission::Full) {
    throw ObjectIOBusy();
  }
  if (admission != IOAdmission::Accepted) {
    throw MetadataError(MetadataErrorCode::NotReady, "object IO admission rejected");
  }
}
void Accepted(const IOPreparation &prepared) {
  if (prepared.shared_memory_limited_) {
    throw MetadataCommitBusy("object IO shared memory is held");
  }
  Accepted(prepared.admission_);
}
void Successful(const IOBatchResult &result) {
  for (const auto &member : result.operations_) {
    if (member.error_) {
      std::rethrow_exception(member.error_);
    }
    if (member.outcome_ != IOOutcome::Succeeded) {
      throw std::runtime_error("object IO member did not complete");
    }
  }
  if (result.flush_error_) {
    std::rethrow_exception(result.flush_error_);
  }
}
auto UnitChecksums(const void *source, size_t size, uint64_t unit) -> std::vector<uint32_t> {
  const auto *p = static_cast<const std::byte *>(source);
  static constexpr std::array<std::byte, 256> zeros{};
  std::vector<uint32_t> result;
  result.reserve((size + unit - 1) / unit);
  for (size_t pos = 0; pos < size; pos += unit) {
    auto length = std::min<uint64_t>(unit, size - pos);
    auto crc = Crc32c(p + pos, length);
    for (auto tail = unit - length; tail;) {
      const auto take = std::min<uint64_t>(tail, zeros.size());
      crc = Crc32cExtend(crc, zeros.data(), take);
      tail -= take;
    }
    result.push_back(crc);
  }
  return result;
}
struct ReadValidation {
  struct Unit {
    ObjectUnitChecksum expected_;
    const std::byte *bytes_;
  };
  ResourceCharge memory_;
  std::vector<Unit> units_;
  void Reserve(std::shared_ptr<ResourceAccount> account, size_t count) {
    const auto bytes = count * (sizeof(Unit) + sizeof(ObjectUnitChecksum));
    const auto progress = ProgressWork::Active();
    if (!account->Reserve(bytes, progress)) throw MetadataCommitBusy("integrity descriptor credits held");
    memory_ = ResourceCharge(std::move(account), bytes, progress);
    units_.reserve(count);
  }
  uint64_t unit_;
  std::shared_ptr<ObjectIOState> state_;
  std::exception_ptr error_;
  void Add(const std::vector<ObjectUnitChecksum> &checksums, const void *buffer) {
    const auto *p = static_cast<const std::byte *>(buffer);
    for (const auto &crc : checksums) {
      units_.push_back({crc, p});
      p += unit_;
    }
  }
  // F02 has drained all writes into these bytes. The completion owner keeps
  // the original buffer/content permission alive until verification finishes.
  void Complete(const IOBatchResult &result) noexcept {
    try {
      Successful(result);
      for (const auto &u : units_) {
        const auto actual = Crc32c(u.bytes_, unit_);
        const auto &e = u.expected_;
        if (actual != e.crc_) {
          std::ostringstream message;
          message << "Data checksum mismatch: owner=" << e.owner_.space_ << '/' << e.owner_.number_
                  << " allocation=" << e.allocation_ << " offset=" << e.offset_ << " length=" << unit_
                  << " expected=" << e.crc_ << " actual=" << actual;
          throw MetadataError(MetadataErrorCode::Corrupt, message.str());
        }
      }
    } catch (...) {
      error_ = std::current_exception();
      std::lock_guard lock(state_->mutex_);
      if (!state_->error_) state_->error_ = error_;
    }
  }
  void Check() const {
    if (error_) std::rethrow_exception(error_);
  }
};
struct Piece {
  size_t size_;
  size_t skip_;
  std::optional<size_t> member_;
  std::optional<size_t> journal_{std::nullopt};
};
}  // namespace
struct ObjectReadData {
  size_t size_{0};
  std::shared_ptr<ReadValidation> validation_;
  std::vector<Piece> pieces_;
  std::vector<JournalPayloadRead> journals_;
  std::optional<IOBatch> batch_;
  std::optional<ObjectReadTarget> target_;
  bool external_{false};
  bool finished_{false};
};
struct ObjectWriteData {
  std::shared_ptr<ObjectIOState> owner_;
  DataReservation reservation_;
  IOBatch batch_;
  size_t size_;
  bool published_{false};
  ResourceCharge checksum_memory_;
  std::vector<uint32_t> checksums_;
};
ObjectRead::ObjectRead(std::unique_ptr<ObjectReadData> data) : data_(std::move(data)) {}
ObjectRead::~ObjectRead() = default;
ObjectRead::ObjectRead(ObjectRead &&other) noexcept = default;
auto ObjectRead::operator=(ObjectRead &&other) noexcept -> ObjectRead & = default;
auto ObjectRead::Size() const -> size_t { return data_->size_; }
void ObjectRead::Wait() const {
  for (const auto &read : data_->journals_) read.Wait();
  if (data_->batch_) {
    data_->batch_->Wait();
  }
}
auto ObjectRead::WaitFor(std::chrono::milliseconds timeout) const -> bool {
  const auto end = std::chrono::steady_clock::now() + timeout;
  for (const auto &read : data_->journals_) {
    if (!read.WaitFor(std::max(std::chrono::milliseconds(0), std::chrono::duration_cast<std::chrono::milliseconds>(
                                                                 end - std::chrono::steady_clock::now()))))
      return false;
  }
  return !data_->batch_ ||
         data_->batch_->WaitFor(
             std::max(std::chrono::milliseconds(0),
                      std::chrono::duration_cast<std::chrono::milliseconds>(end - std::chrono::steady_clock::now())));
}
void ObjectRead::Finish() const {
  if (!data_->target_) {
    throw std::logic_error("Finish requires a read target");
  }
  if (data_->finished_) {
    return;
  }
  Wait();
  if (data_->external_) {
    if (data_->batch_) {
      Successful(data_->batch_->Result());
      data_->validation_->Check();
    }
  } else {
    CopyTo(data_->target_->data_, data_->target_->capacity_);
  }
  data_->finished_ = true;
  data_->target_->owner_.reset();
}
void ObjectRead::CopyTo(void *destination, size_t capacity) const {
  if (data_->external_) {
    throw std::logic_error("external read uses Finish");
  }
  if (capacity < Size() || (Size() != 0 && destination == nullptr)) {
    throw std::invalid_argument("object read destination is too small");
  }
  if (data_->batch_) {
    Successful(data_->batch_->Result());
    data_->validation_->Check();
  }
  std::vector<std::vector<std::byte>> journal_bytes;
  for (const auto &read : data_->journals_) {
    try {
      journal_bytes.push_back(DecodeMetadataPayload(read));
    } catch (...) {
      std::lock_guard lock(data_->validation_->state_->mutex_);
      if (!data_->validation_->state_->error_) data_->validation_->state_->error_ = std::current_exception();
      throw;
    }
  }
  auto *out = static_cast<char *>(destination);
  for (const auto &piece : data_->pieces_) {
    if (piece.journal_) {
      const auto &body = journal_bytes[*piece.journal_];
      if (piece.skip_ > body.size() || piece.size_ > body.size() - piece.skip_)
        throw std::runtime_error("object payload slice exceeds record");
      std::memcpy(out, body.data() + piece.skip_, piece.size_);
    } else if (piece.member_) {
      std::memcpy(out, data_->batch_->Buffer(*piece.member_) + piece.skip_, piece.size_);
    } else {
      std::memset(out, 0, piece.size_);
    }
    out += piece.size_;
  }
}
ObjectWrite::ObjectWrite(std::unique_ptr<ObjectWriteData> data) : data_(std::move(data)) {}
ObjectWrite::~ObjectWrite() = default;
ObjectWrite::ObjectWrite(ObjectWrite &&other) noexcept = default;
auto ObjectWrite::operator=(ObjectWrite &&other) noexcept -> ObjectWrite & = default;
auto ObjectWrite::Size() const -> size_t { return data_->size_; }
void ObjectWrite::Wait() const { data_->batch_.Wait(); }
auto ObjectWrite::WaitFor(std::chrono::milliseconds timeout) const -> bool { return data_->batch_.WaitFor(timeout); }
auto ObjectWrite::Result() const -> const IOBatchResult & { return data_->batch_.Result(); }

ObjectIO::ObjectIO(RegionManager &regions, IOExecutor &executor, DataAllocator &allocator, ObjectMappingStore &mapping,
                   ObjectReferenceManager &references, uint64_t max_read_bytes, uint64_t max_write_bytes,
                   const IOExecutorOptions &io_limits, size_t external_bytes)
    : regions_(regions),
      executor_(executor),
      allocator_(allocator),
      mapping_(mapping),
      references_(references),
      region_(regions.Region(RegionKind::Data)),
      max_read_bytes_(max_read_bytes),
      max_write_bytes_(max_write_bytes),
      io_limits_(io_limits),
      external_bytes_(external_bytes),
      memory_(ResourceAccount::Create(io_limits.memory_budget_)),
      state_(std::make_shared<ObjectIOState>()) {
  if (max_read_bytes == 0 || max_write_bytes == 0) {
    throw std::invalid_argument("object IO requires positive request budgets");
  }
}
ObjectIO::~ObjectIO() { Close(); }
void ObjectIO::CheckBatchBudget(const std::vector<RegionIORequest> &requests) const {
  const auto operations = io_limits_.max_operations_ - (ProgressWork::Active() ? 0 : io_limits_.progress_operations_);
  if (requests.size() > operations) {
    throw MetadataError(MetadataErrorCode::ResourceUnavailable, "object batch exceeds executor member limit");
  }
  auto remaining = io_limits_.max_buffer_bytes_ - (ProgressWork::Active() ? 0 : io_limits_.progress_buffer_bytes_);
  const auto padding = executor_.DeviceInfo().memory_alignment_ - 1;
  for (const auto &request : requests) {
    if (request.size_ > remaining || padding > remaining - request.size_) {
      throw MetadataError(MetadataErrorCode::ResourceUnavailable, "object batch exceeds executor buffer limit");
    }
    remaining -= request.size_ + padding;
  }
}
auto ObjectIO::Read(const ObjectMappingSnapshot &view, ObjectKey key, uint64_t offset, uint64_t length) -> ObjectRead {
  return Read(view, key, offset, length, {});
}
auto ObjectIO::Read(const ObjectMappingSnapshot &view, ObjectKey key, uint64_t offset, uint64_t length,
                    std::function<void()> ready) -> ObjectRead {
  if (length > max_read_bytes_) {
    throw MetadataError(MetadataErrorCode::ResourceUnavailable, "object read exceeds request budget");
  }
  return ReadProtected(view, key, references_.ProtectRead(view, key, offset, length), std::move(ready));
}
auto ObjectIO::ReadProtected(const ObjectMappingSnapshot &view, ObjectKey key, ObjectReadLease lease,
                             std::function<void()> ready) -> ObjectRead {
  auto activity = std::make_shared<Activity>(state_);
  activity->read_.emplace(std::move(lease));
  auto data = std::make_unique<ObjectReadData>();
  data->validation_ = std::make_shared<ReadValidation>();
  data->validation_->unit_ = ObjectMappingAccess::Unit(mapping_);
  data->validation_->state_ = state_;
  size_t check_count = 0;
  const auto unit = data->validation_->unit_;
  for (const auto &span : activity->read_->Spans())
    if (span.data_ && !span.journal_) check_count += (span.data_->offset_ % unit + span.size_ + unit - 1) / unit;
  if (ObjectMappingAccess::Checksummed(mapping_)) data->validation_->Reserve(memory_, check_count);
  std::vector<std::vector<ObjectUnitChecksum>> checks;
  std::vector<RegionIORequest> requests;
  const uint64_t align = ObjectMappingAccess::Checksummed(mapping_) ? ObjectMappingAccess::Unit(mapping_)
                                                                    : regions_.Describe(region_).offset_alignment_;
  for (const auto &span : activity->read_->Spans()) {
    Piece piece{static_cast<size_t>(span.size_), 0, std::nullopt};
    if (span.journal_) {
      piece.skip_ = span.journal_->offset_;
    } else if (span.data_) {
      const auto start = span.data_->offset_ / align * align;
      const auto skip = span.data_->offset_ - start;
      const auto amount = (skip + span.size_ + align - 1) / align * align;
      piece.skip_ = skip;
      piece.member_ = requests.size();
      requests.push_back({region_, IOOperation::Read, start, amount});
      checks.push_back(ObjectMappingAccess::Checksums(view, span.data_->owner_.value_or(key), span.data_->allocation_,
                                                      start, amount));
    }
    data->pieces_.push_back(piece);
    data->size_ += piece.size_;
  }
  // Count all Data members before admitting Journal reads. Otherwise a logical
  // read whose combined results cannot fit could retry forever as "busy".
  IOReadBudget budget;
  for (const auto &request : requests) {
    if (!executor_.AccumulateReadBudget(&budget, 1, request.size_ + executor_.DeviceInfo().memory_alignment_ - 1)) {
      throw MetadataError(MetadataErrorCode::ResourceUnavailable, "object read exceeds total executor capacity");
    }
  }
  const auto &spans = activity->read_->Spans();
  for (size_t i = 0; i < spans.size(); ++i) {
    if (spans[i].journal_) {
      data->pieces_[i].journal_ = data->journals_.size();
      data->journals_.push_back(ObjectMappingAccess::ReadPayload(
          mapping_, spans[i].journal_->payload_, budget,
          [activity](const IOBatchResult &result) { activity->Complete(result); }, ready));
    }
  }
  if (!requests.empty()) {
    auto prepared = regions_.TryPrepare(requests, false);
    Accepted(prepared);
    data->batch_ = std::move(prepared.batch_);
    for (size_t i = 0; i < checks.size(); ++i) data->validation_->Add(checks[i], data->batch_->Buffer(i));
    data->batch_->RetainUntilComplete(
        [activity, validation = data->validation_](const IOBatchResult &result) {
          activity->Complete(result);
          validation->Complete(result);
        },
        std::move(ready));
    Accepted(executor_.TrySubmit(*data->batch_));
  }
  return ObjectRead(std::move(data));
}
void ObjectIO::CheckExternalBudget(const std::vector<RegionIORequest> &requests, size_t capacity) const {
  const auto operations = io_limits_.max_operations_ - (ProgressWork::Active() ? 0 : io_limits_.progress_operations_);
  if (requests.size() > operations || capacity > external_bytes_) {
    throw MetadataError(MetadataErrorCode::ResourceUnavailable, "external object IO exceeds configured capacity");
  }
}
auto ObjectIO::ReadInto(const ObjectMappingSnapshot &view, ObjectKey key, uint64_t offset, uint64_t length,
                        ObjectReadTarget target) -> ObjectRead {
  return std::move(*ReadIntoImpl(view, key, offset, length, std::move(target), {}));
}
auto ObjectIO::PrefetchInto(const ObjectMappingSnapshot &view, ObjectKey key, uint64_t offset, uint64_t length,
                            ObjectReadTarget target, std::function<void(std::exception_ptr)> complete) -> bool {
  if (!complete) {
    throw std::invalid_argument("prefetch requires completion owner");
  }
  return ReadIntoImpl(view, key, offset, length, std::move(target), std::move(complete)).has_value();
}
auto ObjectIO::ReadIntoImpl(const ObjectMappingSnapshot &view, ObjectKey key, uint64_t offset, uint64_t length,
                            ObjectReadTarget target, std::function<void(std::exception_ptr)> complete)
    -> std::optional<ObjectRead> {
  if (!target.owner_ || !target.data_ || length > target.capacity_ || length > max_read_bytes_) {
    throw std::invalid_argument("invalid object read target or request budget");
  }
  auto activity = std::make_shared<Activity>(state_);
  activity->read_.emplace(references_.ProtectRead(view, key, offset, length));
  std::vector<RegionIORequest> requests;
  std::vector<IOBufferLease> leases;
  const auto info = regions_.Describe(region_);
  const auto check_align =
      ObjectMappingAccess::Checksummed(mapping_) ? ObjectMappingAccess::Unit(mapping_) : info.offset_alignment_;
  auto validation = std::make_shared<ReadValidation>();
  validation->unit_ = ObjectMappingAccess::Unit(mapping_);
  validation->state_ = state_;
  if (ObjectMappingAccess::Checksummed(mapping_))
    validation->Reserve(memory_, (length + check_align - 1) / check_align + activity->read_->Spans().size());
  size_t cursor = 0;
  bool direct = true;
  const auto &spans = activity->read_->Spans();
  const bool holes = std::any_of(spans.begin(), spans.end(), [](const auto &s) { return !s.data_; });
  const bool payload = std::any_of(spans.begin(), spans.end(), [](const auto &s) { return s.data_.has_value(); });
  if (holes && payload) {
    direct = false;
  }
  for (const auto &span : spans) {
    auto *out = static_cast<char *>(target.data_) + cursor;
    if (span.journal_) {
      direct = false;
      break;
    }
    if (span.data_) {
      if (span.data_->offset_ % check_align || span.size_ % check_align ||
          reinterpret_cast<uintptr_t>(out) % info.memory_alignment_) {
        direct = false;
        break;
      }
      requests.push_back({region_, IOOperation::Read, span.data_->offset_, span.size_});
      validation->Add(ObjectMappingAccess::Checksums(view, span.data_->owner_.value_or(key), span.data_->allocation_,
                                                     span.data_->offset_, span.size_),
                      out);
      const auto capacity = &span == &spans.back() ? target.capacity_ - cursor : span.size_;
      leases.push_back(IOBufferLease::ForRead(out, capacity, [owner = target.owner_] {}));
    }
    cursor += span.size_;
  }
  if (!direct) {
    if (complete) {
      return std::nullopt;
    }
    // Physical edge alignment genuinely requires a working buffer. The caller
    // completes the copy before releasing its parent page content permission.
    validation.reset();
    activity.reset();
    auto read = Read(view, key, offset, length);
    read.data_->target_ = std::move(target);
    return read;
  }
  auto data = std::make_unique<ObjectReadData>();
  data->size_ = cursor;
  data->external_ = true;
  data->validation_ = validation;
  data->target_ = target;
  cursor = 0;
  for (const auto &span : activity->read_->Spans()) {
    if (!span.data_) {
      std::memset(static_cast<char *>(target.data_) + cursor, 0, span.size_);
    }
    cursor += span.size_;
  }
  if (!requests.empty()) {
    // Capacity budgets include the retained frame even when holes use no IO.
    CheckExternalBudget(requests, target.capacity_);
    if (requests.size() == 1 && cursor == length && requests[0].size_ == length) {
      leases.clear();
      leases.push_back(IOBufferLease::ForRead(target.data_, target.capacity_, [owner = target.owner_] {}));
    }
    auto prepared = complete ? regions_.TryPrepareReadAhead(requests, leases)
                             : regions_.TryPrepareExternal(requests, leases, false);
    if (complete && prepared.admission_ == IOAdmission::Full) {
      return std::nullopt;
    }
    Accepted(prepared);
    data->batch_ = std::move(prepared.batch_);
    data->batch_->RetainUntilComplete([activity, validation, owner = target.owner_, complete = std::move(complete),
                                       cursor, length](const IOBatchResult &r) {
      activity->Complete(r);
      validation->Complete(r);
      if (complete) {
        std::exception_ptr error;
        try {
          Successful(r);
          validation->Check();
          if (cursor != length) {
            throw std::runtime_error("short prefetched page");
          }
        } catch (...) {
          error = std::current_exception();
        }
        complete(error);
      }
    });
    Accepted(executor_.TrySubmit(*data->batch_));
  }
  if (requests.empty() && complete) {
    // No device members: an all-hole read completes during admission.
    std::exception_ptr error;
    if (cursor != length) {
      error = std::make_exception_ptr(std::runtime_error("short prefetched page"));
    }
    complete(error);
  }
  return ObjectRead(std::move(data));
}
auto ObjectIO::Write(const void *source, size_t size) -> ObjectWrite {
  if (source == nullptr || size == 0) {
    throw std::invalid_argument("object data write requires nonempty bytes");
  }
  if (size > max_write_bytes_) {
    throw MetadataError(MetadataErrorCode::ResourceUnavailable, "object write exceeds request budget");
  }
  auto activity = std::make_shared<Activity>(state_);
  auto reservation = allocator_.Reserve(size);
  activity->write_.emplace(reservation.Lease());
  std::vector<RegionIORequest> requests;
  for (const auto &extent : reservation.Extents()) {
    requests.push_back({region_, IOOperation::Write, extent.Offset(), extent.Size()});
  }
  CheckBatchBudget(requests);
  auto prepared = regions_.TryPrepare(requests, true);
  Accepted(prepared);
  auto data = std::make_unique<ObjectWriteData>(
      ObjectWriteData{state_, std::move(reservation), std::move(*prepared.batch_), size, false, {}, {}});
  if (ObjectMappingAccess::Checksummed(mapping_)) {
    const auto bytes =
        (size + ObjectMappingAccess::Unit(mapping_) - 1) / ObjectMappingAccess::Unit(mapping_) * sizeof(uint32_t);
    if (!memory_->Reserve(bytes, ProgressWork::Active())) throw MetadataCommitBusy("write checksum credits held");
    data->checksum_memory_ = ResourceCharge(memory_, bytes, ProgressWork::Active());
    data->checksums_ = UnitChecksums(source, size, ObjectMappingAccess::Unit(mapping_));
  }
  size_t cursor = 0;
  for (size_t i = 0; i < requests.size(); ++i) {
    auto *buffer = data->batch_.Buffer(i);
    const auto take = std::min<size_t>(requests[i].size_, size - cursor);
    std::memcpy(buffer, static_cast<const char *>(source) + cursor, take);
    std::memset(buffer + take, 0, requests[i].size_ - take);
    cursor += take;
  }
  data->batch_.RetainUntilComplete([activity](const IOBatchResult &result) { activity->Complete(result); });
  Accepted(executor_.TrySubmit(data->batch_));
  return ObjectWrite(std::move(data));
}
auto ObjectIO::Publish(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t offset, ObjectWrite &write)
    -> JournalResult {
  auto activity = std::make_shared<Activity>(state_);
  auto &data = *write.data_;
  if (data.owner_ != state_ || data.published_) {
    throw MetadataError(MetadataErrorCode::Conflict, "foreign or already published object write");
  }
  Successful(data.batch_.Result());
  if (!data.batch_.Result().writes_durable_) {
    throw std::logic_error("object publication requires durable data");
  }
  auto result =
      ObjectMappingAccess::ReplaceChecked(mapping_, base, key, offset, data.size_, data.reservation_, data.checksums_);
  // Any submitted commit consumes this publication attempt. Stale-base exceptions
  // before submission still allow an explicit retry with a new base.
  data.published_ = true;
  return result;
}
auto ObjectIO::ScrubRead(const ObjectMappingSnapshot &view, const ObjectScrubCandidate &candidate) -> ObjectRead {
  return ReadProtected(view, candidate.owner_, references_.ProtectOwned(candidate.owner_, *candidate.span_), {});
}
void ObjectIO::FinishScrub(const ObjectRead &read) {
  if (read.data_->batch_) {
    Successful(read.data_->batch_->Result());
    read.data_->validation_->Check();
  }
  for (const auto &journal : read.data_->journals_) journal.Verify();
}
auto ObjectIO::Error() const -> std::exception_ptr {
  std::lock_guard<std::mutex> lock(state_->mutex_);
  return state_->error_;
}
auto ObjectIO::WriteCommon(const ObjectMappingSnapshot &base, std::vector<ObjectChange> *changes,
                           const std::vector<CommonInput> &bytes, const std::vector<ObjectControlMutation> &controls,
                           std::function<void()> ready) -> CommonDataWrite {
  CommonDataWrite data;
  auto activity = std::make_shared<Activity>(state_);
  const auto unit = ObjectMappingAccess::Unit(mapping_);
  uint64_t allocation_size = 0;
  for (const auto &op : *changes) {
    if (op.length_ != 0 && (op.operation_ == ObjectOperation::Write || op.operation_ == ObjectOperation::Append)) {
      const auto rounded = (op.length_ + unit - 1) / unit * unit;
      if (rounded > max_write_bytes_ - allocation_size) {
        throw MetadataError(MetadataErrorCode::ResourceUnavailable, "Common allocation exceeds data budget");
      }
      allocation_size += rounded;
    }
  }
  if (allocation_size != 0) {
    data.reservation_.emplace(allocator_.Reserve(allocation_size));
    activity->write_.emplace(data.reservation_->Lease());
  }
  std::vector<RegionIORequest> requests;
  struct InputSlice {
    size_t input_, offset_, length_;
  };
  std::vector<InputSlice> slices;
  size_t extent = 0;
  uint64_t used = 0;
  for (size_t i = 0; i < changes->size(); ++i) {
    auto &op = (*changes)[i];
    op.extents_.clear();
    if (bytes[i].Size() == 0) {
      continue;
    }
    op.checksums_.clear();
    if (ObjectMappingAccess::Checksummed(mapping_))
      op.checksums_ = UnitChecksums(bytes[i].Data(), bytes[i].Size(), unit);
    uint64_t remaining = (op.length_ + unit - 1) / unit * unit;
    size_t cursor = 0;
    while (remaining != 0) {
      const auto &owned = data.reservation_->Extents()[extent];
      const auto take = std::min(remaining, owned.Size() - used);
      op.extents_.push_back(*StorageByteRange::Create(owned.Offset() + used, take));
      const auto payload = std::min<uint64_t>(take, bytes[i].Size() - cursor);
      if (!op.deferred_) {
        requests.push_back({region_, IOOperation::Write, owned.Offset() + used, take});
        slices.push_back({i, cursor, static_cast<size_t>(payload)});
      }
      cursor += payload;
      remaining -= take;
      used += take;
      if (used == owned.Size()) {
        ++extent;
        used = 0;
      }
    }
  }
  ObjectMappingAccess::Preflight(mapping_, base, *changes, data.reservation_ ? &*data.reservation_ : nullptr, controls);
  if (requests.empty()) {
    data.durable_ = true;  // Metadata-only request, no data Flush to invent.
    return data;
  }
  bool external = true;
  size_t capacity = 0;
  for (size_t i = 0; i < slices.size(); ++i) {
    const auto &slice = slices[i];
    const auto &input = bytes[slice.input_];
    external =
        external && input.source_ && slice.length_ == requests[i].size_ &&
        (reinterpret_cast<uintptr_t>(input.Data()) + slice.offset_) % executor_.DeviceInfo().memory_alignment_ == 0;
    capacity += input.source_ && slice.offset_ + slice.length_ == input.Size()
                    ? input.source_->capacity_ - slice.offset_
                    : requests[i].size_;
  }
  if (external) {
    CheckExternalBudget(requests, capacity);
    std::vector<IOBufferLease> leases;
    for (size_t i = 0; i < slices.size(); ++i) {
      const auto &slice = slices[i];
      const auto &input = bytes[slice.input_];
      leases.push_back(IOBufferLease::ForWrite(
          static_cast<const char *>(input.Data()) + slice.offset_,
          slice.offset_ + slice.length_ == input.Size() ? input.source_->capacity_ - slice.offset_ : requests[i].size_,
          [owner = input.source_->owner_] {}));
    }
    auto prepared = regions_.TryPrepareExternal(requests, leases, true);
    Accepted(prepared);
    data.batch_ = std::move(prepared.batch_);
  } else {
    CheckBatchBudget(requests);
    auto prepared = regions_.TryPrepare(requests, true);
    Accepted(prepared);
    data.batch_ = std::move(prepared.batch_);
    for (size_t i = 0; i < requests.size(); ++i) {
      auto *buffer = data.batch_->Buffer(i);
      const auto &slice = slices[i];
      std::memcpy(buffer, static_cast<const char *>(bytes[slice.input_].Data()) + slice.offset_, slice.length_);
      std::memset(buffer + slice.length_, 0, requests[i].size_ - slice.length_);
    }
  }
  data.batch_->RetainUntilComplete([activity](const IOBatchResult &result) { activity->Complete(result); },
                                   std::move(ready));
  Accepted(executor_.TrySubmit(*data.batch_));
  return data;
}
void ObjectIO::FinishCommon(CommonDataWrite *write) {
  if (write->batch_) {
    Successful(write->batch_->Result());
    if (!write->batch_->Result().writes_durable_) {
      throw std::logic_error("Common publication requires durable data");
    }
    write->durable_ = true;
    write->batch_.reset();  // B's Journal must be able to use these IO slots.
  }
}
auto ObjectIO::WriteDeferred(const DeferredTarget &target, const std::vector<std::byte> &body) -> IOBatch {
  if (body.empty() || body.size() > target.capacity_ || target.capacity_ > max_write_bytes_)
    throw std::invalid_argument("Deferred body exceeds its owned target or IO budget");
  auto activity = std::make_shared<Activity>(state_);
  std::vector<RegionIORequest> requests{{region_, IOOperation::Write, target.offset_, target.capacity_}};
  CheckBatchBudget(requests);
  auto prepared = regions_.TryPrepare(requests, true);
  Accepted(prepared);
  std::memcpy(prepared.batch_->Buffer(0), body.data(), body.size());
  std::memset(prepared.batch_->Buffer(0) + body.size(), 0, target.capacity_ - body.size());
  prepared.batch_->RetainUntilComplete(
      [activity, pin = target.payload_.retention_](const IOBatchResult &result) { activity->Complete(result); });
  Accepted(executor_.TrySubmit(*prepared.batch_));
  return std::move(*prepared.batch_);
}
void ObjectIO::Close() {
  std::unique_lock<std::mutex> lock(state_->mutex_);
  state_->closed_ = true;
  state_->idle_.wait(lock, [&] { return state_->active_ == 0; });
}
}  // namespace bustub
