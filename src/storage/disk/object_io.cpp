//===----------------------------------------------------------------------===//
// BusTub: ordinary object range IO on the existing bounded batch executor.
//===----------------------------------------------------------------------===//
#include "storage/disk/object_io.h"

#include <algorithm>
#include <condition_variable>  // NOLINT(build/c++11)
#include <cstring>
#include <mutex>  // NOLINT(build/c++11)
#include <utility>

#include "object_io_internal.h"  // NOLINT(build/include_subdir): private sibling component.

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
  if (admission != IOAdmission::Accepted) {
    throw MetadataError(
        admission == IOAdmission::Full ? MetadataErrorCode::ResourceUnavailable : MetadataErrorCode::NotReady,
        "object IO admission rejected");
  }
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
struct Piece {
  size_t size_;
  size_t skip_;
  std::optional<size_t> member_;
};
}  // namespace
struct ObjectReadData {
  size_t size_{0};
  std::vector<Piece> pieces_;
  std::optional<IOBatch> batch_;
};
struct ObjectWriteData {
  std::shared_ptr<ObjectIOState> owner_;
  DataReservation reservation_;
  IOBatch batch_;
  size_t size_;
  bool published_{false};
};
ObjectRead::ObjectRead(std::unique_ptr<ObjectReadData> data) : data_(std::move(data)) {}
ObjectRead::~ObjectRead() = default;
ObjectRead::ObjectRead(ObjectRead &&other) noexcept = default;
auto ObjectRead::operator=(ObjectRead &&other) noexcept -> ObjectRead & = default;
auto ObjectRead::Size() const -> size_t { return data_->size_; }
void ObjectRead::Wait() const {
  if (data_->batch_) {
    data_->batch_->Wait();
  }
}
auto ObjectRead::WaitFor(std::chrono::milliseconds timeout) const -> bool {
  return !data_->batch_ || data_->batch_->WaitFor(timeout);
}
void ObjectRead::CopyTo(void *destination, size_t capacity) const {
  if (capacity < Size() || (Size() != 0 && destination == nullptr)) {
    throw std::invalid_argument("object read destination is too small");
  }
  if (data_->batch_) {
    Successful(data_->batch_->Result());
  }
  auto *out = static_cast<char *>(destination);
  for (const auto &piece : data_->pieces_) {
    if (piece.member_) {
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
                   ObjectReferenceManager &references, uint64_t max_read_bytes, uint64_t max_write_bytes)
    : regions_(regions),
      executor_(executor),
      allocator_(allocator),
      mapping_(mapping),
      references_(references),
      region_(regions.Region(RegionKind::Data)),
      max_read_bytes_(max_read_bytes),
      max_write_bytes_(max_write_bytes),
      state_(std::make_shared<ObjectIOState>()) {
  if (max_read_bytes == 0 || max_write_bytes == 0) {
    throw std::invalid_argument("object IO requires positive request budgets");
  }
}
ObjectIO::~ObjectIO() { Close(); }
auto ObjectIO::Read(const ObjectMappingSnapshot &view, ObjectKey key, uint64_t offset, uint64_t length) -> ObjectRead {
  if (length > max_read_bytes_) {
    throw MetadataError(MetadataErrorCode::ResourceUnavailable, "object read exceeds request budget");
  }
  auto activity = std::make_shared<Activity>(state_);
  activity->read_.emplace(references_.ProtectRead(view, key, offset, length));
  auto data = std::make_unique<ObjectReadData>();
  std::vector<RegionIORequest> requests;
  const uint64_t align = regions_.Describe(region_).offset_alignment_;
  for (const auto &span : activity->read_->Spans()) {
    Piece piece{static_cast<size_t>(span.size_), 0, std::nullopt};
    if (span.data_) {
      const auto start = span.data_->offset_ / align * align;
      const auto skip = span.data_->offset_ - start;
      const auto amount = (skip + span.size_ + align - 1) / align * align;
      piece.skip_ = skip;
      piece.member_ = requests.size();
      requests.push_back({region_, IOOperation::Read, start, amount});
    }
    data->pieces_.push_back(piece);
    data->size_ += piece.size_;
  }
  if (!requests.empty()) {
    auto prepared = regions_.TryPrepare(requests, false);
    Accepted(prepared.admission_);
    data->batch_ = std::move(prepared.batch_);
    data->batch_->RetainUntilComplete([activity](const IOBatchResult &result) { activity->Complete(result); });
    Accepted(executor_.TrySubmit(*data->batch_));
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
  auto prepared = regions_.TryPrepare(requests, true);
  Accepted(prepared.admission_);
  auto data = std::make_unique<ObjectWriteData>(
      ObjectWriteData{state_, std::move(reservation), std::move(*prepared.batch_), size, false});
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
  auto result = mapping_.Replace(base, key, offset, data.size_, data.reservation_);
  // Any submitted commit consumes this publication attempt. Stale-base exceptions
  // before submission still allow an explicit retry with a new base.
  data.published_ = true;
  return result;
}
auto ObjectIO::Error() const -> std::exception_ptr {
  std::lock_guard<std::mutex> lock(state_->mutex_);
  return state_->error_;
}
void ObjectIO::Close() {
  std::unique_lock<std::mutex> lock(state_->mutex_);
  state_->closed_ = true;
  state_->idle_.wait(lock, [&] { return state_->active_ == 0; });
}
}  // namespace bustub
