//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// bootstrap_store.cpp
//
//===----------------------------------------------------------------------===//

#include "storage/disk/bootstrap_store.h"

#include <algorithm>
#include <condition_variable>  // NOLINT(build/c++11)
#include <cstring>
#include <limits>
#include <mutex>   // NOLINT(build/c++11)
#include <thread>  // NOLINT(build/c++11)
#include <utility>
#include <vector>

#include "common/byte_codec.h"
#include "storage/disk/io_executor.h"

namespace bustub {
namespace {

constexpr size_t BOOTSTRAP_SLOT_SIZE = 64 * 1024;
constexpr size_t BOOTSTRAP_CHECKSUM_OFFSET = BOOTSTRAP_SLOT_SIZE - sizeof(uint32_t);
constexpr uint64_t BOOTSTRAP_SLOT_OFFSETS[] = {0, 1024 * 1024};
constexpr char BOOTSTRAP_MAGIC[] = "BUSTBOOT";
using Image = std::vector<std::byte>;
using Images = std::array<Image, 2>;

[[noreturn]] void Fail(BootstrapErrorCode code, const char *message) { throw BootstrapError(code, message); }

auto IsZero(const std::byte *begin, const std::byte *end) -> bool {
  return std::all_of(begin, end, [](std::byte b) { return b == std::byte{0}; });
}

auto SameIdentity(const BootstrapIdentity &a, const BootstrapIdentity &b) -> bool {
  return a.storage_ == b.storage_ && a.device_ == b.device_;
}

void ValidateIdentity(const BootstrapIdentity &identity) {
  for (const auto *id : {&identity.storage_, &identity.device_}) {
    if (std::all_of(id->begin(), id->end(), [](uint8_t b) { return b == 0; })) {
      Fail(BootstrapErrorCode::IdentityMismatch, "bootstrap identity must be nonzero");
    }
  }
}

auto Slot(size_t index) -> StorageByteRange {
  return *StorageByteRange::Create(BOOTSTRAP_SLOT_OFFSETS[index], BOOTSTRAP_SLOT_SIZE);
}

auto Overlaps(StorageByteRange a, StorageByteRange b) -> bool {
  return a.Offset() < b.Offset() + b.Size() && b.Offset() < a.Offset() + a.Size();
}

void ValidateGeometry(const BlockDeviceInfo &info) {
  for (size_t i = 0; i < 2; ++i) {
    const auto slot = Slot(i);
    if (info.offset_alignment_ == 0 || slot.Offset() % info.offset_alignment_ != 0 ||
        slot.Size() % info.offset_alignment_ != 0 || slot.Offset() + slot.Size() > info.capacity_) {
      Fail(BootstrapErrorCode::InvalidLayout, "device cannot hold aligned version 1 bootstrap slots");
    }
  }
}

void ValidateLayout(const BootstrapLayout &layout, const BlockDeviceInfo &info) {
  ValidateIdentity(layout.identity_);
  if (layout.capacity_ != info.capacity_) {
    Fail(BootstrapErrorCode::InvalidLayout, "recorded device capacity differs from opened device");
  }
  for (size_t i = 0; i < layout.regions_.size(); ++i) {
    const auto range = layout.regions_[i];
    if (range.Size() == 0 || range.Offset() + range.Size() > info.capacity_ ||
        range.Offset() + range.Size() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
        range.Offset() % info.offset_alignment_ != 0 || range.Size() % info.offset_alignment_ != 0) {
      Fail(BootstrapErrorCode::InvalidLayout, "invalid bootstrap region boundary or alignment");
    }
    for (size_t j = 0; j < 2; ++j) {
      if (Overlaps(range, Slot(j))) {
        Fail(BootstrapErrorCode::InvalidLayout, "bootstrap region overlaps a bootstrap slot");
      }
    }
    for (size_t j = 0; j < i; ++j) {
      if (Overlaps(range, layout.regions_[j])) {
        Fail(BootstrapErrorCode::InvalidLayout, "bootstrap regions overlap");
      }
    }
  }
}

auto Encode(const BootstrapLayout &layout) -> Image {
  ByteWriter writer;
  writer.PutBytes(BOOTSTRAP_MAGIC, 8);
  writer.PutU32(1);
  writer.PutU32(112);
  writer.PutU32(0);
  writer.PutU32(0);
  writer.PutBytes(layout.identity_.storage_.data(), 16);
  writer.PutBytes(layout.identity_.device_.data(), 16);
  writer.PutU64(layout.capacity_);
  for (const auto &range : layout.regions_) {
    writer.PutU64(range.Offset());
    writer.PutU64(range.Size());
  }
  auto image = writer.Take();
  image.resize(BOOTSTRAP_CHECKSUM_OFFSET, std::byte{0});
  ByteWriter checksum;
  checksum.PutU32(Crc32c(image));
  image.insert(image.end(), checksum.Data().begin(), checksum.Data().end());
  return image;
}

enum class CopyKind { Empty, Corrupt, Valid };
struct Copy {
  CopyKind kind_;
  std::optional<BootstrapLayout> layout_;
};

auto Decode(const Image &image, const BlockDeviceInfo &info, const BootstrapIdentity &expected) -> Copy {
  if (IsZero(image.data(), image.data() + image.size())) {
    return {CopyKind::Empty, std::nullopt};
  }
  ByteReader checksum(image.data() + BOOTSTRAP_CHECKSUM_OFFSET, sizeof(uint32_t));
  if (Crc32c(image.data(), BOOTSTRAP_CHECKSUM_OFFSET) != checksum.ReadU32()) {
    return {CopyKind::Corrupt, std::nullopt};
  }
  ByteReader reader(image);
  reader.Skip(8);
  if (std::memcmp(image.data(), BOOTSTRAP_MAGIC, 8) != 0 || reader.ReadU32() != 1 || reader.ReadU32() != 112 ||
      reader.ReadU32() != 0 || reader.ReadU32() != 0 ||
      !IsZero(image.data() + 112, image.data() + BOOTSTRAP_CHECKSUM_OFFSET)) {
    Fail(BootstrapErrorCode::UnsupportedFormat, "unsupported bootstrap format or features");
  }
  BootstrapIdentity identity{};
  for (auto *id : {&identity.storage_, &identity.device_}) {
    for (auto &b : *id) {
      b = reader.ReadU8();
    }
  }
  if (!SameIdentity(identity, expected)) {
    Fail(BootstrapErrorCode::IdentityMismatch, "bootstrap copy belongs to a different storage or device");
  }
  const auto capacity = reader.ReadU64();
  auto read_range = [&reader]() {
    const auto offset = reader.ReadU64();
    const auto length = reader.ReadU64();
    auto range = StorageByteRange::Create(offset, length);
    if (!range) {
      Fail(BootstrapErrorCode::InvalidLayout, "bootstrap region overflows address space");
    }
    return *range;
  };
  BootstrapLayout layout{identity, capacity, {read_range(), read_range(), read_range()}};
  ValidateLayout(layout, info);
  return {CopyKind::Valid, layout};
}

struct Selection {
  BootstrapLayout layout_;
  size_t source_;
  bool degraded_;
};

auto Select(const Images &images, const BlockDeviceInfo &info, const BootstrapIdentity &expected) -> Selection {
  // Inspect both, including a checksum-valid unknown/conflicting second copy.
  const auto a = Decode(images[0], info, expected);
  const auto b = Decode(images[1], info, expected);
  if (a.kind_ == CopyKind::Valid && b.kind_ == CopyKind::Valid) {
    if (images[0] != images[1]) {
      Fail(BootstrapErrorCode::ConflictingCopies, "valid bootstrap copies disagree");
    }
    return {*a.layout_, 0, false};
  }
  if (a.kind_ == CopyKind::Valid) {
    return {*a.layout_, 0, true};
  }
  if (b.kind_ == CopyKind::Valid) {
    return {*b.layout_, 1, true};
  }
  if (a.kind_ == CopyKind::Empty && b.kind_ == CopyKind::Empty) {
    Fail(BootstrapErrorCode::Unformatted, "bootstrap slots are empty; explicit Create required");
  }
  Fail(BootstrapErrorCode::Corrupt, "no valid bootstrap copy");
}

struct RepairStopped {};

auto Retryable(const std::exception_ptr &error) -> bool {
  try {
    std::rethrow_exception(error);
  } catch (const BootstrapError &e) {
    return e.Code() == BootstrapErrorCode::Corrupt || e.Code() == BootstrapErrorCode::ResourceUnavailable;
  } catch (const std::system_error &) {
    return true;
  } catch (...) {
    return false;
  }
}

}  // namespace

struct BootstrapStore::Impl {
  explicit Impl(IOExecutor &executor) : executor_(executor) { ValidateGeometry(executor_.DeviceInfo()); }

  void CheckLifecycle() const {
    if (closed_ || opened_) {
      throw std::logic_error("bootstrap instance is closed or already open");
    }
  }

  void CheckStop() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stop_) {
      throw RepairStopped{};
    }
  }

  void Pause(std::chrono::milliseconds delay) {
    const auto start = std::chrono::steady_clock::now();
    std::unique_lock<std::mutex> lock(mutex_);
    for (;;) {
      const auto elapsed =
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
      if (stop_) {
        throw RepairStopped{};
      }
      if (elapsed >= delay) {
        return;
      }
      // Bound each clock addition, even for a caller-supplied very large wait.
      wake_.wait_for(lock, std::min(delay - elapsed, std::chrono::milliseconds(std::chrono::hours(1))),
                     [this] { return stop_; });
    }
  }

  auto Prepare(const std::vector<IORequest> &requests, bool flush, const BootstrapRepairOptions *options) -> IOBatch {
    const auto start = std::chrono::steady_clock::now();
    for (;;) {
      if (options != nullptr) {
        CheckStop();
      }
      auto result = executor_.TryPrepare(requests, flush);
      if (result.admission_ == IOAdmission::Accepted) {
        return std::move(*result.batch_);
      }
      if (result.admission_ == IOAdmission::Stopped) {
        Fail(BootstrapErrorCode::ExecutorStopped, "bootstrap IO executor is stopped");
      }
      const auto elapsed =
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
      if (options == nullptr || elapsed >= options->admission_timeout_) {
        Fail(BootstrapErrorCode::ResourceUnavailable, "bootstrap IO admission capacity unavailable");
      }
      Pause(std::min(options->retry_delay_, options->admission_timeout_ - elapsed));
    }
  }

  void Execute(IOBatch &batch, bool flush, const BootstrapRepairOptions *options) {
    IOAdmission admission;
    if (options != nullptr) {
      // Serialize the short, non-waiting submission with Close's stop decision.
      // No state lock is held while waiting for the device or a batch result.
      std::lock_guard<std::mutex> lock(mutex_);
      if (stop_) {
        throw RepairStopped{};
      }
      admission = executor_.TrySubmit(batch);
    } else {
      admission = executor_.TrySubmit(batch);
    }
    if (admission != IOAdmission::Accepted) {
      Fail(BootstrapErrorCode::ExecutorStopped, "bootstrap IO executor stopped before submission");
    }
    batch.Wait();  // Accepted IO must drain even when Close requests stop.
    const auto &result = batch.Result();
    for (const auto &operation : result.operations_) {
      if (operation.error_) {
        std::rethrow_exception(operation.error_);
      }
    }
    if (result.flush_error_) {
      std::rethrow_exception(result.flush_error_);
    }
    if (std::any_of(result.operations_.begin(), result.operations_.end(),
                    [](const auto &op) { return op.outcome_ != IOOutcome::Succeeded; }) ||
        (flush && !result.writes_durable_)) {
      throw std::runtime_error("incomplete bootstrap IO batch");
    }
    if (options != nullptr) {
      CheckStop();
    }
  }

  auto Read(const BootstrapRepairOptions *options = nullptr) -> Images {
    auto batch = Prepare({{IOOperation::Read, Slot(0)}, {IOOperation::Read, Slot(1)}}, false, options);
    Execute(batch, false, options);
    Images images;
    for (size_t i = 0; i < 2; ++i) {
      images[i].resize(BOOTSTRAP_SLOT_SIZE);
      std::memcpy(images[i].data(), batch.Buffer(i), BOOTSTRAP_SLOT_SIZE);
    }
    return images;
  }

  void Write(const Image &image, const std::vector<size_t> &slots, const BootstrapRepairOptions *options = nullptr) {
    std::vector<IORequest> requests;
    requests.reserve(slots.size());
    for (const auto slot : slots) {
      requests.push_back({IOOperation::Write, Slot(slot)});
    }
    auto batch = Prepare(requests, true, options);
    for (size_t i = 0; i < slots.size(); ++i) {
      std::memcpy(batch.Buffer(i), image.data(), BOOTSTRAP_SLOT_SIZE);
    }
    Execute(batch, true, options);
  }

  void CheckRepairSource(const Images &images) const {
    const auto source = Decode(images[source_slot_], executor_.DeviceInfo(), layout_->identity_);
    if (source.kind_ != CopyKind::Valid || images[source_slot_] != source_image_) {
      Fail(BootstrapErrorCode::SourceChanged, "bootstrap repair source changed or was lost");
    }
  }

  void Repair(const BootstrapRepairOptions &options) {
    // A failed Flush followed by readable matching bytes is not durable success.
    bool needs_durability = false;
    for (uint32_t attempt = 0; attempt < options.max_attempts_; ++attempt) {
      try {
        CheckStop();
        {
          std::lock_guard<std::mutex> lock(mutex_);
          status_.repair_state_ = BootstrapRepairState::Running;
          status_.attempts_ = attempt + 1;
        }
        auto images = Read(&options);
        CheckRepairSource(images);
        const auto selected = Select(images, executor_.DeviceInfo(), layout_->identity_);
        if (selected.degraded_ || needs_durability) {
          try {
            Write(source_image_, {1 - source_slot_}, &options);
          } catch (...) {
            // Even matching readback cannot discharge a failed persistence barrier.
            needs_durability = true;
            throw;
          }
          needs_durability = false;
          images = Read(&options);
          CheckRepairSource(images);
          const auto verified = Select(images, executor_.DeviceInfo(), layout_->identity_);
          if (verified.degraded_ || images[0] != source_image_ || images[1] != source_image_) {
            Fail(BootstrapErrorCode::Corrupt, "bootstrap repair verification did not restore both copies");
          }
        }
        {
          std::lock_guard<std::mutex> lock(mutex_);
          if (stop_) {
            throw RepairStopped{};
          }
          status_.repair_state_ = BootstrapRepairState::Succeeded;
          status_.redundancy_lost_ = false;
          status_.last_error_ = nullptr;
        }
        return;
      } catch (const RepairStopped &) {
        throw;
      } catch (...) {
        const auto error = std::current_exception();
        const bool retry = Retryable(error) && attempt + 1 < options.max_attempts_;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          status_.last_error_ = error;
          status_.repair_state_ = retry ? BootstrapRepairState::Pending : BootstrapRepairState::Failed;
        }
        CheckStop();
        if (!retry) {
          return;
        }
        Pause(options.retry_delay_);
      }
    }
  }

  void Close() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stop_ = true;
    }
    wake_.notify_all();
    if (coordinator_.joinable()) {
      coordinator_.join();
    }
    closed_ = true;
  }

  IOExecutor &executor_;
  mutable std::mutex mutex_;
  std::condition_variable wake_;
  BootstrapStatus status_;
  bool stop_{false};
  bool closed_{false};
  bool opened_{false};
  size_t source_slot_{0};
  std::optional<BootstrapLayout> layout_;
  Image source_image_;
  std::thread coordinator_;
};

BootstrapStore::BootstrapStore(IOExecutor &executor) : impl_(std::make_unique<Impl>(executor)) {}
BootstrapStore::~BootstrapStore() { impl_->Close(); }

void BootstrapStore::Create(const BootstrapLayout &layout) {
  impl_->CheckLifecycle();
  ValidateLayout(layout, impl_->executor_.DeviceInfo());
  const auto images = impl_->Read();
  for (const auto &image : images) {
    if (!IsZero(image.data(), image.data() + image.size())) {
      Fail(BootstrapErrorCode::NotEmpty, "Create refuses to overwrite nonzero bootstrap slots");
    }
  }
  impl_->Write(Encode(layout), {0, 1});
}

auto BootstrapStore::Open(const BootstrapIdentity &expected) -> BootstrapOpenResult {
  impl_->CheckLifecycle();
  ValidateIdentity(expected);
  auto images = impl_->Read();
  const auto selected = Select(images, impl_->executor_.DeviceInfo(), expected);
  impl_->layout_ = selected.layout_;
  impl_->source_slot_ = selected.source_;
  impl_->source_image_ = std::move(images[selected.source_]);
  {
    std::lock_guard<std::mutex> lock(impl_->mutex_);
    impl_->status_.redundancy_lost_ = selected.degraded_;
  }
  impl_->opened_ = true;
  return {selected.layout_, selected.degraded_};
}

void BootstrapStore::StartRepair(const BootstrapRepairOptions &options) {
  if (impl_->closed_ || !impl_->opened_) {
    throw std::logic_error("repair requires a live open bootstrap instance");
  }
  if (options.max_attempts_ == 0 || options.retry_delay_.count() <= 0 || options.admission_timeout_.count() <= 0) {
    throw std::invalid_argument("repair requires positive attempt and wait limits");
  }
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  if (!impl_->status_.redundancy_lost_ || impl_->coordinator_.joinable()) {
    return;
  }
  impl_->status_.repair_state_ = BootstrapRepairState::Pending;
  try {
    impl_->coordinator_ = std::thread([this, options] {
      try {
        impl_->Repair(options);
      } catch (const RepairStopped &) {
        std::lock_guard<std::mutex> lock(impl_->mutex_);
        impl_->status_.repair_state_ = BootstrapRepairState::Stopped;
      } catch (...) {
        std::lock_guard<std::mutex> lock(impl_->mutex_);
        impl_->status_.repair_state_ = BootstrapRepairState::Failed;
        impl_->status_.last_error_ = std::current_exception();
      }
    });
  } catch (...) {
    impl_->status_.repair_state_ = BootstrapRepairState::Idle;
    throw;
  }
}

auto BootstrapStore::Status() const -> BootstrapStatus {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  return impl_->status_;
}

void BootstrapStore::Close() { impl_->Close(); }

auto BootstrapStore::BindRegions() const -> RegionBinding {
  if (impl_->closed_ || !impl_->opened_) {
    throw std::logic_error("region binding requires a live open bootstrap instance");
  }
  return {&impl_->executor_, impl_->layout_->regions_, impl_->layout_->identity_};
}

}  // namespace bustub
