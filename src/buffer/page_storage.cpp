#include "buffer/page_storage.h"
#include <algorithm>
#include <cstring>
#include <mutex>
#include <set>
#include <thread>
#include "common/byte_codec.h"
#include "storage/disk/disk_manager.h"
#include "storage/disk/node_storage.h"

namespace bustub {
namespace {
void Durable(const JournalResult &r) {
  if (r.outcome_ != JournalOutcome::Durable) {
    if (r.error_) {
      std::rethrow_exception(r.error_);
    }
    throw std::runtime_error("page transaction did not durably publish");
  }
}
void Submit(NodeStorage &storage, ObjectTransaction &tx) {
  for (;;) {
    auto submitted = storage.SubmitObjects(tx);
    if (submitted.admission_ == IOAdmission::Accepted) {
      submitted.ticket_->Wait();
      Durable(submitted.ticket_->Result());
      return;  // release retained result capacity before the next batch
    }
    if (submitted.admission_ != IOAdmission::Full) {
      throw std::runtime_error("page transaction admission is stopped");
    }
    // Synchronous compatibility facade; decoupling Raft advancement from this
    // wait belongs to F35. No additional thread is created per page request.
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}
auto Descriptor(ObjectPageOptions options, uint64_t objects, std::optional<uint64_t> sealed = std::nullopt)
    -> std::vector<std::byte> {
  ByteWriter w;
  w.PutU32(sealed ? 2 : 1);
  w.PutU32(BUSTUB_PAGE_SIZE);
  w.PutU32(options.pages_per_object_);
  w.PutU64(objects);
  w.PutU64(options.registry_.space_);
  w.PutU64(options.registry_.number_);
  if (sealed) w.PutU64(*sealed);
  return w.Data();
}
}  // namespace

struct ObjectPageStorage::Impl {
  std::shared_ptr<NodeStorage> storage_;
  uint64_t space_;
  ObjectPageOptions options_;
  PageIOCapabilities caps_;
  std::mutex allocation_;
  uint64_t objects_{0};
  bool retired_{false};
  std::optional<uint64_t> sealed_;
  std::set<page_id_t> pending_pages_;
  Impl(std::shared_ptr<NodeStorage> storage, uint64_t space, ObjectPageOptions options)
      : storage_(std::move(storage)), space_(space), options_(options), caps_(storage_->PageIO()) {
    if (options.registry_.space_ == 0 || options.pages_per_object_ == 0 || caps_.max_batch_pages_ == 0 ||
        caps_.max_read_bytes_ < BUSTUB_PAGE_SIZE) {
      throw std::invalid_argument("page backend has no usable page capacity");
    }
  }
  auto Key(page_id_t page) const -> ObjectKey {
    return {space_, 1 + static_cast<uint64_t>(page) / options_.pages_per_object_};
  }
  auto Offset(page_id_t page) const -> uint64_t {
    return (page % options_.pages_per_object_) * uint64_t{BUSTUB_PAGE_SIZE};
  }
  void CheckPage(page_id_t page) const {
    if (storage_->Objects().Control({space_, 0}, uint64_t{static_cast<uint32_t>(page)} + 1)) {
      throw std::runtime_error("database page identity is retired");
    }
  }
  void Writable() const {
    if (sealed_ || retired_) throw std::runtime_error("page workspace is immutable or retired");
  }
  // allocation_ serializes workspace growth/retirement, never BufferPool's
  // residency or content locks. F14's ordinary background GC handles the bytes.
  void Reclaim(size_t limit) {
    while (limit-- != 0 && !pending_pages_.empty()) {
      const auto page = *pending_pages_.begin();
      ObjectTransaction tx;
      ObjectMutation unmap{ObjectOperation::Unmap, Key(page), Offset(page), ObjectSizeMode::Fixed, {}};
      unmap.length_ = BUSTUB_PAGE_SIZE;
      tx.objects_.push_back(std::move(unmap));
      tx.controls_.push_back(
          {{space_, 0}, uint64_t{static_cast<uint32_t>(page)} + 1, std::vector<std::byte>{std::byte{1}, std::byte{2}}});
      Submit(*storage_, tx);
      pending_pages_.erase(page);
    }
  }
};
ObjectPageStorage::ObjectPageStorage(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
ObjectPageStorage::~ObjectPageStorage() = default;
auto ObjectPageStorage::Create(std::shared_ptr<NodeStorage> storage, ObjectPageOptions options)
    -> std::shared_ptr<ObjectPageStorage> {
  auto impl = std::make_unique<Impl>(storage, 0, options);  // validate before allocating identity
  for (;;) {
    try {
      const auto creation = storage->CreateObjectSpace(storage->Objects());
      Durable(creation.result_);
      impl->space_ = *creation.space_;
      break;
    } catch (const MetadataViewConflict &) {
      // Background metadata work may publish between Read and Commit. No
      // identity was allocated by this rejected attempt; recompute from B.
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  ObjectTransaction tx{{},
                       {{{impl->space_, 0}, 0, Descriptor(options, 0)},
                        {options.registry_, impl->space_, std::vector<std::byte>{std::byte{1}}}}};
  Submit(*storage, tx);
  return std::shared_ptr<ObjectPageStorage>(new ObjectPageStorage(std::move(impl)));
}
auto ObjectPageStorage::Open(std::shared_ptr<NodeStorage> storage, uint64_t space, ObjectPageOptions options)
    -> std::shared_ptr<ObjectPageStorage> {
  auto impl = std::make_unique<Impl>(storage, space, options);
  auto record = storage->Objects().Control({space, 0}, 0);
  if (!record) {
    throw std::runtime_error("missing page workspace descriptor");
  }
  ByteReader r(*record);
  const auto format = r.ReadU32();
  if ((format != 1 && format != 2) || r.ReadU32() != BUSTUB_PAGE_SIZE || r.ReadU32() != options.pages_per_object_) {
    throw std::runtime_error("page workspace geometry mismatch");
  }
  impl->objects_ = r.ReadU64();
  if (r.ReadU64() != options.registry_.space_ || r.ReadU64() != options.registry_.number_) {
    throw std::runtime_error("page workspace registry mismatch");
  }
  if (format == 2) {
    impl->sealed_ = r.ReadU64();
    if (*impl->sealed_ > uint64_t{INT32_MAX} + 1 ||
        (*impl->sealed_ + options.pages_per_object_ - 1) / options.pages_per_object_ != impl->objects_) {
      throw std::runtime_error("invalid checkpoint page allocation boundary");
    }
  }
  if (!r.Empty()) {
    throw std::runtime_error("trailing workspace descriptor bytes");
  }
  uint64_t cursor = 1;
  for (;;) {
    const auto records = storage->Objects().Controls({space, 0}, cursor, 1);
    if (records.empty()) {
      break;
    }
    const auto &entry = records.front();
    if (entry.item_ > uint64_t{INT32_MAX} + 1 || entry.value_.size() != 2 || entry.value_[0] != std::byte{1} ||
        (entry.value_[1] != std::byte{1} && entry.value_[1] != std::byte{2})) {
      throw std::runtime_error("invalid retired page record");
    }
    if (entry.value_[1] == std::byte{1}) {
      impl->pending_pages_.insert(static_cast<page_id_t>(entry.item_ - 1));
    }
    cursor = entry.item_ + 1;
  }
  while (!impl->pending_pages_.empty()) {
    if (impl->sealed_) throw std::runtime_error("immutable page workspace contains unfinished retirement");
    impl->Reclaim(4);
  }
  return std::shared_ptr<ObjectPageStorage>(new ObjectPageStorage(std::move(impl)));
}
void ObjectPageStorage::RetireAbandoned(std::shared_ptr<NodeStorage> storage, ObjectPageOptions options) {
  RetireAbandoned(std::move(storage), options, {});
}
void ObjectPageStorage::RetireAbandoned(std::shared_ptr<NodeStorage> storage, ObjectPageOptions options,
                                        const std::vector<uint64_t> &retained) {
  uint64_t cursor = 1;
  for (;;) {
    const auto entries = storage->Objects().Controls(options.registry_, cursor, 1);
    if (entries.empty()) {
      return;
    }
    const auto id = entries.front().item_;
    cursor = id + 1;
    if (std::find(retained.begin(), retained.end(), id) != retained.end()) continue;
    auto workspace = Open(storage, id, options);
    workspace->Retire();
  }
}
auto ObjectPageStorage::Capture(uint64_t next_page, std::optional<std::vector<page_id_t>> pages) -> ObjectPageCapture {
  std::lock_guard<std::mutex> lock(impl_->allocation_);
  if (pages) std::sort(pages->begin(), pages->end());
  ObjectPageCapture capture(impl_->storage_->Objects(), impl_->space_, impl_->objects_, next_page, impl_->options_,
                            std::move(pages));
  for (uint64_t n = 1; n <= impl_->objects_; ++n) {
    const auto begin = (n - 1) * impl_->options_.pages_per_object_;
    if (capture.pages_) {
      const auto found = std::lower_bound(capture.pages_->begin(), capture.pages_->end(), begin);
      if (found == capture.pages_->end() ||
          uint64_t{static_cast<uint32_t>(*found)} >= begin + impl_->options_.pages_per_object_)
        continue;
    }
    const ObjectKey key{impl_->space_, n};
    const auto size = capture.view_.Describe(key).size_;
    const auto ranges = capture.view_.Resolve(key, 0, size);
    if (ranges.complete_ && std::none_of(ranges.spans_.begin(), ranges.spans_.end(),
                                         [](const auto &span) { return span.data_.has_value(); }))
      continue;
    capture.leases_.push_back(impl_->storage_->ProtectObject(capture.view_, key, 0, size));
  }
  return capture;
}
auto ObjectPageStorage::Clone(std::shared_ptr<NodeStorage> storage, const ObjectPageCapture &capture)
    -> std::shared_ptr<ObjectPageStorage> {
  auto result = Create(storage, capture.options_);
  result->EnsurePages(capture.next_);
  // A page bounds each metadata publication. Captured leases keep source bytes
  // alive while these durable shared references are established in batches.
  for (uint64_t n = 1; n <= capture.objects_; ++n) {
    const ObjectKey source{capture.space_, n}, destination{result->Space(), n};
    const auto size = capture.view_.Describe(source).size_;
    for (uint64_t offset = 0; offset < size; offset += BUSTUB_PAGE_SIZE) {
      const auto page = (n - 1) * capture.options_.pages_per_object_ + offset / BUSTUB_PAGE_SIZE;
      if (page >= capture.next_ ||
          (capture.pages_ &&
           !std::binary_search(capture.pages_->begin(), capture.pages_->end(), static_cast<page_id_t>(page))))
        continue;
      const auto length = std::min<uint64_t>(BUSTUB_PAGE_SIZE, size - offset);
      const auto resolved = capture.view_.Resolve(source, offset, length);
      if (resolved.complete_ && std::none_of(resolved.spans_.begin(), resolved.spans_.end(),
                                             [](const auto &span) { return span.data_.has_value(); }))
        continue;
      for (;;) {
        try {
          Durable(storage->ShareObjectRange(capture.view_, source, destination, offset, length, offset));
          break;
        } catch (const MetadataViewConflict &) {
          continue;
        } catch (const MetadataCommitBusy &) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
      }
    }
  }
  uint64_t cursor = 1;
  for (;;) {
    const auto entries = capture.view_.Controls({capture.space_, 0}, cursor, 1);
    if (entries.empty()) break;
    const auto &e = entries.front();
    ObjectTransaction tx{{}, {{{result->Space(), 0}, e.item_, std::vector<std::byte>{std::byte{1}, std::byte{2}}}}};
    Submit(*storage, tx);
    cursor = e.item_ + 1;
  }
  return result;
}
void ObjectPageStorage::Seal(uint64_t next_page) {
  std::lock_guard<std::mutex> lock(impl_->allocation_);
  impl_->Writable();
  ObjectTransaction tx{{}, {{{impl_->space_, 0}, 0, Descriptor(impl_->options_, impl_->objects_, next_page)}}};
  Submit(*impl_->storage_, tx);
  impl_->sealed_ = next_page;
}
auto ObjectPageStorage::SealedPageCount() const -> uint64_t {
  if (!impl_->sealed_) throw std::runtime_error("checkpoint does not name an immutable workspace");
  return *impl_->sealed_;
}
auto ObjectPageStorage::Space() const -> uint64_t { return impl_->space_; }
auto ObjectPageStorage::MemoryAlignment() const -> size_t { return impl_->caps_.memory_alignment_; }
auto ObjectPageStorage::MaxBatchPages() const -> size_t { return impl_->caps_.max_batch_pages_; }
auto ObjectPageStorage::WriteDomain(page_id_t page) const -> uint64_t { return impl_->Key(page).number_; }
void ObjectPageStorage::EnsurePages(uint64_t count) {
  std::lock_guard<std::mutex> lock(impl_->allocation_);
  impl_->Writable();
  const uint64_t needed = (count + impl_->options_.pages_per_object_ - 1) / impl_->options_.pages_per_object_;
  while (impl_->objects_ < needed) {
    const auto next = impl_->objects_ + 1;
    ObjectTransaction tx;
    tx.objects_.push_back({ObjectOperation::Create,
                           {impl_->space_, next},
                           uint64_t{impl_->options_.pages_per_object_} * BUSTUB_PAGE_SIZE,
                           ObjectSizeMode::Variable,
                           {}});
    tx.controls_.push_back({{impl_->space_, 0}, 0, Descriptor(impl_->options_, next)});
    Submit(*impl_->storage_, tx);
    impl_->objects_ = next;
  }
}
void ObjectPageStorage::Read(const PageBuffer &b) {
  impl_->CheckPage(b.page_);
  for (;;) {
    try {
      auto read =
          impl_->storage_->ReadObjectInto(impl_->storage_->Objects(), impl_->Key(b.page_), impl_->Offset(b.page_),
                                          BUSTUB_PAGE_SIZE, {b.data_, b.capacity_, b.owner_});
      read.Finish();
      if (read.Size() != BUSTUB_PAGE_SIZE) {
        throw std::runtime_error("short database page");
      }
      return;
    } catch (const ObjectIOBusy &) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
}
auto ObjectPageStorage::Prefetch(const PageBuffer &b, std::function<void(std::exception_ptr)> complete) -> bool {
  impl_->CheckPage(b.page_);
  return impl_->storage_->PrefetchObjectInto(impl_->storage_->Objects(), impl_->Key(b.page_), impl_->Offset(b.page_),
                                             BUSTUB_PAGE_SIZE, {b.data_, b.capacity_, b.owner_}, std::move(complete));
}
void ObjectPageStorage::Write(const std::vector<PageBuffer> &buffers) {
  impl_->Writable();
  ObjectTransaction tx;
  for (const auto &b : buffers) {
    impl_->CheckPage(b.page_);
    ObjectMutation op{ObjectOperation::Write, impl_->Key(b.page_), impl_->Offset(b.page_), ObjectSizeMode::Fixed, {}};
    op.source_ = ObjectWriteSource{b.data_, BUSTUB_PAGE_SIZE, b.capacity_, b.owner_};
    tx.objects_.push_back(std::move(op));
  }
  Submit(*impl_->storage_, tx);
}
void ObjectPageStorage::Delete(page_id_t page) {
  // Legacy index/cache deletion has no durable parent-change proof. It must not
  // release storage. F36 uses the explicit retirement protocol instead.
  (void)page;
}
void ObjectPageStorage::RetirePage(page_id_t page, page_id_t link,
                                   const std::array<char, BUSTUB_PAGE_SIZE> &replacement) {
  std::lock_guard<std::mutex> lock(impl_->allocation_);
  impl_->Writable();
  impl_->CheckPage(page);
  impl_->CheckPage(link);
  ObjectMutation write{ObjectOperation::Write, impl_->Key(link), impl_->Offset(link), ObjectSizeMode::Fixed, {}};
  write.bytes_.resize(BUSTUB_PAGE_SIZE);
  std::memcpy(write.bytes_.data(), replacement.data(), replacement.size());
  ObjectTransaction tx;
  tx.objects_.push_back(std::move(write));
  tx.controls_.push_back({{impl_->space_, 0},
                          uint64_t{static_cast<uint32_t>(page)} + 1,
                          std::vector<std::byte>{std::byte{1}, std::byte{1}}});
  impl_->pending_pages_.insert(page);  // Reserve publication memory before durable work.
  try {
    Submit(*impl_->storage_, tx);  // A: link replacement and pending retirement are one commit.
  } catch (...) {
    impl_->pending_pages_.erase(page);
    throw;  // Caller fences the cache; reopening resolves an indeterminate result.
  }
}
void ObjectPageStorage::ReclaimRetiredPages() {
  std::lock_guard<std::mutex> lock(impl_->allocation_);
  impl_->Reclaim(4);  // B: hole + persistent old-ID rejection, bounded per maintenance call.
}
void ObjectPageStorage::Retire() {
  std::lock_guard<std::mutex> lock(impl_->allocation_);
  if (impl_->retired_) {
    return;
  }
  // Object zero is the immutable business manifest, not a database page object.
  // A crash before root publication leaves it in this registered candidate.
  bool manifest = false;
  try {
    impl_->storage_->Objects().Describe({impl_->space_, 0});
    manifest = true;
  } catch (const ObjectMappingError &e) {
    if (e.Code() != ObjectMappingErrorCode::NotFound) throw;
  }
  if (manifest) {
    const ObjectKey key{impl_->space_, 0};
    for (;;) {
      const auto view = impl_->storage_->Objects();
      if (view.Describe(key).size_ == 0) break;
      ObjectTransaction trim{{{ObjectOperation::Resize,
                               key,
                               view.PlanTailTrim(key, impl_->caps_.max_write_bytes_),
                               ObjectSizeMode::Variable,
                               {}}},
                             {}};
      Submit(*impl_->storage_, trim);
    }
    ObjectTransaction remove{{{ObjectOperation::Remove, key, 0, ObjectSizeMode::Variable, {}}}, {}};
    Submit(*impl_->storage_, remove);
  }
  while (!impl_->pending_pages_.empty()) {
    impl_->Reclaim(4);
  }
  while (impl_->objects_ != 0) {
    const auto last = impl_->objects_;
    const ObjectKey key{impl_->space_, last};
    for (;;) {
      const auto view = impl_->storage_->Objects();
      if (view.Describe(key).size_ == 0) {
        break;
      }
      const auto length = view.PlanTailTrim(key, impl_->caps_.max_write_bytes_);
      ObjectTransaction trim;
      trim.objects_.push_back({ObjectOperation::Resize, key, length, ObjectSizeMode::Fixed, {}});
      Submit(*impl_->storage_, trim);
    }
    ObjectTransaction tx;
    tx.objects_.push_back({ObjectOperation::Remove, {impl_->space_, last}, 0, ObjectSizeMode::Fixed, {}});
    tx.controls_.push_back({{impl_->space_, 0}, 0, Descriptor(impl_->options_, last - 1)});
    Submit(*impl_->storage_, tx);
    --impl_->objects_;
  }
  for (;;) {
    const auto records = impl_->storage_->Objects().Controls({impl_->space_, 0}, 1, 1);
    if (records.empty()) {
      break;
    }
    ObjectTransaction erase{{}, {{{impl_->space_, 0}, records.front().item_, std::nullopt}}};
    Submit(*impl_->storage_, erase);
  }
  ObjectTransaction tx{
      {}, {{{impl_->space_, 0}, 0, std::nullopt}, {impl_->options_.registry_, impl_->space_, std::nullopt}}};
  Submit(*impl_->storage_, tx);
  impl_->retired_ = true;
}
}  // namespace bustub
