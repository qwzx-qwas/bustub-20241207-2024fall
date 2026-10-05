#include "buffer/page_storage.h"
#include <algorithm>
#include <cstring>
#include <mutex>
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
auto Descriptor(ObjectPageOptions options, uint64_t objects) -> std::vector<std::byte> {
  ByteWriter w;
  w.PutU32(1);
  w.PutU32(BUSTUB_PAGE_SIZE);
  w.PutU32(options.pages_per_object_);
  w.PutU64(objects);
  w.PutU64(options.registry_.space_);
  w.PutU64(options.registry_.number_);
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
  if (r.ReadU32() != 1 || r.ReadU32() != BUSTUB_PAGE_SIZE || r.ReadU32() != options.pages_per_object_) {
    throw std::runtime_error("page workspace geometry mismatch");
  }
  impl->objects_ = r.ReadU64();
  if (r.ReadU64() != options.registry_.space_ || r.ReadU64() != options.registry_.number_) {
    throw std::runtime_error("page workspace registry mismatch");
  }
  if (!r.Empty()) {
    throw std::runtime_error("trailing workspace descriptor bytes");
  }
  return std::shared_ptr<ObjectPageStorage>(new ObjectPageStorage(std::move(impl)));
}
void ObjectPageStorage::RetireAbandoned(std::shared_ptr<NodeStorage> storage, ObjectPageOptions options) {
  for (;;) {
    const auto entries = storage->Objects().Controls(options.registry_, 0, 1);
    if (entries.empty()) {
      return;
    }
    auto workspace = Open(storage, entries.front().item_, options);
    workspace->Retire();
  }
}
auto ObjectPageStorage::Space() const -> uint64_t { return impl_->space_; }
auto ObjectPageStorage::MemoryAlignment() const -> size_t { return impl_->caps_.memory_alignment_; }
auto ObjectPageStorage::MaxBatchPages() const -> size_t { return impl_->caps_.max_batch_pages_; }
auto ObjectPageStorage::WriteDomain(page_id_t page) const -> uint64_t { return impl_->Key(page).number_; }
void ObjectPageStorage::EnsurePages(uint64_t count) {
  std::lock_guard<std::mutex> lock(impl_->allocation_);
  if (impl_->retired_) {
    throw std::runtime_error("page workspace is retired");
  }
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
  return impl_->storage_->PrefetchObjectInto(impl_->storage_->Objects(), impl_->Key(b.page_), impl_->Offset(b.page_),
                                             BUSTUB_PAGE_SIZE, {b.data_, b.capacity_, b.owner_}, std::move(complete));
}
void ObjectPageStorage::Write(const std::vector<PageBuffer> &buffers) {
  ObjectTransaction tx;
  for (const auto &b : buffers) {
    ObjectMutation op{ObjectOperation::Write, impl_->Key(b.page_), impl_->Offset(b.page_), ObjectSizeMode::Fixed, {}};
    op.source_ = ObjectWriteSource{b.data_, BUSTUB_PAGE_SIZE, b.capacity_, b.owner_};
    tx.objects_.push_back(std::move(op));
  }
  Submit(*impl_->storage_, tx);
}
void ObjectPageStorage::Delete(page_id_t page) {
  // No page-id reuse or partial-object reclamation before F36. Database deletion
  // removes residency; whole abandoned workspaces have explicit retirement below.
  (void)page;
}
void ObjectPageStorage::Retire() {
  std::lock_guard<std::mutex> lock(impl_->allocation_);
  if (impl_->retired_) {
    return;
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
  ObjectTransaction tx{
      {}, {{{impl_->space_, 0}, 0, std::nullopt}, {impl_->options_.registry_, impl_->space_, std::nullopt}}};
  Submit(*impl_->storage_, tx);
  impl_->retired_ = true;
}
}  // namespace bustub
