//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// raft_state_machine.cpp
//
//===----------------------------------------------------------------------===//

#include "distributed/raft_state_machine.h"

#include <algorithm>
#include <array>
#include <condition_variable>
#include <iomanip>
#include <limits>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>

#include "binder/binder.h"
#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog_snapshot.h"
#include "common/byte_codec.h"
#include "common/config.h"
#include "concurrency/transaction_manager.h"
#include "distributed/client_protocol.h"
#include "distributed/sql_command_preparer.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "optimizer/optimizer.h"
#include "planner/planner.h"
#include "raft/snapshot_store.h"
#include "recovery/canonical_snapshot.h"
#include "storage/byte_range.h"
#include "storage/disk/disk_manager.h"
#include "storage/disk/node_storage.h"

namespace bustub {
namespace {

constexpr std::array<std::byte, 8> BUNDLE_MAGIC{std::byte{'B'}, std::byte{'S'}, std::byte{'B'}, std::byte{'U'},
                                                std::byte{'N'}, std::byte{'D'}, std::byte{'0'}, std::byte{'1'}};
static_assert(BusTubSnapshotBundleCodec::MAX_STREAM_BUNDLE_BYTES == SnapshotStore::MAX_SNAPSHOT_BYTES);

void PutBlob(ByteWriter *writer, const std::vector<std::byte> &bytes) {
  writer->PutU64(bytes.size());
  writer->PutBytes(bytes);
}

auto ReadBlob(ByteReader *reader) -> std::vector<std::byte> {
  const auto size = reader->ReadU64();
  if (size > BusTubSnapshotBundleCodec::MAX_IN_MEMORY_BUNDLE_BYTES || size > reader->Remaining()) {
    throw std::runtime_error("BusTub snapshot bundle blob exceeds its frame");
  }
  return reader->ReadBytes(static_cast<size_t>(size));
}

auto GenerationName(uint64_t generation) -> std::string {
  std::ostringstream output;
  output << "state-" << std::setw(20) << std::setfill('0') << generation;
  return output.str();
}

auto ReadExact(const SnapshotInput &slice, size_t maximum_size, std::string_view name) -> std::vector<std::byte> {
  if (slice.size_ > maximum_size) {
    throw std::runtime_error(std::string(name) + " exceeds its limit");
  }
  auto bytes = slice.Read(0, static_cast<size_t>(slice.size_));
  if (bytes.size() != slice.size_) {
    throw std::runtime_error(std::string(name) + " was truncated");
  }
  return bytes;
}

auto ChecksumSlice(const SnapshotInput &slice, uint32_t initial = 0) -> uint32_t {
  uint32_t checksum = initial;
  uint64_t consumed = 0;
  while (consumed < slice.size_) {
    constexpr size_t chunk_size = 1U * 1024U * 1024U;
    const auto request = static_cast<size_t>(std::min<uint64_t>(chunk_size, slice.size_ - consumed));
    const auto chunk = slice.Read(consumed, request);
    if (chunk.size() != request) {
      throw std::runtime_error("snapshot bundle was truncated during streaming checksum");
    }
    checksum = Crc32cExtend(checksum, chunk.data(), chunk.size());
    consumed += chunk.size();
  }
  return checksum;
}

auto AppendSlice(const SnapshotInput &slice, const SnapshotAppend &append, uint32_t initial_checksum) -> uint32_t {
  uint32_t checksum = initial_checksum;
  uint64_t consumed = 0;
  while (consumed < slice.size_) {
    constexpr size_t chunk_size = 1U * 1024U * 1024U;
    const auto request = static_cast<size_t>(std::min<uint64_t>(chunk_size, slice.size_ - consumed));
    const auto chunk = slice.Read(consumed, request);
    if (chunk.size() != request) {
      throw std::runtime_error("canonical snapshot file was truncated while bundling");
    }
    append(chunk);
    checksum = Crc32cExtend(checksum, chunk.data(), chunk.size());
    consumed += chunk.size();
  }
  return checksum;
}

void CopySlice(DurableStorage *storage, const SnapshotInput &slice, const std::filesystem::path &output) {
  storage->WriteFile(output, {});
  static_cast<void>(AppendSlice(
      slice, [&](const auto &bytes) { storage->AppendFile(output, bytes); }, 0));
}

}  // namespace

struct BusTubRaftStateMachine::WorkingState {
  std::shared_ptr<ObjectPageStorage> object_pages_;
  std::unique_ptr<DiskManager> disk_manager_;
  std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<SessionTable> sessions_;
  std::unique_ptr<TransactionManager> transaction_manager_;
  std::unique_ptr<ExecutionEngine> execution_engine_;
  ~WorkingState() {
    if (object_pages_) {
      if (buffer_pool_manager_) {
        buffer_pool_manager_->Close();
      }
      try {
        object_pages_->Retire();
      } catch (const std::exception &e) {
        // The persistent registry keeps cleanup resumable on the next Open.
        LOG_ERROR("working page retirement deferred: %s", e.what());
      }
    }
  }
};

struct BusTubRaftStateMachine::CheckpointWorker {
  std::mutex mutex_;
  std::condition_variable changed_;
  bool stop_{false}, busy_{false};
  uint64_t index_{0}, term_{0}, published_{0};
  std::exception_ptr error_;
  std::thread thread_;
  explicit CheckpointWorker(BusTubRaftStateMachine *owner, uint64_t published) : published_(published) {
    thread_ = std::thread([this, owner] {
      std::unique_lock lock(mutex_);
      for (;;) {
        changed_.wait(lock, [&] { return stop_ || busy_; });
        if (stop_ && !busy_) return;
        const auto index = index_, term = term_;
        lock.unlock();
        std::exception_ptr error;
        try {
          owner->BuildLocalCheckpoint(index, term);
        } catch (...) {
          error = std::current_exception();
        }
        lock.lock();
        error_ = error;
        busy_ = false;
        changed_.notify_all();
      }
    });
  }
  ~CheckpointWorker() {
    {
      std::lock_guard lock(mutex_);
      stop_ = true;
      changed_.notify_all();
    }
    thread_.join();
  }
};

namespace {
constexpr std::array<std::byte, 8> CHECKPOINT_MAGIC{std::byte{'B'}, std::byte{'S'}, std::byte{'A'}, std::byte{'C'},
                                                    std::byte{'K'}, std::byte{'P'}, std::byte{'0'}, std::byte{'1'}};
constexpr size_t CHECKPOINT_LIMIT = 128U * 1024U * 1024U + 64;
struct BusinessCheckpoint {
  StateMachineRecoveryPoint point_;
  std::vector<std::byte> catalog_, sessions_;
};
void SubmitCheckpoint(NodeStorage &storage, ObjectTransaction &tx) {
  for (;;) {
    auto submission = storage.SubmitObjects(tx);
    if (submission.admission_ == IOAdmission::Full) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }
    if (!submission.ticket_) throw std::runtime_error("checkpoint storage stopped");
    submission.ticket_->Wait();
    const auto &r = submission.ticket_->Result();
    if (r.outcome_ != JournalOutcome::Durable) {
      if (r.error_) std::rethrow_exception(r.error_);
      throw std::runtime_error("checkpoint publication is not durable");
    }
    return;
  }
}
auto CheckpointSpace(NodeStorage &storage, ObjectKey registry) -> uint64_t {
  const auto root = storage.Objects().Control(registry, 0);
  if (!root) return 0;
  ByteReader r(*root);
  const auto space = r.ReadU64();
  if (space == 0 || !r.Empty()) throw std::runtime_error("invalid business checkpoint root");
  return space;
}
auto ReadBusinessCheckpoint(NodeStorage &storage, uint64_t space) -> BusinessCheckpoint {
  const ObjectKey key{space, 0};
  const auto size = storage.Objects().Describe(key).size_;
  if (size > CHECKPOINT_LIMIT) throw std::runtime_error("business checkpoint manifest exceeds limit");
  std::vector<std::byte> bytes(size);
  for (uint64_t offset = 0; offset < size;) {
    const auto take = std::min<uint64_t>(storage.PageIO().max_read_bytes_, size - offset);
    auto read = storage.ReadObject(storage.Objects(), key, offset, take);
    read.Wait();
    if (read.Size() != take) throw std::runtime_error("short business checkpoint manifest");
    read.CopyTo(bytes.data() + offset, take);
    offset += take;
  }
  const auto body = DecodeChecksummedFrame(CHECKPOINT_MAGIC.data(), CHECKPOINT_MAGIC.size(), bytes, CHECKPOINT_LIMIT,
                                           "business checkpoint");
  ByteReader r(body);
  BusinessCheckpoint result{{r.ReadU64(), r.ReadU64()}, {}, {}};
  const auto catalog_size = r.ReadU64();
  result.catalog_ = r.ReadBytes(catalog_size);
  const auto session_size = r.ReadU64();
  result.sessions_ = r.ReadBytes(session_size);
  if (!r.Empty() || result.point_.index_ == 0 || result.point_.index_ >= TXN_START_ID || result.point_.term_ == 0) {
    throw std::runtime_error("invalid business checkpoint boundary");
  }
  return result;
}
}  // namespace

BusTubRaftStateMachine::~BusTubRaftStateMachine() {
  if (checkpoint_) {
    std::unique_lock lock(checkpoint_->mutex_);
    checkpoint_->changed_.wait(lock, [&] { return !checkpoint_->busy_; });
  }
  checkpoint_.reset();
}
auto BusTubRaftStateMachine::LocalRecoveryPoint() const -> std::optional<StateMachineRecoveryPoint> {
  std::lock_guard lock(lifecycle_mutex_);
  return recovered_point_;
}
auto BusTubRaftStateMachine::RequestCheckpoint(uint64_t index, uint64_t term) -> bool {
  if (!checkpoint_ || index == 0) return false;
  std::lock_guard lock(checkpoint_->mutex_);
  if (checkpoint_->error_) std::rethrow_exception(checkpoint_->error_);
  if (checkpoint_->busy_ || index <= checkpoint_->published_) return false;
  checkpoint_->index_ = index;
  checkpoint_->term_ = term;
  checkpoint_->busy_ = true;
  checkpoint_->changed_.notify_all();
  return true;
}
auto BusTubRaftStateMachine::PollCheckpoint() -> uint64_t {
  if (!checkpoint_) return 0;
  std::lock_guard lock(checkpoint_->mutex_);
  if (checkpoint_->error_) std::rethrow_exception(checkpoint_->error_);
  return checkpoint_->published_;
}
void BusTubRaftStateMachine::DrainCheckpoint() {
  if (!checkpoint_) return;
  std::unique_lock lock(checkpoint_->mutex_);
  checkpoint_->changed_.wait(lock, [&] { return !checkpoint_->busy_; });
  if (checkpoint_->error_) std::rethrow_exception(checkpoint_->error_);
}
void BusTubRaftStateMachine::OpenLocalCheckpoint() {
  auto &deployment = *page_deployment_;
  const auto space = CheckpointSpace(*deployment.storage_, deployment.pages_.registry_);
  if (space == 0) {
    ObjectPageStorage::RetireAbandoned(deployment.storage_, deployment.pages_);
    InitializeEmpty();
    return;
  }
  storage_->RemoveTree(runtime_directory_);
  storage_->CreateDirectories(runtime_directory_);
  active_directory_ = runtime_directory_ / GenerationName(next_generation_++);
  storage_->CreateDirectories(active_directory_);
  const auto record = ReadBusinessCheckpoint(*deployment.storage_, space);
  auto frozen = ObjectPageStorage::Open(deployment.storage_, space, deployment.pages_);
  const auto count = frozen->SealedPageCount();
  auto capture = frozen->Capture(count, std::nullopt);
  auto state = std::make_shared<WorkingState>();
  state->object_pages_ = ObjectPageStorage::Clone(deployment.storage_, capture);
  state->buffer_pool_manager_ =
      std::make_unique<BufferPoolManager>(buffer_pool_size_, state->object_pages_, deployment.cache_);
  if (count > INT32_MAX) throw std::runtime_error("checkpoint has exhausted page identities");
  state->buffer_pool_manager_->SetNextPageIdForRecovery(static_cast<page_id_t>(count));
  const auto catalog = CatalogSnapshotCodec::Decode(record.catalog_);
  ValidateReplicatedCatalogV1(catalog);
  state->sessions_ = std::make_unique<SessionTable>();
  SessionSnapshotCodec::DecodeInto(record.sessions_, state->sessions_.get());
  state->sessions_->ValidateSnapshotBoundary(record.point_.index_);
  state->catalog_ = std::make_unique<Catalog>(state->buffer_pool_manager_.get(), nullptr, nullptr);
  CatalogSnapshotCodec::Restore(catalog, state->catalog_.get(), state->buffer_pool_manager_.get(), nullptr);
  for (const auto &name : state->catalog_->GetTableNames()) {
    for (auto it = state->catalog_->GetTable(name)->table_->MakeIterator(); !it.IsEnd(); ++it) {
      const auto meta = it.GetTuple().first;
      if (meta.ts_ < 0 || static_cast<uint64_t>(meta.ts_) > record.point_.index_)
        throw std::runtime_error("checkpoint contains a row beyond its business boundary");
    }
  }
  state->transaction_manager_ = std::make_unique<TransactionManager>();
  state->transaction_manager_->catalog_ = state->catalog_.get();
  state->execution_engine_ = std::make_unique<ExecutionEngine>(
      state->buffer_pool_manager_.get(), state->transaction_manager_.get(), state->catalog_.get());
  state_ = std::move(state);
  fsm_ = std::make_unique<BusTubStateMachine>(state_->catalog_.get(), state_->sessions_.get(), &visibility_,
                                              record.point_.index_);
  recovered_point_ = record.point_;
  applied_term_ = record.point_.term_;
  ObjectPageStorage::RetireAbandoned(deployment.storage_, deployment.pages_, {space, state_->object_pages_->Space()});
}
void BusTubRaftStateMachine::BuildLocalCheckpoint(uint64_t requested_index, uint64_t requested_term) {
  std::shared_ptr<WorkingState> state;
  {
    std::lock_guard lock(lifecycle_mutex_);
    state = state_;
  }
  // WiredTiger-style preflush: this worker waits, the protocol/caller does not.
  state->buffer_pool_manager_->FlushDirtyPages(state->object_pages_->MaxBatchPages());
  BusinessCheckpoint record;
  std::optional<PageCapture> pages;
  std::optional<ObjectPageCapture> capture;
  {
    std::lock_guard lock(lifecycle_mutex_);
    if (state != state_) return;  // An installed snapshot superseded this candidate.
    auto exclusive = visibility_.LockExclusive();
    const auto index = fsm_->LastApplied();
    const auto term = index == requested_index ? requested_term : applied_term_;
    if (index < requested_index || term == 0) throw std::runtime_error("checkpoint request lacks its applied term");
    record.point_ = {index, term};
    state->sessions_->ValidateSnapshotBoundary(index);
    record.catalog_ = CatalogSnapshotCodec::Encode(CatalogSnapshotCodec::Capture(*state->catalog_));
    record.sessions_ = SessionSnapshotCodec::Encode(*state->sessions_);
    std::vector<page_id_t> selected;
    for (const auto &name : state->catalog_->GetTableNames()) {
      const auto limit = page_deployment_->cache_.arena_bytes_ / (2 * sizeof(page_id_t));
      if (selected.size() > limit) throw std::runtime_error("checkpoint page directory budget exceeded");
      const auto ids = state->catalog_->GetTable(name)->table_->CheckpointPages(limit - selected.size());
      selected.insert(selected.end(), ids.begin(), ids.end());
    }
    pages.emplace(state->buffer_pool_manager_->CapturePages(selected, page_deployment_->cache_.arena_bytes_));
    capture.emplace(state->object_pages_->Capture(pages->next_page_, std::move(selected)));
  }
  auto &deployment = *page_deployment_;
  auto candidate = ObjectPageStorage::Clone(deployment.storage_, *capture);
  capture.reset();  // Durable sharing now owns all captured persistent content.
  ObjectTransaction tx;
  std::set<uint64_t> domains;
  auto flush = [&] {
    if (!tx.objects_.empty()) SubmitCheckpoint(*deployment.storage_, tx);
    tx = {};
    domains.clear();
  };
  for (const auto &page : pages->dirty_) {
    const auto object = 1 + static_cast<uint64_t>(page.page_) / deployment.pages_.pages_per_object_;
    if (domains.count(object) || tx.objects_.size() == candidate->MaxBatchPages()) flush();
    domains.insert(object);
    ObjectMutation write{ObjectOperation::Write,
                         {candidate->Space(), object},
                         (page.page_ % deployment.pages_.pages_per_object_) * uint64_t{BUSTUB_PAGE_SIZE},
                         ObjectSizeMode::Fixed,
                         {}};
    const auto *begin = reinterpret_cast<const std::byte *>(page.bytes_.data());
    write.bytes_.assign(begin, begin + page.bytes_.size());
    tx.objects_.push_back(std::move(write));
  }
  flush();
  candidate->Seal(pages->next_page_);
  pages.reset();
  ByteWriter body;
  body.PutU64(record.point_.index_);
  body.PutU64(record.point_.term_);
  PutBlob(&body, record.catalog_);
  PutBlob(&body, record.sessions_);
  const auto bytes = EncodeChecksummedFrame(CHECKPOINT_MAGIC.data(), CHECKPOINT_MAGIC.size(), body.Data(),
                                            CHECKPOINT_LIMIT, "business checkpoint");
  const ObjectKey manifest{candidate->Space(), 0};
  tx.objects_.push_back({ObjectOperation::Create, manifest, 0, ObjectSizeMode::Variable, {}});
  SubmitCheckpoint(*deployment.storage_, tx);
  for (size_t cursor = 0; cursor < bytes.size();) {
    const auto take = std::min<uint64_t>(deployment.storage_->PageIO().max_write_bytes_, bytes.size() - cursor);
    tx = {};
    ObjectMutation write{ObjectOperation::Append, manifest, 0, ObjectSizeMode::Variable, {}};
    write.bytes_.assign(bytes.begin() + cursor, bytes.begin() + cursor + take);
    tx.objects_.push_back(std::move(write));
    SubmitCheckpoint(*deployment.storage_, tx);
    cursor += take;
  }
  const auto previous = CheckpointSpace(*deployment.storage_, deployment.pages_.registry_);
  ByteWriter root;
  root.PutU64(candidate->Space());
  tx = {{}, {{deployment.pages_.registry_, 0, root.Take()}}};
  SubmitCheckpoint(*deployment.storage_, tx);
  {
    std::lock_guard lock(checkpoint_->mutex_);
    checkpoint_->published_ = record.point_.index_;
  }
  if (previous != 0) ObjectPageStorage::Open(deployment.storage_, previous, deployment.pages_)->Retire();
}

auto BusTubSnapshotBundleCodec::Encode(const BusTubSnapshotBundleV1 &bundle) -> std::vector<std::byte> {
  ByteWriter body;
  body.PutU32(FORMAT_VERSION);
  body.PutU64(bundle.last_included_index_);
  PutBlob(&body, bundle.database_);
  PutBlob(&body, bundle.catalog_);
  PutBlob(&body, bundle.sessions_);
  if (body.Data().size() > MAX_IN_MEMORY_BUNDLE_BYTES - BUNDLE_MAGIC.size() - sizeof(uint32_t)) {
    throw std::runtime_error("BusTub snapshot bundle exceeds its in-memory compatibility limit");
  }
  return EncodeChecksummedFrame(BUNDLE_MAGIC.data(), BUNDLE_MAGIC.size(), body.Data(),
                                MAX_IN_MEMORY_BUNDLE_BYTES - BUNDLE_MAGIC.size() - sizeof(uint32_t),
                                "BusTub snapshot bundle");
}

auto BusTubSnapshotBundleCodec::Decode(const std::vector<std::byte> &bytes) -> BusTubSnapshotBundleV1 {
  if (bytes.size() < BUNDLE_MAGIC.size() + sizeof(uint32_t) * 2 + sizeof(uint64_t) * 4 ||
      bytes.size() > MAX_IN_MEMORY_BUNDLE_BYTES) {
    throw std::runtime_error("invalid BusTub snapshot bundle size");
  }
  const auto body_bytes = DecodeChecksummedFrame(BUNDLE_MAGIC.data(), BUNDLE_MAGIC.size(), bytes,
                                                 MAX_IN_MEMORY_BUNDLE_BYTES - BUNDLE_MAGIC.size() - sizeof(uint32_t),
                                                 "BusTub snapshot bundle");
  ByteReader body(body_bytes);
  if (body.ReadU32() != FORMAT_VERSION) {
    throw std::runtime_error("unsupported BusTub snapshot bundle version");
  }
  BusTubSnapshotBundleV1 bundle;
  bundle.last_included_index_ = body.ReadU64();
  bundle.database_ = ReadBlob(&body);
  bundle.catalog_ = ReadBlob(&body);
  bundle.sessions_ = ReadBlob(&body);
  if (!body.Empty() || Encode(bundle) != bytes) {
    throw std::runtime_error("non-canonical BusTub snapshot bundle");
  }
  return bundle;
}

void BusTubSnapshotBundleCodec::Write(uint64_t last_included_index, const CanonicalSnapshotPaths &paths,
                                      const SnapshotAppend &append, DurableStorage *storage) {
  if (storage == nullptr || !append || last_included_index >= TXN_START_ID) {
    throw std::runtime_error("invalid streamed BusTub snapshot bundle target");
  }
  const auto database = FileSnapshotInput({paths.database_file_, 0, storage->FileSize(paths.database_file_)},
                                          std::shared_ptr<DurableStorage>(storage, [](DurableStorage *) {}));
  const auto catalog = FileSnapshotInput({paths.catalog_file_, 0, storage->FileSize(paths.catalog_file_)},
                                         std::shared_ptr<DurableStorage>(storage, [](DurableStorage *) {}));
  const auto sessions = FileSnapshotInput({paths.session_file_, 0, storage->FileSize(paths.session_file_)},
                                          std::shared_ptr<DurableStorage>(storage, [](DurableStorage *) {}));
  const auto framing_bytes = BUNDLE_MAGIC.size() + sizeof(uint32_t) * 2 + sizeof(uint64_t) * 4;
  if (database.size_ > MAX_STREAM_BUNDLE_BYTES || catalog.size_ > CatalogSnapshotCodec::MAX_CATALOG_BYTES ||
      sessions.size_ > 64U * 1024U * 1024U ||
      database.size_ + catalog.size_ + sessions.size_ > MAX_STREAM_BUNDLE_BYTES - framing_bytes) {
    throw std::runtime_error("streamed BusTub snapshot bundle exceeds its limit");
  }

  ByteWriter prefix;
  prefix.PutBytes(BUNDLE_MAGIC.data(), BUNDLE_MAGIC.size());
  ByteWriter body_header;
  body_header.PutU32(FORMAT_VERSION);
  body_header.PutU64(last_included_index);
  body_header.PutU64(database.size_);
  prefix.PutBytes(body_header.Data());
  append(prefix.Data());
  uint32_t checksum = Crc32c(body_header.Data());
  checksum = AppendSlice(database, append, checksum);

  ByteWriter catalog_size;
  catalog_size.PutU64(catalog.size_);
  append(catalog_size.Data());
  checksum = Crc32cExtend(checksum, catalog_size.Data().data(), catalog_size.Data().size());
  checksum = AppendSlice(catalog, append, checksum);

  ByteWriter session_size;
  session_size.PutU64(sessions.size_);
  append(session_size.Data());
  checksum = Crc32cExtend(checksum, session_size.Data().data(), session_size.Data().size());
  checksum = AppendSlice(sessions, append, checksum);

  ByteWriter trailer;
  trailer.PutU32(checksum);
  append(trailer.Data());
}

auto BusTubSnapshotBundleCodec::Read(const SnapshotInput &payload) -> BusTubSnapshotBundleView {
  constexpr uint64_t fixed_bytes = BUNDLE_MAGIC.size() + sizeof(uint32_t) * 2 + sizeof(uint64_t) * 4;
  const auto payload_range = StorageByteRange::Create(payload.offset_, payload.size_);
  if (!payload_range.has_value() || payload.size_ < fixed_bytes || payload.size_ > MAX_STREAM_BUNDLE_BYTES) {
    throw std::runtime_error("invalid streamed BusTub snapshot bundle size");
  }
  const auto slice_at = [&](uint64_t offset, uint64_t size) -> SnapshotInput {
    const auto range = payload_range->Subrange(offset, size);
    if (!range.has_value()) {
      throw std::runtime_error("file range exceeds streamed BusTub snapshot bundle");
    }
    return payload.Slice(offset, size);
  };
  auto read_u64 = [&](uint64_t offset) {
    const auto bytes = ReadExact(slice_at(offset, sizeof(uint64_t)), sizeof(uint64_t), "snapshot bundle length");
    ByteReader reader(bytes);
    return reader.ReadU64();
  };
  const auto first =
      ReadExact(slice_at(0, BUNDLE_MAGIC.size() + 20), BUNDLE_MAGIC.size() + 20, "snapshot bundle header");
  ByteReader header(first);
  if (header.ReadBytes(BUNDLE_MAGIC.size()) != std::vector<std::byte>(BUNDLE_MAGIC.begin(), BUNDLE_MAGIC.end()) ||
      header.ReadU32() != FORMAT_VERSION) {
    throw std::runtime_error("unsupported streamed BusTub snapshot bundle");
  }
  BusTubSnapshotBundleView result{};
  result.last_included_index_ = header.ReadU64();
  const auto database_size = header.ReadU64();
  uint64_t cursor = first.size();
  result.database_ = slice_at(cursor, database_size);
  cursor += database_size;
  const auto catalog_size = read_u64(cursor);
  cursor += sizeof(uint64_t);
  if (catalog_size > CatalogSnapshotCodec::MAX_CATALOG_BYTES) {
    throw std::runtime_error("catalog file exceeds streamed snapshot bundle");
  }
  result.catalog_ = slice_at(cursor, catalog_size);
  cursor += catalog_size;
  const auto session_size = read_u64(cursor);
  cursor += sizeof(uint64_t);
  if (session_size > 64U * 1024U * 1024U) {
    throw std::runtime_error("session file exceeds streamed snapshot bundle");
  }
  result.sessions_ = slice_at(cursor, session_size);
  cursor += session_size;
  if (payload.size_ - cursor != sizeof(uint32_t)) {
    throw std::runtime_error("session file exceeds streamed snapshot bundle");
  }

  const auto protected_body = slice_at(BUNDLE_MAGIC.size(), payload.size_ - BUNDLE_MAGIC.size() - sizeof(uint32_t));
  const auto expected_bytes =
      ReadExact(slice_at(cursor, sizeof(uint32_t)), sizeof(uint32_t), "snapshot bundle checksum");
  ByteReader expected(expected_bytes);
  if (ChecksumSlice(protected_body) != expected.ReadU32()) {
    throw std::runtime_error("streamed BusTub snapshot bundle checksum mismatch");
  }
  return result;
}

void BusTubSnapshotBundleCodec::EncodeFiles(uint64_t index, const CanonicalSnapshotPaths &paths,
                                            const std::filesystem::path &output, DurableStorage *storage) {
  storage->WriteFile(output, {});
  Write(
      index, paths, [&](const auto &bytes) { storage->AppendFile(output, bytes); }, storage);
}
auto BusTubSnapshotBundleCodec::DecodeFile(const DurableFileSlice &payload, DurableStorage *storage)
    -> BusTubSnapshotBundleFileView {
  auto view = Read(FileSnapshotInput(payload, std::shared_ptr<DurableStorage>(storage, [](DurableStorage *) {})));
  return {view.last_included_index_,
          {payload.path_, view.database_.offset_, view.database_.size_},
          {payload.path_, view.catalog_.offset_, view.catalog_.size_},
          {payload.path_, view.sessions_.offset_, view.sessions_.size_}};
}

auto BusTubRaftStateMachine::Open(NodeDirectory *node_directory, std::shared_ptr<DurableStorage> storage,
                                  size_t buffer_pool_size) -> std::shared_ptr<BusTubRaftStateMachine> {
  if (node_directory == nullptr || buffer_pool_size == 0) {
    throw std::runtime_error("invalid BusTub Raft state-machine configuration");
  }
  if (storage == nullptr) {
    storage = std::make_shared<PosixDurableStorage>();
  }
  auto result = std::shared_ptr<BusTubRaftStateMachine>(
      new BusTubRaftStateMachine(node_directory, std::move(storage), buffer_pool_size));
  result->InitializeEmpty();
  return result;
}

auto BusTubRaftStateMachine::OpenObjectPages(NodeDirectory *directory, std::shared_ptr<DurableStorage> storage,
                                             size_t frames, ObjectPageDeployment deployment)
    -> std::shared_ptr<BusTubRaftStateMachine> {
  if (!directory || !storage || frames == 0 || !deployment.storage_) {
    throw std::invalid_argument("invalid object page deployment");
  }
  auto result =
      std::shared_ptr<BusTubRaftStateMachine>(new BusTubRaftStateMachine(directory, std::move(storage), frames));
  result->page_deployment_ = std::move(deployment);
  if (result->page_deployment_->storage_->SupportsObjectSharing()) {
    result->OpenLocalCheckpoint();
    result->checkpoint_ = std::make_unique<CheckpointWorker>(
        result.get(), result->recovered_point_ ? result->recovered_point_->index_ : 0);
  } else {
    ObjectPageStorage::RetireAbandoned(result->page_deployment_->storage_, result->page_deployment_->pages_);
    result->InitializeEmpty();
  }
  return result;
}

BusTubRaftStateMachine::BusTubRaftStateMachine(NodeDirectory *node_directory, std::shared_ptr<DurableStorage> storage,
                                               size_t buffer_pool_size)
    : node_directory_(node_directory),
      storage_(std::move(storage)),
      buffer_pool_size_(buffer_pool_size),
      runtime_directory_(node_directory_->WorkingDirectory() / "bustub-raft-fsm") {}

void BusTubRaftStateMachine::InitializeEmpty() {
  storage_->RemoveTree(runtime_directory_);
  storage_->CreateDirectories(runtime_directory_);
  active_directory_ = runtime_directory_ / GenerationName(next_generation_++);
  storage_->CreateDirectories(active_directory_);
  auto state = std::make_unique<WorkingState>();
  if (page_deployment_) {
    state->object_pages_ = ObjectPageStorage::Create(page_deployment_->storage_, page_deployment_->pages_);
    state->buffer_pool_manager_ =
        std::make_unique<BufferPoolManager>(buffer_pool_size_, state->object_pages_, page_deployment_->cache_);
  } else {
    state->disk_manager_ = std::make_unique<DiskManager>(active_directory_ / "db.bustub");
    state->buffer_pool_manager_ = std::make_unique<BufferPoolManager>(buffer_pool_size_, state->disk_manager_.get());
  }
  state->catalog_ = std::make_unique<Catalog>(state->buffer_pool_manager_.get(), nullptr, nullptr);
  state->sessions_ = std::make_unique<SessionTable>();
  state->transaction_manager_ = std::make_unique<TransactionManager>();
  state->transaction_manager_->catalog_ = state->catalog_.get();
  state->execution_engine_ = std::make_unique<ExecutionEngine>(
      state->buffer_pool_manager_.get(), state->transaction_manager_.get(), state->catalog_.get());
  fsm_ = std::make_unique<BusTubStateMachine>(state->catalog_.get(), state->sessions_.get(), &visibility_, 0);
  state_ = std::move(state);
}

void BusTubRaftStateMachine::ValidateProposalPayload(EntryType type, const std::vector<std::byte> &payload) const {
  if (type != EntryType::COMMAND_BATCH) {
    throw std::runtime_error("unsupported proposal type for BusTub state machine");
  }
  ValidateProposal(CommandBatchCodec::Decode(payload));
}

void BusTubRaftStateMachine::Apply(const ReplicatedLogEntry &entry) {
  std::lock_guard lifecycle(lifecycle_mutex_);
  fsm_->Apply(entry);
  applied_term_ = entry.term_;
}

auto BusTubRaftStateMachine::LastApplied() const -> uint64_t {
  std::lock_guard lifecycle(lifecycle_mutex_);
  return fsm_->LastApplied();
}

void BusTubRaftStateMachine::WriteSnapshot(const SnapshotAppend &append) const {
  std::filesystem::path capture_directory;
  uint64_t snapshot_index = 0;
  {
    std::lock_guard lifecycle(lifecycle_mutex_);
    snapshot_index = fsm_->LastApplied();
    capture_directory =
        runtime_directory_ / ("capture-" + std::to_string(snapshot_index) + "-" + std::to_string(next_generation_++));
    storage_->RemoveTree(capture_directory);
    auto exclusive = visibility_.LockExclusive();
    state_->sessions_->ValidateSnapshotBoundary(snapshot_index);
    CanonicalSnapshotBuilder::BuildUnsynced(
        *state_->catalog_, *state_->sessions_,
        {capture_directory / "db.bustub", capture_directory / "catalog.bin", capture_directory / "session.bin"},
        storage_.get(), buffer_pool_size_);
  }
  try {
    BusTubSnapshotBundleCodec::Write(
        snapshot_index,
        {capture_directory / "db.bustub", capture_directory / "catalog.bin", capture_directory / "session.bin"}, append,
        storage_.get());
    storage_->RemoveTree(capture_directory);
  } catch (...) {
    storage_->RemoveTree(capture_directory);
    throw;
  }
}

void BusTubRaftStateMachine::CreateSnapshotFile(const std::filesystem::path &path) const {
  storage_->WriteFile(path, {});
  WriteSnapshot([&](const auto &bytes) { storage_->AppendFile(path, bytes); });
}
void BusTubRaftStateMachine::ValidateSnapshotFile(const DurableFileSlice &payload, uint64_t index) {
  ValidateSnapshot(FileSnapshotInput(payload, storage_), index);
}
void BusTubRaftStateMachine::InstallSnapshotFile(const DurableFileSlice &payload, uint64_t index) {
  LoadSnapshot(FileSnapshotInput(payload, storage_), index);
}

auto BusTubRaftStateMachine::OpenWorkingState(uint64_t last_included_index, const std::vector<std::byte> &catalog_bytes,
                                              const std::vector<std::byte> &session_bytes,
                                              const std::filesystem::path &directory) -> std::unique_ptr<WorkingState> {
  const auto catalog_snapshot = CatalogSnapshotCodec::Decode(catalog_bytes);
  ValidateReplicatedCatalogV1(catalog_snapshot);
  auto sessions = std::make_unique<SessionTable>();
  SessionSnapshotCodec::DecodeInto(session_bytes, sessions.get());
  sessions->ValidateSnapshotBoundary(last_included_index);
  auto state = std::make_unique<WorkingState>();
  if (page_deployment_) {
    state->object_pages_ = ObjectPageStorage::Create(page_deployment_->storage_, page_deployment_->pages_);
    state->buffer_pool_manager_ =
        std::make_unique<BufferPoolManager>(buffer_pool_size_, state->object_pages_, page_deployment_->cache_);
    const auto file = directory / "db.bustub";
    const auto size = storage_->FileSize(file);
    if (size % BUSTUB_PAGE_SIZE != 0 || size / BUSTUB_PAGE_SIZE > INT32_MAX) {
      throw std::runtime_error("snapshot database page geometry is invalid");
    }
    state->buffer_pool_manager_->SetNextPageIdForRecovery(size / BUSTUB_PAGE_SIZE);
    for (uint64_t page = 0; page < size / BUSTUB_PAGE_SIZE; ++page) {
      auto guard = state->buffer_pool_manager_->WritePage(page);
      const auto bytes = storage_->ReadFileRange(file, page * BUSTUB_PAGE_SIZE, BUSTUB_PAGE_SIZE);
      if (bytes.size() != BUSTUB_PAGE_SIZE) {
        throw std::runtime_error("short snapshot database page");
      }
      std::memcpy(guard.GetDataMut(), bytes.data(), bytes.size());
    }
  } else {
    state->disk_manager_ = std::make_unique<DiskManager>(directory / "db.bustub");
    state->buffer_pool_manager_ = std::make_unique<BufferPoolManager>(buffer_pool_size_, state->disk_manager_.get());
  }
  state->catalog_ = std::make_unique<Catalog>(state->buffer_pool_manager_.get(), nullptr, nullptr);
  CatalogSnapshotCodec::Restore(catalog_snapshot, state->catalog_.get(), state->buffer_pool_manager_.get(), nullptr);
  for (const auto &table_name : state->catalog_->GetTableNames()) {
    const auto table = state->catalog_->GetTable(table_name);
    for (auto iterator = table->table_->MakeIterator(); !iterator.IsEnd(); ++iterator) {
      const auto [meta, tuple] = iterator.GetTuple();
      static_cast<void>(tuple);
      if (meta.is_deleted_ || meta.ts_ < 0 || static_cast<uint64_t>(meta.ts_) > last_included_index) {
        throw std::runtime_error("BusTub snapshot row timestamp exceeds its included index");
      }
    }
  }
  state->sessions_ = std::move(sessions);
  state->transaction_manager_ = std::make_unique<TransactionManager>();
  state->transaction_manager_->catalog_ = state->catalog_.get();
  state->execution_engine_ = std::make_unique<ExecutionEngine>(
      state->buffer_pool_manager_.get(), state->transaction_manager_.get(), state->catalog_.get());
  return state;
}

auto BusTubRaftStateMachine::BuildWorkingState(const BusTubSnapshotBundleView &bundle,
                                               const std::filesystem::path &directory)
    -> std::unique_ptr<WorkingState> {
  storage_->RemoveTree(directory);
  storage_->CreateDirectories(directory);
  CopySlice(storage_.get(), bundle.database_, directory / "db.bustub");
  const auto catalog = ReadExact(bundle.catalog_, CatalogSnapshotCodec::MAX_CATALOG_BYTES, "catalog snapshot");
  const auto sessions = ReadExact(bundle.sessions_, 64U * 1024U * 1024U, "session snapshot");
  return OpenWorkingState(bundle.last_included_index_, catalog, sessions, directory);
}

void BusTubRaftStateMachine::ValidateSnapshot(const SnapshotInput &payload, uint64_t last_included_index) {
  auto bundle = BusTubSnapshotBundleCodec::Read(payload);
  if (bundle.last_included_index_ != last_included_index || last_included_index >= TXN_START_ID) {
    throw std::runtime_error("BusTub streamed snapshot bundle index mismatch");
  }

  std::filesystem::path candidate_directory;
  {
    std::lock_guard lifecycle(lifecycle_mutex_);
    candidate_directory = runtime_directory_ / GenerationName(next_generation_++);
  }
  try {
    auto candidate = BuildWorkingState(bundle, candidate_directory);
    candidate.reset();
    storage_->RemoveTree(candidate_directory);
  } catch (...) {
    storage_->RemoveTree(candidate_directory);
    throw;
  }
}

void BusTubRaftStateMachine::LoadSnapshot(const SnapshotInput &payload, uint64_t last_included_index) {
  auto bundle = BusTubSnapshotBundleCodec::Read(payload);
  if (bundle.last_included_index_ != last_included_index || last_included_index >= TXN_START_ID) {
    throw std::runtime_error("BusTub streamed snapshot bundle index mismatch");
  }

  std::filesystem::path candidate_directory;
  {
    std::lock_guard lifecycle(lifecycle_mutex_);
    candidate_directory = runtime_directory_ / GenerationName(next_generation_++);
  }
  std::unique_ptr<WorkingState> candidate;
  try {
    candidate = BuildWorkingState(bundle, candidate_directory);
  } catch (...) {
    storage_->RemoveTree(candidate_directory);
    throw;
  }
  auto candidate_fsm = std::make_unique<BusTubStateMachine>(candidate->catalog_.get(), candidate->sessions_.get(),
                                                            &visibility_, last_included_index);

  std::unique_ptr<BusTubStateMachine> old_fsm;
  std::shared_ptr<WorkingState> old_state;
  std::filesystem::path old_directory;
  {
    std::lock_guard lifecycle(lifecycle_mutex_);
    auto exclusive = visibility_.LockExclusive();
    old_fsm = std::move(fsm_);
    old_state = std::move(state_);
    old_directory = active_directory_;
    fsm_ = std::move(candidate_fsm);
    state_ = std::move(candidate);
    recovered_point_.reset();
    applied_term_ = 0;
    active_directory_ = candidate_directory;
  }
  old_fsm.reset();
  old_state.reset();
  if (old_directory != candidate_directory) {
    storage_->RemoveTree(old_directory);
  }
}

auto BusTubRaftStateMachine::PrepareSql(const std::string &sql, uint64_t client_id, uint64_t request_id,
                                        const RequestFingerprintV1 &request_fingerprint) const
    -> TransactionCommandBatch {
  std::lock_guard lifecycle(lifecycle_mutex_);
  auto shared = visibility_.LockShared();
  return SqlCommandPreparer(state_->catalog_.get()).Prepare(sql, client_id, request_id, request_fingerprint);
}

auto BusTubRaftStateMachine::ClassifyRequest(uint64_t client_id, uint64_t request_id,
                                             const RequestFingerprintV1 &request_fingerprint) const
    -> RequestDisposition {
  std::lock_guard lifecycle(lifecycle_mutex_);
  auto shared = visibility_.LockShared();
  return state_->sessions_->Classify(client_id, request_id, request_fingerprint);
}

void BusTubRaftStateMachine::ValidateProposal(const TransactionCommandBatch &batch) const {
  std::lock_guard lifecycle(lifecycle_mutex_);
  fsm_->ValidateProposal(batch);
}

auto BusTubRaftStateMachine::GetRow(table_oid_t table_oid, const EncodedPrimaryKeyV1 &primary_key) const
    -> std::optional<std::pair<TupleMeta, Tuple>> {
  std::lock_guard lifecycle(lifecycle_mutex_);
  return fsm_->GetRow(table_oid, primary_key);
}

auto BusTubRaftStateMachine::GetLastResponse(uint64_t client_id) const -> std::optional<std::vector<std::byte>> {
  std::lock_guard lifecycle(lifecycle_mutex_);
  return fsm_->GetLastResponse(client_id);
}

auto BusTubRaftStateMachine::ExecuteReadSql(const std::string &sql, uint64_t read_timestamp) const
    -> std::vector<std::byte> {
  std::lock_guard lifecycle(lifecycle_mutex_);
  auto shared = visibility_.LockShared();
  if (read_timestamp != fsm_->PublishedAppliedIndex() || read_timestamp >= TXN_START_ID) {
    throw std::runtime_error("read timestamp is not the current published Raft index");
  }

  Binder binder(*state_->catalog_);
  binder.ParseAndSave(sql);
  if (binder.statement_nodes_.size() != 1) {
    throw std::runtime_error("V1 read request must contain exactly one statement");
  }
  auto statement = binder.BindStatement(binder.statement_nodes_.front());
  if (statement->type_ != StatementType::SELECT_STATEMENT) {
    throw std::runtime_error("read endpoint accepts only SELECT");
  }
  Planner planner(*state_->catalog_);
  planner.PlanQuery(*statement);
  Optimizer optimizer(*state_->catalog_, false);
  const auto plan = optimizer.Optimize(planner.plan_);

  auto *transaction = state_->transaction_manager_->BeginReadAt(static_cast<timestamp_t>(read_timestamp));
  try {
    ExecutorContext context(transaction, state_->catalog_.get(), state_->buffer_pool_manager_.get(),
                            state_->transaction_manager_.get(), nullptr, false);
    std::vector<Tuple> tuples;
    if (!state_->execution_engine_->Execute(plan, &tuples, transaction, &context)) {
      throw std::runtime_error("read query execution failed");
    }
    ClientQueryResultV1 result;
    // Execution returns tuples shaped by the optimized root. Optimizer rules may replace a scan and its output
    // layout, so decoding with the pre-optimization planner schema can interpret an inline offset as a varlen length.
    const auto &schema = plan->OutputSchema();
    result.columns_.reserve(schema.GetColumnCount());
    for (const auto &column : schema.GetColumns()) {
      result.columns_.push_back(column.GetName());
    }
    result.rows_.reserve(tuples.size());
    for (const auto &tuple : tuples) {
      std::vector<std::string> row;
      row.reserve(schema.GetColumnCount());
      for (uint32_t column = 0; column < schema.GetColumnCount(); column++) {
        row.push_back(tuple.GetValue(&schema, column).ToString());
      }
      result.rows_.push_back(std::move(row));
    }
    state_->transaction_manager_->EndRead(transaction);
    return ClientQueryResultCodec::Encode(result);
  } catch (...) {
    state_->transaction_manager_->Abort(transaction);
    throw;
  }
}

auto BusTubRaftStateMachine::PublishedAppliedIndex() const -> uint64_t {
  std::lock_guard lifecycle(lifecycle_mutex_);
  return fsm_->PublishedAppliedIndex();
}

auto BusTubRaftStateMachine::CatalogSnapshotForRead() const -> CatalogSnapshot {
  std::lock_guard lifecycle(lifecycle_mutex_);
  auto shared = visibility_.LockShared();
  return CatalogSnapshotCodec::Capture(*state_->catalog_);
}

}  // namespace bustub
