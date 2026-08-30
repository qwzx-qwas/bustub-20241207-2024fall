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
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string_view>
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
#include "storage/disk/disk_manager.h"

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

auto ReadExact(DurableStorage *storage, const DurableFileSlice &slice, size_t maximum_size, std::string_view name)
    -> std::vector<std::byte> {
  if (slice.size_ > maximum_size) {
    throw std::runtime_error(std::string(name) + " exceeds its limit");
  }
  auto bytes = storage->ReadFileRange(slice.path_, slice.offset_, static_cast<size_t>(slice.size_));
  if (bytes.size() != slice.size_) {
    throw std::runtime_error(std::string(name) + " was truncated");
  }
  return bytes;
}

auto ChecksumSlice(DurableStorage *storage, const DurableFileSlice &slice, uint32_t initial = 0) -> uint32_t {
  uint32_t checksum = initial;
  uint64_t consumed = 0;
  while (consumed < slice.size_) {
    constexpr size_t chunk_size = 1U * 1024U * 1024U;
    const auto request = static_cast<size_t>(std::min<uint64_t>(chunk_size, slice.size_ - consumed));
    const auto chunk = storage->ReadFileRange(slice.path_, slice.offset_ + consumed, request);
    if (chunk.size() != request) {
      throw std::runtime_error("snapshot bundle was truncated during streaming checksum");
    }
    checksum = Crc32cExtend(checksum, chunk.data(), chunk.size());
    consumed += chunk.size();
  }
  return checksum;
}

auto AppendSlice(DurableStorage *storage, const DurableFileSlice &slice, const std::filesystem::path &output,
                 uint32_t initial_checksum) -> uint32_t {
  uint32_t checksum = initial_checksum;
  uint64_t consumed = 0;
  while (consumed < slice.size_) {
    constexpr size_t chunk_size = 1U * 1024U * 1024U;
    const auto request = static_cast<size_t>(std::min<uint64_t>(chunk_size, slice.size_ - consumed));
    const auto chunk = storage->ReadFileRange(slice.path_, slice.offset_ + consumed, request);
    if (chunk.size() != request) {
      throw std::runtime_error("canonical snapshot file was truncated while bundling");
    }
    storage->AppendFile(output, chunk);
    checksum = Crc32cExtend(checksum, chunk.data(), chunk.size());
    consumed += chunk.size();
  }
  return checksum;
}

void CopySlice(DurableStorage *storage, const DurableFileSlice &slice, const std::filesystem::path &output) {
  storage->WriteFile(output, {});
  static_cast<void>(AppendSlice(storage, slice, output, 0));
}

}  // namespace

struct BusTubRaftStateMachine::WorkingState {
  std::unique_ptr<DiskManager> disk_manager_;
  std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<SessionTable> sessions_;
  std::unique_ptr<TransactionManager> transaction_manager_;
  std::unique_ptr<ExecutionEngine> execution_engine_;
};

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

void BusTubSnapshotBundleCodec::EncodeFiles(uint64_t last_included_index, const CanonicalSnapshotPaths &paths,
                                            const std::filesystem::path &output, DurableStorage *storage) {
  if (storage == nullptr || output.empty() || last_included_index >= TXN_START_ID) {
    throw std::runtime_error("invalid streamed BusTub snapshot bundle target");
  }
  const DurableFileSlice database{paths.database_file_, 0, storage->FileSize(paths.database_file_)};
  const DurableFileSlice catalog{paths.catalog_file_, 0, storage->FileSize(paths.catalog_file_)};
  const DurableFileSlice sessions{paths.session_file_, 0, storage->FileSize(paths.session_file_)};
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
  storage->WriteFile(output, prefix.Data());
  uint32_t checksum = Crc32c(body_header.Data());
  checksum = AppendSlice(storage, database, output, checksum);

  ByteWriter catalog_size;
  catalog_size.PutU64(catalog.size_);
  storage->AppendFile(output, catalog_size.Data());
  checksum = Crc32cExtend(checksum, catalog_size.Data().data(), catalog_size.Data().size());
  checksum = AppendSlice(storage, catalog, output, checksum);

  ByteWriter session_size;
  session_size.PutU64(sessions.size_);
  storage->AppendFile(output, session_size.Data());
  checksum = Crc32cExtend(checksum, session_size.Data().data(), session_size.Data().size());
  checksum = AppendSlice(storage, sessions, output, checksum);

  ByteWriter trailer;
  trailer.PutU32(checksum);
  storage->AppendFile(output, trailer.Data());
}

auto BusTubSnapshotBundleCodec::DecodeFile(const DurableFileSlice &payload, DurableStorage *storage)
    -> BusTubSnapshotBundleFileView {
  constexpr uint64_t fixed_bytes = BUNDLE_MAGIC.size() + sizeof(uint32_t) * 2 + sizeof(uint64_t) * 4;
  if (storage == nullptr || payload.size_ < fixed_bytes || payload.size_ > MAX_STREAM_BUNDLE_BYTES) {
    throw std::runtime_error("invalid streamed BusTub snapshot bundle size");
  }
  auto read_u64 = [&](uint64_t offset) {
    const auto bytes = storage->ReadFileRange(payload.path_, payload.offset_ + offset, sizeof(uint64_t));
    if (bytes.size() != sizeof(uint64_t)) {
      throw std::runtime_error("truncated streamed BusTub snapshot bundle length");
    }
    ByteReader reader(bytes);
    return reader.ReadU64();
  };
  const auto first = storage->ReadFileRange(payload.path_, payload.offset_, BUNDLE_MAGIC.size() + 20);
  if (first.size() != BUNDLE_MAGIC.size() + 20) {
    throw std::runtime_error("truncated streamed BusTub snapshot bundle header");
  }
  ByteReader header(first);
  if (header.ReadBytes(BUNDLE_MAGIC.size()) != std::vector<std::byte>(BUNDLE_MAGIC.begin(), BUNDLE_MAGIC.end()) ||
      header.ReadU32() != FORMAT_VERSION) {
    throw std::runtime_error("unsupported streamed BusTub snapshot bundle");
  }
  BusTubSnapshotBundleFileView result;
  result.last_included_index_ = header.ReadU64();
  const auto database_size = header.ReadU64();
  uint64_t cursor = first.size();
  if (database_size > payload.size_ - cursor) {
    throw std::runtime_error("database file exceeds streamed snapshot bundle");
  }
  result.database_ = {payload.path_, payload.offset_ + cursor, database_size};
  cursor += database_size;
  if (payload.size_ - cursor < sizeof(uint64_t)) {
    throw std::runtime_error("streamed snapshot bundle is missing catalog length");
  }
  const auto catalog_size = read_u64(cursor);
  cursor += sizeof(uint64_t);
  if (catalog_size > CatalogSnapshotCodec::MAX_CATALOG_BYTES || catalog_size > payload.size_ - cursor) {
    throw std::runtime_error("catalog file exceeds streamed snapshot bundle");
  }
  result.catalog_ = {payload.path_, payload.offset_ + cursor, catalog_size};
  cursor += catalog_size;
  if (payload.size_ - cursor < sizeof(uint64_t)) {
    throw std::runtime_error("streamed snapshot bundle is missing session length");
  }
  const auto session_size = read_u64(cursor);
  cursor += sizeof(uint64_t);
  if (session_size > 64U * 1024U * 1024U || session_size > payload.size_ - cursor ||
      payload.size_ - cursor - session_size != sizeof(uint32_t)) {
    throw std::runtime_error("session file exceeds streamed snapshot bundle");
  }
  result.sessions_ = {payload.path_, payload.offset_ + cursor, session_size};

  const DurableFileSlice protected_body{payload.path_, payload.offset_ + BUNDLE_MAGIC.size(),
                                        payload.size_ - BUNDLE_MAGIC.size() - sizeof(uint32_t)};
  const auto expected_bytes =
      storage->ReadFileRange(payload.path_, payload.offset_ + payload.size_ - sizeof(uint32_t), sizeof(uint32_t));
  ByteReader expected(expected_bytes);
  if (ChecksumSlice(storage, protected_body) != expected.ReadU32()) {
    throw std::runtime_error("streamed BusTub snapshot bundle checksum mismatch");
  }
  return result;
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
  state->disk_manager_ = std::make_unique<DiskManager>(active_directory_ / "db.bustub");
  state->buffer_pool_manager_ = std::make_unique<BufferPoolManager>(buffer_pool_size_, state->disk_manager_.get());
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
}

auto BusTubRaftStateMachine::LastApplied() const -> uint64_t {
  std::lock_guard lifecycle(lifecycle_mutex_);
  return fsm_->LastApplied();
}

void BusTubRaftStateMachine::CreateSnapshotFile(const std::filesystem::path &path) const {
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
    BusTubSnapshotBundleCodec::EncodeFiles(
        snapshot_index,
        {capture_directory / "db.bustub", capture_directory / "catalog.bin", capture_directory / "session.bin"}, path,
        storage_.get());
    storage_->RemoveTree(capture_directory);
  } catch (...) {
    storage_->RemoveTree(capture_directory);
    throw;
  }
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
  state->disk_manager_ = std::make_unique<DiskManager>(directory / "db.bustub");
  state->buffer_pool_manager_ = std::make_unique<BufferPoolManager>(buffer_pool_size_, state->disk_manager_.get());
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

auto BusTubRaftStateMachine::BuildWorkingState(const BusTubSnapshotBundleFileView &bundle,
                                               const std::filesystem::path &directory)
    -> std::unique_ptr<WorkingState> {
  storage_->RemoveTree(directory);
  storage_->CreateDirectories(directory);
  CopySlice(storage_.get(), bundle.database_, directory / "db.bustub");
  const auto catalog =
      ReadExact(storage_.get(), bundle.catalog_, CatalogSnapshotCodec::MAX_CATALOG_BYTES, "catalog snapshot");
  const auto sessions = ReadExact(storage_.get(), bundle.sessions_, 64U * 1024U * 1024U, "session snapshot");
  return OpenWorkingState(bundle.last_included_index_, catalog, sessions, directory);
}

void BusTubRaftStateMachine::ValidateSnapshotFile(const DurableFileSlice &payload, uint64_t last_included_index) {
  auto bundle = BusTubSnapshotBundleCodec::DecodeFile(payload, storage_.get());
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

void BusTubRaftStateMachine::InstallSnapshotFile(const DurableFileSlice &payload, uint64_t last_included_index) {
  auto bundle = BusTubSnapshotBundleCodec::DecodeFile(payload, storage_.get());
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
  std::unique_ptr<WorkingState> old_state;
  std::filesystem::path old_directory;
  {
    std::lock_guard lifecycle(lifecycle_mutex_);
    auto exclusive = visibility_.LockExclusive();
    old_fsm = std::move(fsm_);
    old_state = std::move(state_);
    old_directory = active_directory_;
    fsm_ = std::move(candidate_fsm);
    state_ = std::move(candidate);
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
