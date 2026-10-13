// Focused final-acceptance contracts. All IO uses existing production components.
#include <unistd.h>
#include <filesystem>
#include <fstream>
#include <optional>
#include <vector>

#include "../../src/storage/disk/object_transaction_internal.h"
#include "../../tools/bustub-node/storage_config.h"
#include "common/byte_codec.h"
#include "distributed/client_protocol.h"
#include "distributed/command.h"
#include "distributed/raft_state_machine.h"
#include "distributed/request_fingerprint.h"
#include "gtest/gtest.h"
#include "raft/log_store.h"
#include "raft/object_storage.h"
#include "raft/persistent_state.h"
#include "raft/snapshot_store.h"
#include "recovery/node_directory.h"
#include "storage/disk/node_storage.h"

namespace bustub {
namespace {
struct DeviceFixture {
  std::filesystem::path root;
  cli::StorageConfig config;
  DeviceFixture()
      : config(cli::ReadStorageConfig(std::filesystem::path(__FILE__).parent_path().parent_path().parent_path() /
                                      "docs/storage_redesign/node-storage.example.conf")) {
    auto pattern = (std::filesystem::temp_directory_path() / "bustub-final-fixes-XXXXXX").string();
    std::vector<char> path(pattern.begin(), pattern.end());
    path.push_back(0);
    const auto *created = mkdtemp(path.data());
    if (!created) throw std::runtime_error("cannot create test workspace");
    root = created;
    config.deployment_.storage_.device_path_ = root / "node.device";
    { std::ofstream out(config.deployment_.storage_.device_path_, std::ios::binary); }
    std::filesystem::resize_file(config.deployment_.storage_.device_path_, config.layout_.capacity_);
    // Deterministic foreground contract tests; background GC is exercised by P3.
    auto &tx = *config.deployment_.storage_.transactions_;
    tx.gc_interval_ = std::chrono::hours(1);
    tx.integrity_scan_ = false;
    tx.metadata_writeback_pages_ = 0;
    tx.metadata_checkpoint_rounds_ = 0;
  }
  ~DeviceFixture() { std::filesystem::remove_all(root); }
};
void Durable(const JournalResult &result) {
  if (result.error_) std::rethrow_exception(result.error_);
  ASSERT_EQ(result.outcome_, JournalOutcome::Durable);
}
auto OpenStorage(DeviceFixture &f) -> std::shared_ptr<NodeStorage> {
  auto storage = std::make_shared<NodeStorage>(f.config.deployment_.storage_);
  storage->Create(f.config.layout_);
  const auto space = storage->CreateObjectSpace(storage->Objects());
  Durable(space.result_);
  return storage;
}
void ApplySql(BusTubRaftStateMachine &machine, uint64_t index, const std::string &sql) {
  const auto command = machine.PrepareSql(sql, 71, index, ComputeWriteIntentFingerprintV1(sql));
  machine.ValidateProposal(command);
  machine.Apply({1, index, 3, EntryType::COMMAND_BATCH, CommandBatchCodec::Encode(command)});
}
}  // namespace

TEST(FinalPerformanceTest, ObjectRecoveryCandidatePreservesSuffixAndRejectsWrongBase) {
  DeviceFixture f;
  auto storage = OpenStorage(f);
  auto objects = RaftObjectStorage::Create(storage, f.config.deployment_.space_, f.config.deployment_.raft_);
  const std::vector<ReplicatedLogEntry> entries{
      {1, 1, 3, EntryType::KV_COMMAND, std::vector<std::byte>(8197, std::byte{0x5a})},
      {1, 2, 4, EntryType::KV_COMMAND, std::vector<std::byte>(113, std::byte{0x93})}};
  auto log = LogStore::OpenObjects(objects, 0, 0, 0);
  log->Append(entries);
  log->AdvanceCommittedIndex(2);
  log.reset();
  auto wrong = LogStore::ProbeObjects(objects, 2);
  EXPECT_THROW(LogStore::OpenObjects(std::move(wrong), 0, 9), std::runtime_error);
  auto stable = StableStore::OpenObjects(objects);
  stable->Update(4, std::nullopt, 2);
  stable.reset();
  // Preparation is independent of the FSM and must not apply anything.
  auto prepared = PrepareRaftStores(objects);
  auto machine = std::make_shared<KvStateMachine>();
  const auto probe = prepared.log_->RecoveryProbe(0, 2);
  EXPECT_EQ(probe.last_log_index_, 2);
  EXPECT_EQ(probe.latest_boundary_term_, 4);
  auto recovered = RecoverRaftPersistentState(objects, machine, std::move(prepared));
  EXPECT_EQ(machine->LastApplied(), 0);
  EXPECT_EQ(recovered.stable_store_->State().commit_index_, 2);
  EXPECT_EQ(recovered.log_store_->Entries(1, 2), entries);
  recovered = {};
  objects.reset();
  storage->Close();
}

TEST(FinalPerformanceTest, CapturedSnapshotSurvivesLaterApplyAndRestoresItsOwnSessionBoundary) {
  DeviceFixture f;
  auto storage = OpenStorage(f);
  auto objects = RaftObjectStorage::Create(storage, f.config.deployment_.space_, f.config.deployment_.raft_);
  auto snapshots = SnapshotStore::OpenObjects(objects);
  auto files = std::make_shared<PosixDurableStorage>();
  auto left_directory = NodeDirectory::Open(f.root / "left", files);
  auto right_directory = NodeDirectory::Open(f.root / "right", files);
  auto cache = f.config.deployment_.pages_->cache_;
  auto left = BusTubRaftStateMachine::OpenObjectPages(left_directory.get(), files, 64,
                                                      {storage, {256, {f.config.deployment_.space_, 5}}, cache});
  auto right = BusTubRaftStateMachine::OpenObjectPages(right_directory.get(), files, 64,
                                                       {storage, {256, {f.config.deployment_.space_, 6}}, cache});
  ApplySql(*left, 1, "CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32));");
  ApplySql(*left, 2, "INSERT INTO accounts VALUES (7, 'before');");
  auto capture = snapshots->PrepareCapture(2, 3, *left);
  ASSERT_TRUE(capture);
  // Deterministic interleaving: do not even begin the expensive build until
  // the active state has changed. No timing sleeps or production test gates.
  ApplySql(*left, 3, "UPDATE accounts SET name = 'after' WHERE id = 7;");
  capture->build_();
  EXPECT_FALSE(snapshots->Latest());  // Complete candidate is not published.
  const auto snapshot = capture->publish_();
  capture.reset();
  right->LoadSnapshot(snapshots->Input(snapshot), 2);
  EXPECT_EQ(ClientQueryResultCodec::Decode(right->ExecuteReadSql("SELECT name FROM accounts;", 2).payload_).rows_,
            (std::vector<std::vector<std::string>>{{"before"}}));
  EXPECT_EQ(ClientQueryResultCodec::Decode(left->ExecuteReadSql("SELECT name FROM accounts;", 3).payload_).rows_,
            (std::vector<std::vector<std::string>>{{"after"}}));
  const auto sql = "INSERT INTO accounts VALUES (7, 'before');";
  EXPECT_EQ(right->ClassifyRequest(71, 2, ComputeWriteIntentFingerprintV1(sql)), RequestDisposition::RETRY_LAST);
  EXPECT_EQ(right->LastApplied(), 2);
  ApplySql(*right, 3, "UPDATE accounts SET name = 'suffix' WHERE id = 7;");
  EXPECT_EQ(ClientQueryResultCodec::Decode(right->ExecuteReadSql("SELECT name FROM accounts;", 3).payload_).rows_,
            (std::vector<std::vector<std::string>>{{"suffix"}}));
  right.reset();
  left.reset();
  snapshots.reset();
  objects.reset();
  storage->Close();
}

TEST(FinalPerformanceTest, GroupedReclaimSharesDiscoveryAndRechecksPins) {
  DeviceFixture f;
  auto options = f.config.deployment_.storage_;
  options.objects_->references_.max_scan_entries_ = 4;
  BlockDevice device(options.device_path_, options.device_);
  IOExecutor executor(device, options.io_);
  BootstrapStore bootstrap(executor);
  bootstrap.Create(f.config.layout_);
  bootstrap.Open(options.identity_);
  MetadataEngine metadata(bootstrap, options.journal_identity_, options.journal_, options.metadata_);
  metadata.Create();
  DataAllocator allocator(metadata, options.objects_->allocator_);
  Durable(allocator.Create());
  ObjectMappingStore mapping(allocator, options.objects_->mapping_);
  Durable(mapping.Create());
  ObjectReferenceManager refs(mapping, options.objects_->references_);
  RegionManager regions(bootstrap);
  ObjectIO io(regions, executor, allocator, mapping, refs, options.objects_->max_read_bytes_,
              options.objects_->max_write_bytes_, options.io_, 0);
  ObjectTransactionPipeline pipeline(io, mapping, refs, *options.transactions_, {});
  const auto space = mapping.CreateSpace(mapping.Read());
  Durable(space.result_);
  const ObjectKey key{*space.space_, 17};
  Durable(mapping.CreateObject(mapping.Read(), key, 0, ObjectSizeMode::Variable));
  std::vector<uint64_t> ids;
  const auto unit = options.objects_->allocator_.allocation_bytes_;
  for (uint64_t i = 0; i < 6; ++i) {
    ObjectMutation write{ObjectOperation::Write, key, i * unit, ObjectSizeMode::Variable,
                         std::vector<std::byte>(unit, static_cast<std::byte>(31 + i))};
    write.common_only_ = true;
    ObjectTransaction tx{{std::move(write)}, {}};
    auto accepted = pipeline.Submit(tx);
    ASSERT_EQ(accepted.admission_, IOAdmission::Accepted);
    accepted.ticket_->Wait();
    Durable(accepted.ticket_->Result());
    ids.push_back(mapping.Read().Resolve(key, i * unit, unit).spans_.front().data_->allocation_);
  }
  const auto view = mapping.Read();
  std::optional<ObjectReadLease> pin(refs.ProtectRead(view, key, unit, unit));
  Durable(mapping.Remove(mapping.Read(), key));
  ObjectReclaimCursor cursor;
  refs.BeginReclaim(mapping.Read(), key, ids, &cursor);
  // Six retirement records take two scan rounds at four entries per round.
  EXPECT_TRUE(refs.Reclaim(mapping.Read(), key, ids[0], &cursor).scanning_);
  EXPECT_TRUE(refs.Reclaim(mapping.Read(), key, ids[0], &cursor).scanning_);
  uint64_t released = 0;
  for (size_t i = 0; i < ids.size(); ++i) {
    const auto r = refs.Reclaim(mapping.Read(), key, ids[i], &cursor);
    EXPECT_FALSE(r.scanning_) << "later allocations must reuse the completed discovery";
    if (r.commit_) Durable(*r.commit_);
    EXPECT_EQ(r.released_bytes_, i == 1 ? 0 : unit);
    released += r.released_bytes_;
  }
  EXPECT_EQ(released, 5 * unit);
  pin.reset();
  auto last = refs.Reclaim(mapping.Read(), key, ids[1]);
  ASSERT_TRUE(last.commit_);
  Durable(*last.commit_);
  EXPECT_EQ(last.released_bytes_, unit);
  EXPECT_EQ(allocator.Usage().committed_bytes_, 0);
  auto reused = allocator.Reserve(6 * unit);
  EXPECT_FALSE(reused.Extents().empty());
  pipeline.Close();
}
}  // namespace bustub
