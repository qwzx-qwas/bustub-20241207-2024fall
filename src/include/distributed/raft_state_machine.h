//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// raft_state_machine.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>  // NOLINT(build/c++11)
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog_snapshot.h"
#include "common/state_visibility.h"
#include "distributed/bustub_state_machine.h"
#include "raft/state_machine.h"
#include "recovery/canonical_snapshot.h"
#include "recovery/node_directory.h"

namespace bustub {

/** Small-payload compatibility value used only by codec tests and migrations. */
struct BusTubSnapshotBundleV1 {
  uint64_t last_included_index_{0};
  std::vector<std::byte> database_;
  std::vector<std::byte> catalog_;
  std::vector<std::byte> sessions_;
};

struct BusTubSnapshotBundleFileView {
  uint64_t last_included_index_{0};
  DurableFileSlice database_;
  DurableFileSlice catalog_;
  DurableFileSlice sessions_;
};

struct BusTubSnapshotBundleView {
  uint64_t last_included_index_;
  SnapshotInput database_;
  SnapshotInput catalog_;
  SnapshotInput sessions_;
};

/** Portable Raft payload containing the complete canonical BusTub snapshot, not a working-file reference. */
class BusTubSnapshotBundleCodec {
 public:
  static constexpr uint32_t FORMAT_VERSION = 1;
  static constexpr size_t MAX_IN_MEMORY_BUNDLE_BYTES = 128U * 1024U * 1024U;
  /** Course-scale file bound shared with SnapshotStore; it is not a capacity target. */
  static constexpr uint64_t MAX_STREAM_BUNDLE_BYTES = 1ULL << 30U;

  /** Compatibility helpers; the formal BusTub FSM never calls these aggregate APIs. */
  static auto Encode(const BusTubSnapshotBundleV1 &bundle) -> std::vector<std::byte>;
  static auto Decode(const std::vector<std::byte> &bytes) -> BusTubSnapshotBundleV1;
  static void Write(uint64_t index, const CanonicalSnapshotPaths &paths, const SnapshotAppend &append,
                    DurableStorage *storage);
  static auto Read(const SnapshotInput &payload) -> BusTubSnapshotBundleView;
  static void EncodeFiles(uint64_t last_included_index, const CanonicalSnapshotPaths &paths,
                          const std::filesystem::path &output, DurableStorage *storage);
  static auto DecodeFile(const DurableFileSlice &payload, DurableStorage *storage) -> BusTubSnapshotBundleFileView;
};

/** Formal RaftStateMachine adapter that owns replaceable canonical BusTub working state. */
struct ObjectPageDeployment {
  std::shared_ptr<NodeStorage> storage_;
  ObjectPageOptions pages_;
  BufferPoolOptions cache_;
};
struct PublishedSqlRead {
  uint64_t index_;
  std::vector<std::byte> payload_;
};
class BusTubRaftStateMachine : public RaftStateMachine {
 public:
  ~BusTubRaftStateMachine() override;
  static auto Open(NodeDirectory *node_directory, std::shared_ptr<DurableStorage> storage = nullptr,
                   size_t buffer_pool_size = 128) -> std::shared_ptr<BusTubRaftStateMachine>;

  static auto OpenObjectPages(NodeDirectory *node_directory, std::shared_ptr<DurableStorage> storage,
                              size_t buffer_pool_size, ObjectPageDeployment deployment)
      -> std::shared_ptr<BusTubRaftStateMachine>;

  void ValidateProposalPayload(EntryType type, const std::vector<std::byte> &payload) const override;
  void Apply(const ReplicatedLogEntry &entry) override;
  auto LastApplied() const -> uint64_t override;
  auto LocalRecoveryPoint() const -> std::optional<StateMachineRecoveryPoint> override;
  /** One background candidate; false means unsupported or already in progress.
   * Request acceptance is not publication. Poll reports worker errors. */
  auto RequestCheckpoint(uint64_t index, uint64_t term) -> bool;
  auto PollCheckpoint() -> uint64_t;
  void DrainCheckpoint();
  void WriteSnapshot(const SnapshotAppend &append) const override;
  auto WriteSharedSnapshot(NodeStorage &storage, const ObjectKey &destination, uint64_t index, uint64_t term,
                           uint64_t limit) -> std::optional<SharedSnapshotResult> override;
  auto PrepareSnapshot(const SnapshotInput &payload, uint64_t index) -> std::unique_ptr<PreparedSnapshot> override;
  void ValidateSnapshot(const SnapshotInput &payload, uint64_t index) override;
  void LoadSnapshot(const SnapshotInput &payload, uint64_t index) override;
  void CreateSnapshotFile(const std::filesystem::path &path) const override;
  void ValidateSnapshotFile(const DurableFileSlice &payload, uint64_t last_included_index) override;
  void InstallSnapshotFile(const DurableFileSlice &payload, uint64_t last_included_index) override;

  auto PrepareSql(const std::string &sql, uint64_t client_id, uint64_t request_id,
                  const RequestFingerprintV1 &request_fingerprint) const -> TransactionCommandBatch;
  auto ClassifyRequest(uint64_t client_id, uint64_t request_id, const RequestFingerprintV1 &request_fingerprint) const
      -> RequestDisposition;
  void ValidateProposal(const TransactionCommandBatch &batch) const;
  auto GetRow(table_oid_t table_oid, const EncodedPrimaryKeyV1 &primary_key) const
      -> std::optional<std::pair<TupleMeta, Tuple>>;
  auto GetLastResponse(uint64_t client_id) const -> std::optional<std::vector<std::byte>>;
  auto ExecuteReadSql(const std::string &sql, uint64_t minimum_index) const -> PublishedSqlRead;
  auto PublishedAppliedIndex() const -> uint64_t;
  auto CatalogSnapshotForRead() const -> CatalogSnapshot;

 private:
  struct WorkingState;
  struct Prepared;
  struct CheckpointWorker;

  BusTubRaftStateMachine(NodeDirectory *node_directory, std::shared_ptr<DurableStorage> storage,
                         size_t buffer_pool_size);
  void InitializeEmpty();
  void OpenLocalCheckpoint();
  void BuildLocalCheckpoint(uint64_t index, uint64_t term);
  auto BuildSnapshotState(const SnapshotInput &payload, uint64_t index, const std::filesystem::path &directory)
      -> std::unique_ptr<WorkingState>;
  auto FinishWorkingState(std::unique_ptr<WorkingState> state, uint64_t index, const std::vector<std::byte> &catalog,
                          const std::vector<std::byte> &sessions, bool canonical) -> std::unique_ptr<WorkingState>;
  auto BuildWorkingState(const BusTubSnapshotBundleView &bundle, const std::filesystem::path &directory)
      -> std::unique_ptr<WorkingState>;
  auto OpenWorkingState(uint64_t last_included_index, const std::vector<std::byte> &catalog_bytes,
                        const std::vector<std::byte> &session_bytes, const std::filesystem::path &directory)
      -> std::unique_ptr<WorkingState>;

  std::optional<ObjectPageDeployment> page_deployment_;
  NodeDirectory *node_directory_;
  std::shared_ptr<DurableStorage> storage_;
  size_t buffer_pool_size_;
  std::filesystem::path runtime_directory_;
  mutable StateVisibilityLatch visibility_;
  mutable std::mutex lifecycle_mutex_;
  // Serializes checkpoint publication/retirement with acquisition of shared
  // snapshot references. Business Apply does not take this mutex.
  std::mutex checkpoint_build_mutex_;
  mutable uint64_t next_generation_{0};
  std::filesystem::path active_directory_;
  std::shared_ptr<WorkingState> state_;
  std::unique_ptr<BusTubStateMachine> fsm_;
  std::optional<StateMachineRecoveryPoint> recovered_point_;
  uint64_t applied_term_{0};
  std::unique_ptr<CheckpointWorker> checkpoint_;
};

}  // namespace bustub
