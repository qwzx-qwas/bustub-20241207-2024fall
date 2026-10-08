//===----------------------------------------------------------------------===//
// BusTub: node-local storage lifecycle and service state (F33/F34).
//===----------------------------------------------------------------------===//
#pragma once

#include <cstdint>
#include <exception>
#include <filesystem>
#include <memory>
#include <optional>
#include <vector>

#include "storage/disk/io_executor.h"
#include "storage/disk/metadata_engine.h"
#include "storage/disk/object_io.h"
#include "storage/disk/object_reference.h"
#include "storage/disk/object_transaction.h"

namespace bustub {

enum class NodeStoragePhase { Stopped, Starting, Recovering, Serving, Draining, Failed };
enum class NodeStorageCondition : uint32_t {
  RecoveryFailed = 1U << 0,
  IOFault = 1U << 1,
  ResourcePressure = 1U << 2,
  BootstrapRedundancyLost = 1U << 3,
  BootstrapRepairFailed = 1U << 4,
  Corruption = 1U << 5
};

struct NodeStorageView {
  uint64_t generation_{0};
  NodeStoragePhase phase_{NodeStoragePhase::Stopped};
  uint32_t conditions_{0};
  std::optional<NodeStorageCondition> primary_condition_;
  bool metadata_read_{false};
  bool metadata_write_{false};
  bool metadata_maintenance_{false};
  bool object_read_{false};
  bool object_data_write_{false};  // New data IO, not a complete S7 transaction API.
  bool object_transaction_{false};
  IntegrityScanStatus integrity_;
  std::exception_ptr object_error_;
  std::exception_ptr error_;
  std::exception_ptr repair_error_;
};

struct ObjectStorageOptions {
  DataAllocatorOptions allocator_;
  ObjectMappingOptions mapping_;
  ObjectReferenceOptions references_;
  uint64_t max_read_bytes_;
  uint64_t max_write_bytes_;
};

struct NodeStorageOptions {
  std::filesystem::path device_path_;
  BlockDeviceOptions device_;
  IOExecutorOptions io_;
  BootstrapIdentity identity_;
  JournalIdentity journal_identity_;
  JournalOptions journal_;
  MetadataOptions metadata_;
  BootstrapRepairOptions repair_;
  // Empty preserves the production metadata-only deployment. Open never formats.
  std::optional<ObjectStorageOptions> objects_{std::nullopt};
  // Explicit S7 service; metadata-only and S6 deployments keep their contracts.
  std::optional<ObjectTransactionOptions> transactions_{std::nullopt};
  size_t external_buffer_bytes_{0};  // Explicit frame retention budget; zero disables it.
  std::optional<ResourceBudgetOptions> memory_budget_{std::nullopt};
};
struct PageIOCapabilities {
  size_t memory_alignment_;
  size_t max_batch_pages_;
  uint64_t max_read_bytes_;
  uint64_t max_write_bytes_;
};

/** Owns one device -> executor -> bootstrap -> B/Journal stack exclusively.
 * S6 optionally owns allocator/mappings/references and ordinary object IO.
 * These expose local storage readiness, NOT SQL/Raft readiness. No automatic
 * format, migration, checkpoint on Close, or retry of an uncertain write.
 * Explicit options retain the existing component budgets and IO semantics.
 *
 * Create/Open perform real work on their caller, without holding the state lock
 * across IO. State/Close may run concurrently with startup. Close invalidates
 * publication, drains accepted calls and repair, then shuts down IO and device.
 * A cancelled startup throws NotReady after cleanup. Reopen requires Close and
 * constructs fresh components; old snapshots remain immutable RAM views.
 * Calls may overlap after opening; B still owns transaction ordering/admission.
 * Destruction requires callers to finish; use explicit Close to observe errors.
 */
class NodeStorage {
 public:
  explicit NodeStorage(NodeStorageOptions options);
  ~NodeStorage();
  NodeStorage(const NodeStorage &) = delete;
  auto operator=(const NodeStorage &) -> NodeStorage & = delete;

  void Create(const BootstrapLayout &layout);
  void Open();
  auto State() const -> NodeStorageView;
  /** Shared assembly dependency for page caches and Raft work. May be absent. */
  auto MemoryBudget() const -> std::shared_ptr<ResourceBudget>;
  /** One Store owner joins the existing bounded GC role. Empty detaches and
   * waits for its accepted callback, without holding the node state lock. */
  void SetStoreMaintenance(std::function<void()> step);
  auto Read() -> MetadataSnapshot;
  auto Commit(const MetadataSnapshot &base, const std::vector<MetadataMutation> &mutations) -> JournalResult;
  auto Writeback(size_t max_pages) -> MetadataWritebackResult;
  auto Checkpoint(size_t max_pages) -> MetadataCheckpointResult;
  /** Explicitly initialize missing ordinary-object structures on an existing B.
   * Resumes partial initialization; never upgrades an existing legacy format.
   */
  void InitializeObjects();
  auto Objects() -> ObjectMappingSnapshot;
  auto CreateObjectSpace(const ObjectMappingSnapshot &base) -> ObjectSpaceCreation;
  auto CreateObject(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t length, ObjectSizeMode mode)
      -> JournalResult;
  auto ResizeObject(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t length) -> JournalResult;
  auto RemoveObject(const ObjectMappingSnapshot &base, ObjectKey key) -> JournalResult;
  auto SupportsObjectSharing() -> bool;
  /** In-memory capture protection; no device IO. Retain until the captured
   * mappings have acquired their persistent shared references. */
  auto ProtectObject(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t offset, uint64_t length)
      -> ObjectReadLease;
  auto ShareObjectRange(const ObjectMappingSnapshot &source, ObjectKey key, ObjectKey destination, uint64_t offset,
                        uint64_t length, uint64_t destination_offset) -> JournalResult;
  auto ReclaimObject(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t allocation) -> ObjectReclaimResult;
  /** Construct an unpublished immutable range using bounded commits. May have
   * committed a prefix on failure; caller owns cancellation of its candidate. */
  void ShareObjectRangeBatched(const ObjectMappingSnapshot &source, ObjectKey key, ObjectKey destination,
                               uint64_t offset, uint64_t length, uint64_t destination_offset);
  auto ReadObject(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t offset, uint64_t length) -> ObjectRead;
  auto ReadObjectInto(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t offset, uint64_t length,
                      ObjectReadTarget target) -> ObjectRead;
  /** Optional direct read-ahead; true transfers completion, false does no IO.
   * Completion may run inline; it must only update state/notify, never throw or wait. */
  auto PrefetchObjectInto(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t offset, uint64_t length,
                          ObjectReadTarget target, std::function<void(std::exception_ptr)> complete) -> bool;
  auto PageIO() const -> PageIOCapabilities;
  auto WriteObjectData(const void *source, size_t size) -> ObjectWrite;
  auto PublishObjectData(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t offset, ObjectWrite &write)
      -> JournalResult;
  /** Non-waiting queue admission; Accepted consumes input and owns execution.
   * Full/Stopped leave input intact. The ticket reports durable publication.
   */
  auto SubmitObjects(ObjectTransaction &transaction) -> ObjectTransactionSubmission;
  /** Preflight a final control-only publication before preparing large bodies.
   * Checks permanent encoding/admission limits, not a promise of free resources. */
  void CheckControlBatch(const std::vector<ObjectControlMutation> &controls) const;
  void Close();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bustub
