#pragma once

#include <memory>
#include <vector>

#include "storage/disk/object_io.h"
#include "storage/disk/object_reference.h"
#include "storage/disk/object_transaction.h"
#include "storage/disk/region_manager.h"

namespace bustub {
struct ObjectIOState;
struct ObjectChange;
struct DeferredTarget;
struct ObjectScrubCandidate;
struct ObjectControlMutation;
struct CommonInput {
  std::vector<std::byte> owned_;
  std::optional<ObjectWriteSource> source_;
  auto Size() const -> size_t { return source_ ? source_->size_ : owned_.size(); }
  auto Data() const -> const void * { return source_ ? source_->data_ : owned_.data(); }
};
struct CommonDataWrite {
  std::optional<DataReservation> reservation_;
  std::optional<IOBatch> batch_;
  bool durable_{false};
};
/** Internal component, constructed only by the node owner from one bound stack.
 * Its dependencies outlive Close. It owns no worker, WAL, or clean-data cache.
 */
class ObjectIO {
 public:
  ObjectIO(RegionManager &regions, IOExecutor &executor, DataAllocator &allocator, ObjectMappingStore &mapping,
           ObjectReferenceManager &references, uint64_t max_read_bytes, uint64_t max_write_bytes,
           const IOExecutorOptions &io_limits, size_t external_bytes);
  ~ObjectIO();
  auto Read(const ObjectMappingSnapshot &view, ObjectKey key, uint64_t offset, uint64_t length) -> ObjectRead;
  auto Read(const ObjectMappingSnapshot &view, ObjectKey key, uint64_t offset, uint64_t length,
            std::function<void()> ready) -> ObjectRead;
  auto ReadInto(const ObjectMappingSnapshot &view, ObjectKey key, uint64_t offset, uint64_t length,
                ObjectReadTarget target) -> ObjectRead;
  auto PrefetchInto(const ObjectMappingSnapshot &view, ObjectKey key, uint64_t offset, uint64_t length,
                    ObjectReadTarget target, std::function<void(std::exception_ptr)> complete) -> bool;
  auto Write(const void *source, size_t size) -> ObjectWrite;
  auto Publish(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t offset, ObjectWrite &write) -> JournalResult;
  auto Error() const -> std::exception_ptr;
  auto WriteCommon(const ObjectMappingSnapshot &base, std::vector<ObjectChange> *changes,
                   const std::vector<CommonInput> &bytes, const std::vector<ObjectControlMutation> &controls,
                   std::function<void()> ready) -> CommonDataWrite;
  void FinishCommon(CommonDataWrite *write);
  auto WriteDeferred(const DeferredTarget &target, const std::vector<std::byte> &body) -> IOBatch;
  auto ScrubRead(const ObjectMappingSnapshot &view, const ObjectScrubCandidate &candidate) -> ObjectRead;
  void FinishScrub(const ObjectRead &read);
  void Close();

 private:
  auto ReadProtected(const ObjectMappingSnapshot &view, ObjectKey key, ObjectReadLease lease,
                     std::function<void()> ready) -> ObjectRead;
  auto ReadIntoImpl(const ObjectMappingSnapshot &view, ObjectKey key, uint64_t offset, uint64_t length,
                    ObjectReadTarget target, std::function<void(std::exception_ptr)> complete)
      -> std::optional<ObjectRead>;
  void CheckBatchBudget(const std::vector<RegionIORequest> &requests) const;
  void CheckExternalBudget(const std::vector<RegionIORequest> &requests, size_t capacity) const;
  RegionManager &regions_;
  IOExecutor &executor_;
  DataAllocator &allocator_;
  ObjectMappingStore &mapping_;
  ObjectReferenceManager &references_;
  RegionHandle region_;
  const uint64_t max_read_bytes_;
  const uint64_t max_write_bytes_;
  const IOExecutorOptions io_limits_;
  const size_t external_bytes_;
  std::shared_ptr<ResourceAccount> memory_;
  std::shared_ptr<ObjectIOState> state_;
};
}  // namespace bustub
