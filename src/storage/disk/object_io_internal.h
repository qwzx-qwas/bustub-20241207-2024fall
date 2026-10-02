#pragma once

#include <memory>
#include <vector>

#include "storage/disk/object_io.h"
#include "storage/disk/object_reference.h"
#include "storage/disk/region_manager.h"

namespace bustub {
struct ObjectIOState;
struct ObjectChange;
struct ObjectControlMutation;
// Only a batch which fits the configured executor can report temporary pressure.
class ObjectIOBusy : public MetadataError {
 public:
  ObjectIOBusy() : MetadataError(MetadataErrorCode::ResourceUnavailable, "object IO capacity is held") {}
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
           const IOExecutorOptions &io_limits);
  ~ObjectIO();
  auto Read(const ObjectMappingSnapshot &view, ObjectKey key, uint64_t offset, uint64_t length) -> ObjectRead;
  auto Read(const ObjectMappingSnapshot &view, ObjectKey key, uint64_t offset, uint64_t length,
            std::function<void()> ready) -> ObjectRead;
  auto Write(const void *source, size_t size) -> ObjectWrite;
  auto Publish(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t offset, ObjectWrite &write) -> JournalResult;
  auto Error() const -> std::exception_ptr;
  auto WriteCommon(std::vector<ObjectChange> *changes, const std::vector<std::vector<std::byte>> &bytes,
                   std::function<void()> ready) -> CommonDataWrite;
  void FinishCommon(CommonDataWrite *write);
  void Close();

 private:
  void CheckBatchBudget(const std::vector<RegionIORequest> &requests) const;
  RegionManager &regions_;
  IOExecutor &executor_;
  DataAllocator &allocator_;
  ObjectMappingStore &mapping_;
  ObjectReferenceManager &references_;
  RegionHandle region_;
  const uint64_t max_read_bytes_;
  const uint64_t max_write_bytes_;
  const IOExecutorOptions io_limits_;
  std::shared_ptr<ObjectIOState> state_;
};
}  // namespace bustub
