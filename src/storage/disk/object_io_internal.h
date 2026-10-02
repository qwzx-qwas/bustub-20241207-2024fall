#pragma once

#include <memory>

#include "storage/disk/object_io.h"
#include "storage/disk/object_reference.h"
#include "storage/disk/region_manager.h"

namespace bustub {
struct ObjectIOState;
/** Internal component, constructed only by the node owner from one bound stack.
 * Its dependencies outlive Close. It owns no worker, WAL, or clean-data cache.
 */
class ObjectIO {
 public:
  ObjectIO(RegionManager &regions, IOExecutor &executor, DataAllocator &allocator, ObjectMappingStore &mapping,
           ObjectReferenceManager &references, uint64_t max_read_bytes, uint64_t max_write_bytes);
  ~ObjectIO();
  auto Read(const ObjectMappingSnapshot &view, ObjectKey key, uint64_t offset, uint64_t length) -> ObjectRead;
  auto Write(const void *source, size_t size) -> ObjectWrite;
  auto Publish(const ObjectMappingSnapshot &base, ObjectKey key, uint64_t offset, ObjectWrite &write) -> JournalResult;
  auto Error() const -> std::exception_ptr;
  void Close();

 private:
  RegionManager &regions_;
  IOExecutor &executor_;
  DataAllocator &allocator_;
  ObjectMappingStore &mapping_;
  ObjectReferenceManager &references_;
  RegionHandle region_;
  const uint64_t max_read_bytes_;
  const uint64_t max_write_bytes_;
  std::shared_ptr<ObjectIOState> state_;
};
}  // namespace bustub
