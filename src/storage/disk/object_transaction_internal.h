#pragma once

#include <memory>

#include "object_change_internal.h"  // NOLINT(build/include_subdir): private schema.
#include "object_io_internal.h"      // NOLINT(build/include_subdir): private sibling component.
#include "storage/disk/object_transaction.h"

namespace bustub {
class ObjectTransactionPipeline {
 public:
  ObjectTransactionPipeline(ObjectIO &io, ObjectMappingStore &mapping, ObjectReferenceManager &references,
                            ObjectTransactionOptions options, std::shared_ptr<ResourceBudget> memory);
  ~ObjectTransactionPipeline();
  auto Submit(ObjectTransaction &transaction) -> ObjectTransactionSubmission;
  auto Error() const -> std::exception_ptr;
  void Close();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
}  // namespace bustub
