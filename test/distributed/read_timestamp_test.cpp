//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// read_timestamp_test.cpp
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/bustub_instance.h"
#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace bustub {

// M5-T05: distributed reads use the Raft-derived timestamp supplied by the caller, not the local allocator.
TEST(DistributedReadTimestampTest, BeginReadAtUsesExactPublishedIndex) {
  auto instance = std::make_unique<BusTubInstance>();
  auto *read = instance->txn_manager_->BeginReadAt(42);
  EXPECT_EQ(read->GetReadTs(), 42);
  EXPECT_EQ(read->GetIsolationLevel(), IsolationLevel::SNAPSHOT_ISOLATION);
  instance->txn_manager_->EndRead(read);
  EXPECT_EQ(read->GetTransactionState(), TransactionState::COMMITTED);
  EXPECT_EQ(read->GetCommitTs(), 42);

  auto *local = instance->txn_manager_->Begin();
  EXPECT_EQ(local->GetReadTs(), 0);
  instance->txn_manager_->Abort(local);

  EXPECT_THROW(instance->txn_manager_->BeginReadAt(INVALID_TS), Exception);
  EXPECT_THROW(instance->txn_manager_->BeginReadAt(TXN_START_ID), Exception);
}

}  // namespace bustub
