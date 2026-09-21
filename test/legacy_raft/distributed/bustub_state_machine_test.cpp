//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// bustub_state_machine_test.cpp
//
//===----------------------------------------------------------------------===//

#include <atomic>
#include <chrono>              // NOLINT(build/c++11)
#include <condition_variable>  // NOLINT(build/c++11)
#include <exception>
#include <memory>
#include <mutex>  // NOLINT(build/c++11)
#include <string>
#include <string_view>
#include <thread>  // NOLINT(build/c++11)
#include <utility>
#include <vector>

#include "common/bustub_instance.h"
#include "distributed/bustub_state_machine.h"
#include "gtest/gtest.h"
#include "type/value_factory.h"

namespace bustub {
namespace {

auto AccountsSchema() -> Schema { return Schema({Column("id", TypeId::INTEGER), Column("name", TypeId::VARCHAR, 32)}); }

auto AccountTuple(int32_t id, const std::string &name) -> Tuple {
  const auto schema = AccountsSchema();
  return Tuple({ValueFactory::GetIntegerValue(id), ValueFactory::GetVarcharValue(name)}, &schema);
}

auto Key(int32_t id) -> EncodedPrimaryKeyV1 { return PrimaryKeyCodecV1::Encode(ValueFactory::GetIntegerValue(id)); }

auto Entry(uint64_t index, uint64_t term, const TransactionCommandBatch &batch) -> ReplicatedLogEntry {
  return {1, index, term, EntryType::COMMAND_BATCH, CommandBatchCodec::Encode(batch)};
}

auto BuildBatch(uint64_t client, uint64_t request, std::string_view sql, uint64_t expected_schema_epoch,
                std::vector<ReplicatedCommand> commands) -> TransactionCommandBatch {
  return CommandBuilder::Build(client, request, ComputeWriteIntentFingerprintV1(sql), expected_schema_epoch,
                               std::move(commands));
}

struct FsmFixture {
  BusTubInstance instance_{64};
  SessionTable sessions_;
  StateVisibilityLatch visibility_;
  BusTubStateMachine fsm_{instance_.catalog_.get(), &sessions_, &visibility_};
};

struct BlockingIndexGate {
  std::mutex mutex_;
  std::condition_variable cv_;
  bool insert_entered_{false};
  bool release_insert_{false};
  bool reader_attempting_{false};
  bool reader_acquired_{false};
};

/** Test adapter that pauses the first secondary-index insert while FSM Apply owns the publication latch. */
class BlockingIndex : public Index {
 public:
  BlockingIndex(std::unique_ptr<Index> inner, const Schema *table_schema, std::shared_ptr<BlockingIndexGate> gate)
      : Index(std::make_unique<IndexMetadata>(inner->GetName(), inner->GetMetadata()->GetTableName(), table_schema,
                                              inner->GetKeyAttrs(), inner->GetMetadata()->IsPrimaryKey())),
        inner_(std::move(inner)),
        gate_(std::move(gate)) {}

  auto InsertEntry(const Tuple &key, RID rid, Transaction *transaction) -> bool override {
    {
      std::unique_lock lock(gate_->mutex_);
      if (!blocked_once_) {
        blocked_once_ = true;
        gate_->insert_entered_ = true;
        gate_->cv_.notify_all();
        gate_->cv_.wait(lock, [&] { return gate_->release_insert_; });
      }
    }
    return inner_->InsertEntry(key, rid, transaction);
  }

  void DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) override {
    inner_->DeleteEntry(key, rid, transaction);
  }

  void ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) override {
    inner_->ScanKey(key, result, transaction);
  }

 private:
  std::unique_ptr<Index> inner_;
  std::shared_ptr<BlockingIndexGate> gate_;
  bool blocked_once_{false};
};

auto CreateAccounts(uint64_t client, uint64_t request) -> TransactionCommandBatch {
  return BuildBatch(client, request, "CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32));", 0,
                    {CreateTableCommand{0,
                                        0,
                                        "accounts",
                                        {{"id", TypeId::INTEGER, 4, false}, {"name", TypeId::VARCHAR, 32, true}},
                                        {0, TypeId::INTEGER, 1}}});
}

}  // namespace

// M5-T01: two independent physical instances Apply byte-identical DDL/DML state transitions and responses.
TEST(BusTubStateMachineTest, DeterministicExplicitOidDdlAndMultiRowDml) {
  FsmFixture left;
  FsmFixture right;
  const auto apply_both = [&](const ReplicatedLogEntry &entry) {
    left.fsm_.Apply(entry);
    right.fsm_.Apply(entry);
  };

  apply_both(Entry(1, 7, CreateAccounts(77, 1)));
  for (const auto *fixture : {&left, &right}) {
    EXPECT_EQ(fixture->instance_.catalog_->GetNextTableOid(), 1);
    EXPECT_EQ(fixture->instance_.catalog_->GetNextIndexOid(), 1);
    EXPECT_EQ(fixture->instance_.catalog_->GetSchemaEpoch(), 1);
    ASSERT_NE(fixture->instance_.catalog_->GetIndex(0), nullptr);
    EXPECT_EQ(fixture->instance_.catalog_->GetIndex(0)->constraint_kind_, IndexConstraintKind::PRIMARY_KEY);
  }

  const auto schema = AccountsSchema();
  const auto row_one = AccountTuple(1, "alice");
  const auto row_two = AccountTuple(2, "bob");
  auto inserts = BuildBatch(77, 2, "INSERT INTO accounts VALUES (2, 'bob'), (1, 'alice');", 1,
                            {InsertRowCommand{0, Key(2), TupleCodecV1::Encode(row_two, schema)},
                             InsertRowCommand{0, Key(1), TupleCodecV1::Encode(row_one, schema)}});
  apply_both(Entry(2, 7, inserts));

  auto secondary = BuildBatch(
      77, 3, "CREATE INDEX accounts_name ON accounts(name);", 1,
      {CreateIndexCommand{
          1, 0, "accounts_name", {1}, IndexType::BPlusTreeIndex, IndexConstraintKind::NON_UNIQUE_SECONDARY}});
  apply_both(Entry(3, 7, secondary));
  EXPECT_EQ(left.instance_.catalog_->GetSchemaEpoch(), 2);
  EXPECT_EQ(right.instance_.catalog_->GetSchemaEpoch(), 2);

  const auto replacement = AccountTuple(1, "alice-new");
  const auto deletion = BuildBatch(77, 4, "DELETE FROM accounts WHERE id = 2;", 2,
                                   {DeleteRowCommand{0, Key(2), 2, TupleCodecV1::Encode(row_two, schema)}});
  apply_both(Entry(4, 8, deletion));
  const auto update = BuildBatch(77, 5, "UPDATE accounts SET name = 'alice-new' WHERE id = 1;", 2,
                                 {UpdateRowCommand{0, Key(1), 2, TupleCodecV1::Encode(row_one, schema),
                                                   TupleCodecV1::Encode(replacement, schema)}});
  apply_both(Entry(5, 8, update));

  for (const auto *fixture : {&left, &right}) {
    const auto live = fixture->fsm_.GetRow(0, Key(1));
    ASSERT_TRUE(live.has_value());
    EXPECT_EQ(live->first.ts_, 5);
    EXPECT_EQ(live->second.GetValue(&schema, 1).ToString(), "alice-new");
    EXPECT_FALSE(fixture->fsm_.GetRow(0, Key(2)).has_value());
    EXPECT_EQ(fixture->fsm_.LastApplied(), 5);
    EXPECT_EQ(fixture->fsm_.PublishedAppliedIndex(), 5);
  }
  ASSERT_TRUE(left.fsm_.GetLastResponse(77).has_value());
  EXPECT_EQ(left.fsm_.GetLastResponse(77), right.fsm_.GetLastResponse(77));
  EXPECT_EQ(WriteResponseCodec::Decode(*left.fsm_.GetLastResponse(77)),
            (WriteResponseV1{1, WriteStatus::COMMITTED, 5, 8, 5}));

  // An accidentally re-proposed retry has no second data side effect and retains the original response bytes.
  const auto original_response = *left.fsm_.GetLastResponse(77);
  apply_both(Entry(6, 9, update));
  EXPECT_EQ(left.fsm_.GetLastResponse(77), original_response);
  EXPECT_EQ(left.fsm_.GetRow(0, Key(1))->first.ts_, 5);
  EXPECT_EQ(left.fsm_.PublishedAppliedIndex(), 6);

  apply_both({1, 7, 9, EntryType::NOOP, {}});
  EXPECT_EQ(left.fsm_.PublishedAppliedIndex(), 7);
  EXPECT_EQ(left.fsm_.GetLastResponse(77), original_response);
}

// M5-T02: precondition or explicit allocator drift in committed input fail-stops instead of skipping an index.
TEST(BusTubStateMachineTest, DriftIsFailStop) {
  FsmFixture fixture;
  fixture.fsm_.Apply(Entry(1, 1, CreateAccounts(5, 1)));
  const auto schema = AccountsSchema();
  const auto row = AccountTuple(1, "before");
  fixture.fsm_.Apply(Entry(2, 1,
                           BuildBatch(5, 2, "INSERT INTO accounts VALUES (1, 'before');", 1,
                                      {InsertRowCommand{0, Key(1), TupleCodecV1::Encode(row, schema)}})));

  const auto replacement = AccountTuple(1, "after");
  auto bad = BuildBatch(
      5, 3, "UPDATE accounts SET name = 'after' WHERE id = 1;", 1,
      {UpdateRowCommand{0, Key(1), 999, TupleCodecV1::Encode(row, schema), TupleCodecV1::Encode(replacement, schema)}});
  EXPECT_THROW(fixture.fsm_.Apply(Entry(3, 1, bad)), std::runtime_error);
  EXPECT_TRUE(fixture.fsm_.IsStopped());
  EXPECT_EQ(fixture.fsm_.LastApplied(), 2);
  EXPECT_THROW(fixture.fsm_.GetRow(0, Key(1)), std::runtime_error);
}

// M8-T01: a committed retry identity with different payload bytes fail-stops before any command can mutate state.
TEST(BusTubStateMachineTest, CommittedPayloadMismatchFailsBeforeCommandAndPublicationSideEffects) {
  FsmFixture fixture;
  fixture.fsm_.Apply(Entry(1, 1, CreateAccounts(314, 1)));

  const auto schema = AccountsSchema();
  const auto settled = AccountTuple(1, "settled");
  constexpr std::string_view original_sql = "INSERT INTO accounts VALUES (1, 'settled');";
  const auto original_fingerprint = ComputeWriteIntentFingerprintV1(original_sql);
  fixture.fsm_.Apply(
      Entry(2, 1,
            CommandBuilder::Build(314, 2, original_fingerprint, 1,
                                  {InsertRowCommand{0, Key(1), TupleCodecV1::Encode(settled, schema)}})));

  const auto response_before = fixture.sessions_.GetLastResponse(314);
  ASSERT_TRUE(response_before.has_value());
  EXPECT_EQ(WriteResponseCodec::Decode(*response_before), (WriteResponseV1{1, WriteStatus::COMMITTED, 2, 1, 2}));
  const auto sessions_before = fixture.sessions_.SnapshotRecords();
  ASSERT_EQ(sessions_before.size(), 1);
  ASSERT_EQ(sessions_before.count(314), 1);
  EXPECT_EQ(sessions_before.at(314).last_request_id_, 2);
  EXPECT_EQ(sessions_before.at(314).request_fingerprint_, original_fingerprint);

  const auto tampered = AccountTuple(1, "tampered");
  constexpr std::string_view changed_sql = "UPDATE accounts SET name = 'tampered' WHERE id = 1;";
  const auto changed_batch = CommandBuilder::Build(
      314, 2, ComputeWriteIntentFingerprintV1(changed_sql), 1,
      {UpdateRowCommand{0, Key(1), 2, TupleCodecV1::Encode(settled, schema), TupleCodecV1::Encode(tampered, schema)}});
  try {
    fixture.fsm_.Apply(Entry(3, 2, changed_batch));
    FAIL() << "a committed request identity with different payload bytes was accepted";
  } catch (const std::runtime_error &error) {
    EXPECT_STREQ(error.what(), "committed payload does not match request identity");
  }

  EXPECT_TRUE(fixture.fsm_.IsStopped());
  EXPECT_EQ(fixture.fsm_.LastApplied(), 2);
  EXPECT_EQ(fixture.fsm_.PublishedAppliedIndex(), 2);
  EXPECT_EQ(fixture.instance_.catalog_->GetSchemaEpoch(), 1);
  EXPECT_EQ(fixture.instance_.catalog_->GetNextTableOid(), 1);
  EXPECT_EQ(fixture.instance_.catalog_->GetNextIndexOid(), 1);
  EXPECT_NE(fixture.instance_.catalog_->GetTable(0), nullptr);
  EXPECT_NE(fixture.instance_.catalog_->GetIndex(0), nullptr);
  EXPECT_EQ(fixture.instance_.catalog_->GetIndex(1), nullptr);

  size_t live_rows = 0;
  const auto table = fixture.instance_.catalog_->GetTable(0);
  ASSERT_NE(table, nullptr);
  for (auto iterator = table->table_->MakeIterator(); !iterator.IsEnd(); ++iterator) {
    const auto [meta, tuple] = iterator.GetTuple();
    if (!meta.is_deleted_) {
      live_rows++;
      EXPECT_EQ(meta.ts_, 2);
      EXPECT_EQ(tuple.GetValue(&schema, 0).GetAs<int32_t>(), 1);
      EXPECT_EQ(tuple.GetValue(&schema, 1).ToString(), "settled");
    }
  }
  EXPECT_EQ(live_rows, 1);

  const auto sessions_after = fixture.sessions_.SnapshotRecords();
  ASSERT_EQ(sessions_after.size(), 1);
  ASSERT_EQ(sessions_after.count(314), 1);
  EXPECT_EQ(sessions_after.at(314).last_request_id_, 2);
  EXPECT_EQ(sessions_after.at(314).request_fingerprint_, original_fingerprint);
  EXPECT_EQ(sessions_after.at(314).encoded_response_, *response_before);
  EXPECT_EQ(WriteResponseCodec::Decode(sessions_after.at(314).encoded_response_),
            (WriteResponseV1{1, WriteStatus::COMMITTED, 2, 1, 2}));
}

// M5-T03: every scalar ordinary-secondary adapter keeps all RIDs for duplicate logical keys.
TEST(BusTubStateMachineTest, OrdinarySecondaryIndexesRetainDuplicateRids) {
  for (const auto index_type : {IndexType::BPlusTreeIndex, IndexType::HashTableIndex, IndexType::STLOrderedIndex,
                                IndexType::STLUnorderedIndex}) {
    SCOPED_TRACE(static_cast<uint32_t>(index_type));
    FsmFixture fixture;
    fixture.fsm_.Apply(Entry(1, 1, CreateAccounts(9, 1)));
    const auto schema = AccountsSchema();
    const auto first = AccountTuple(1, "same");
    const auto second = AccountTuple(2, "same");
    fixture.fsm_.Apply(Entry(2, 1,
                             BuildBatch(9, 2, "INSERT INTO accounts VALUES (1, 'same'), (2, 'same');", 1,
                                        {InsertRowCommand{0, Key(1), TupleCodecV1::Encode(first, schema)},
                                         InsertRowCommand{0, Key(2), TupleCodecV1::Encode(second, schema)}})));
    fixture.fsm_.Apply(Entry(
        3, 1,
        BuildBatch(
            9, 3, "CREATE INDEX same_name ON accounts(name);", 1,
            {CreateIndexCommand{1, 0, "same_name", {1}, index_type, IndexConstraintKind::NON_UNIQUE_SECONDARY}})));

    const auto secondary = fixture.instance_.catalog_->GetIndex(1);
    ASSERT_NE(secondary, nullptr);
    Tuple key_tuple({ValueFactory::GetVarcharValue("same")}, &secondary->key_schema_);
    std::vector<RID> rids;
    secondary->index_->ScanKey(key_tuple, &rids, nullptr);
    EXPECT_EQ(rids.size(), 2);

    fixture.fsm_.Apply(Entry(4, 1,
                             BuildBatch(9, 4, "DELETE FROM accounts WHERE id = 1;", 2,
                                        {DeleteRowCommand{0, Key(1), 2, TupleCodecV1::Encode(first, schema)}})));
    secondary->index_->ScanKey(key_tuple, &rids, nullptr);
    ASSERT_EQ(rids.size(), 1);
    const auto [remaining_meta, remaining_tuple] =
        fixture.instance_.catalog_->GetTable(0)->table_->GetTuple(rids.front());
    EXPECT_FALSE(remaining_meta.is_deleted_);
    EXPECT_EQ(remaining_tuple.GetValue(&schema, 0).GetAs<int32_t>(), 2);
  }
}

// M5-T04: a reader attempting to enter during a partially installed multi-row batch cannot observe it.
TEST(BusTubStateMachineTest, ReaderBlocksUntilDataIndexSessionAndWatermarkPublishTogether) {
  FsmFixture fixture;
  fixture.fsm_.Apply(Entry(1, 1, CreateAccounts(12, 1)));
  const auto schema = AccountsSchema();
  const auto first = AccountTuple(1, "before-1");
  const auto second = AccountTuple(2, "before-2");
  fixture.fsm_.Apply(Entry(2, 1,
                           BuildBatch(12, 2, "INSERT INTO accounts VALUES (1, 'before-1'), (2, 'before-2');", 1,
                                      {InsertRowCommand{0, Key(1), TupleCodecV1::Encode(first, schema)},
                                       InsertRowCommand{0, Key(2), TupleCodecV1::Encode(second, schema)}})));
  fixture.fsm_.Apply(Entry(
      3, 1,
      BuildBatch(
          12, 3, "CREATE INDEX accounts_name ON accounts(name);", 1,
          {CreateIndexCommand{
              1, 0, "accounts_name", {1}, IndexType::BPlusTreeIndex, IndexConstraintKind::NON_UNIQUE_SECONDARY}})));

  const auto secondary = fixture.instance_.catalog_->GetIndex(1);
  ASSERT_NE(secondary, nullptr);
  auto gate = std::make_shared<BlockingIndexGate>();
  secondary->index_ = std::make_unique<BlockingIndex>(std::move(secondary->index_), &schema, gate);

  const auto after_first = AccountTuple(1, "after-1");
  const auto after_second = AccountTuple(2, "after-2");
  const auto batch = BuildBatch(
      12, 4, "UPDATE accounts SET name = CASE id WHEN 1 THEN 'after-1' ELSE 'after-2' END WHERE id IN (1, 2);", 2,
      {UpdateRowCommand{0, Key(1), 2, TupleCodecV1::Encode(first, schema), TupleCodecV1::Encode(after_first, schema)},
       UpdateRowCommand{0, Key(2), 2, TupleCodecV1::Encode(second, schema),
                        TupleCodecV1::Encode(after_second, schema)}});

  std::exception_ptr apply_error;
  std::thread apply_thread([&] {
    try {
      fixture.fsm_.Apply(Entry(4, 2, batch));
    } catch (...) {
      apply_error = std::current_exception();
    }
  });
  bool insert_entered = false;
  {
    std::unique_lock lock(gate->mutex_);
    insert_entered = gate->cv_.wait_for(lock, std::chrono::seconds(2), [&] { return gate->insert_entered_; });
    if (!insert_entered) {
      gate->release_insert_ = true;
      gate->cv_.notify_all();
    }
  }
  if (!insert_entered) {
    apply_thread.join();
    FAIL() << "Apply did not reach the blocking secondary-index insertion";
  }

  std::vector<std::string> observed_names;
  std::optional<std::vector<std::byte>> observed_response;
  uint64_t observed_published = 0;
  std::thread reader_thread([&] {
    {
      std::scoped_lock lock(gate->mutex_);
      gate->reader_attempting_ = true;
      gate->cv_.notify_all();
    }
    auto shared = fixture.visibility_.LockShared();
    {
      std::scoped_lock lock(gate->mutex_);
      gate->reader_acquired_ = true;
      gate->cv_.notify_all();
    }
    const auto table = fixture.instance_.catalog_->GetTable(0);
    for (auto iterator = table->table_->MakeIterator(); !iterator.IsEnd(); ++iterator) {
      const auto [meta, tuple] = iterator.GetTuple();
      if (!meta.is_deleted_) {
        observed_names.push_back(tuple.GetValue(&schema, 1).ToString());
      }
    }
    observed_response = fixture.sessions_.GetLastResponse(12);
    observed_published = fixture.fsm_.PublishedAppliedIndex();
  });
  {
    std::unique_lock lock(gate->mutex_);
    ASSERT_TRUE(gate->cv_.wait_for(lock, std::chrono::seconds(2), [&] { return gate->reader_attempting_; }));
    EXPECT_FALSE(gate->cv_.wait_for(lock, std::chrono::milliseconds(50), [&] { return gate->reader_acquired_; }));
    gate->release_insert_ = true;
    gate->cv_.notify_all();
  }

  apply_thread.join();
  reader_thread.join();
  ASSERT_EQ(apply_error, nullptr);
  EXPECT_EQ(observed_names, (std::vector<std::string>{"after-1", "after-2"}));
  ASSERT_TRUE(observed_response.has_value());
  EXPECT_EQ(WriteResponseCodec::Decode(*observed_response), (WriteResponseV1{1, WriteStatus::COMMITTED, 4, 2, 4}));
  EXPECT_EQ(observed_published, 4);
  EXPECT_EQ(fixture.fsm_.LastApplied(), 4);
}

}  // namespace bustub
