//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// single_node_runtime_test.cpp
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include <filesystem>
#include <memory>
#include <string>
#include <string_view>

#include "distributed/command.h"
#include "gtest/gtest.h"
#include "recovery/single_node_runtime.h"
#include "type/value_factory.h"

namespace bustub {
namespace {

auto RuntimeDirectory() -> std::filesystem::path {
  return std::filesystem::temp_directory_path() / ("bustub-single-node-runtime-" + std::to_string(getpid()));
}

auto AccountsSchema() -> Schema { return Schema({Column("id", TypeId::INTEGER), Column("name", TypeId::VARCHAR, 32)}); }

auto AccountTuple(int32_t id, std::string name) -> Tuple {
  const auto schema = AccountsSchema();
  return Tuple({ValueFactory::GetIntegerValue(id), ValueFactory::GetVarcharValue(name)}, &schema);
}

auto AccountKey(int32_t id) -> EncodedPrimaryKeyV1 {
  return PrimaryKeyCodecV1::Encode(ValueFactory::GetIntegerValue(id));
}

auto Fingerprint(std::string_view sql) -> RequestFingerprintV1 { return ComputeWriteIntentFingerprintV1(sql); }

auto CreateAccounts(uint64_t request_id) -> TransactionCommandBatch {
  return {2,
          41,
          request_id,
          Fingerprint("CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32));"),
          0,
          {CreateTableCommand{0,
                              0,
                              "accounts",
                              {{"id", TypeId::INTEGER, 4, false}, {"name", TypeId::VARCHAR, 32, true}},
                              {0, TypeId::INTEGER, 1}}}};
}

}  // namespace

// M2-IT01 (cumulatively rechecking M0/M1): real files recover a canonical snapshot, rebuild indexes before replay,
// and fall back from a corrupt newest generation while preserving exact SessionTable retry bytes.
TEST(SingleNodeCommandRuntimeTest, SnapshotSuffixReplayAndCorruptCurrentFallback) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto root = RuntimeDirectory();
  storage->RemoveTree(root);

  auto runtime = SingleNodeCommandRuntime::Open(root, storage);
  EXPECT_EQ(runtime->SnapshotGeneration(), 1);
  EXPECT_EQ(runtime->SnapshotBaseIndex(), 0);
  EXPECT_EQ(runtime->CommitIndex(), 0);

  EXPECT_EQ(WriteResponseCodec::Decode(runtime->Commit(CreateAccounts(1))).commit_index_, 1);
  const auto schema = AccountsSchema();
  const auto original = AccountTuple(1, "alice");
  TransactionCommandBatch insert_one{2, 41,
                                     2, Fingerprint("INSERT INTO accounts VALUES (1, 'alice');"),
                                     1, {InsertRowCommand{0, AccountKey(1), TupleCodecV1::Encode(original, schema)}}};
  EXPECT_EQ(WriteResponseCodec::Decode(runtime->Commit(insert_one)).commit_index_, 2);

  TransactionCommandBatch secondary{
      2,
      41,
      3,
      Fingerprint("CREATE INDEX accounts_name ON accounts(name);"),
      1,
      {CreateIndexCommand{
          1, 0, "accounts_name", {1}, IndexType::BPlusTreeIndex, IndexConstraintKind::NON_UNIQUE_SECONDARY}}};
  EXPECT_EQ(WriteResponseCodec::Decode(runtime->Commit(secondary)).commit_index_, 3);
  const auto fallback_manifest = runtime->CreateSnapshot();
  EXPECT_EQ(fallback_manifest.generation_, 2);
  EXPECT_EQ(fallback_manifest.last_included_index_, 3);

  const auto replacement = AccountTuple(1, "alice-updated");
  TransactionCommandBatch update{2,
                                 41,
                                 4,
                                 Fingerprint("UPDATE accounts SET name = 'alice-updated' WHERE id = 1;"),
                                 2,
                                 {UpdateRowCommand{0, AccountKey(1), 2, TupleCodecV1::Encode(original, schema),
                                                   TupleCodecV1::Encode(replacement, schema)}}};
  EXPECT_EQ(WriteResponseCodec::Decode(runtime->Commit(update)).commit_index_, 4);
  const auto current_manifest = runtime->CreateSnapshot();
  EXPECT_EQ(current_manifest.generation_, 3);
  EXPECT_EQ(current_manifest.last_included_index_, 4);

  const auto second = AccountTuple(2, "bob");
  TransactionCommandBatch insert_two{2, 41,
                                     5, Fingerprint("INSERT INTO accounts VALUES (2, 'bob');"),
                                     2, {InsertRowCommand{0, AccountKey(2), TupleCodecV1::Encode(second, schema)}}};
  const auto response_five = runtime->Commit(insert_two);
  EXPECT_EQ(WriteResponseCodec::Decode(response_five).commit_index_, 5);
  runtime.reset();

  // Simulate a crash after a log fdatasync but before HARD_STATE advanced. Recovery must not Apply or retain index 6.
  auto uncommitted = CommandLog::Open(root / "raft" / "log", storage, 5, 4, 0);
  EXPECT_NO_THROW(uncommitted->Append({{1, 6, 0, EntryType::NOOP, {}}}));
  uncommitted.reset();

  // Corrupt the CURRENT generation after shutdown. Recovery must select generation 2 and replay entries 4 and 5.
  const auto corrupt_database = root / "state" / current_manifest.database_file_;
  auto bytes = storage->ReadFile(corrupt_database, 64U * 1024U * 1024U);
  ASSERT_FALSE(bytes.empty());
  bytes[0] ^= std::byte{1};
  storage->WriteFile(corrupt_database, bytes);
  storage->SyncFile(corrupt_database);

  runtime = SingleNodeCommandRuntime::Open(root, storage);
  EXPECT_EQ(runtime->SnapshotGeneration(), 2);
  EXPECT_EQ(runtime->SnapshotBaseIndex(), 3);
  EXPECT_EQ(runtime->CommitIndex(), 5);
  EXPECT_EQ(runtime->LastApplied(), 5);
  EXPECT_EQ(runtime->PublishedAppliedIndex(), 5);
  EXPECT_EQ(runtime->LastLogIndex(), 5);

  const auto recovered_one = runtime->GetRow(0, AccountKey(1));
  ASSERT_TRUE(recovered_one.has_value());
  EXPECT_EQ(recovered_one->first.ts_, 4);
  EXPECT_EQ(recovered_one->second.GetValue(&schema, 1).ToString(), "alice-updated");
  const auto recovered_two = runtime->GetRow(0, AccountKey(2));
  ASSERT_TRUE(recovered_two.has_value());
  EXPECT_EQ(recovered_two->first.ts_, 5);
  EXPECT_EQ(recovered_two->second.GetValue(&schema, 1).ToString(), "bob");

  ASSERT_NE(runtime->CatalogForRead()->GetIndex(0), nullptr);
  ASSERT_NE(runtime->CatalogForRead()->GetIndex(1), nullptr);
  EXPECT_EQ(runtime->CatalogForRead()->GetIndex(0)->constraint_kind_, IndexConstraintKind::PRIMARY_KEY);
  EXPECT_EQ(runtime->CatalogForRead()->GetIndex(1)->constraint_kind_, IndexConstraintKind::NON_UNIQUE_SECONDARY);
  EXPECT_EQ(runtime->CatalogForRead()->GetNextTableOid(), 1);
  EXPECT_EQ(runtime->CatalogForRead()->GetNextIndexOid(), 2);
  EXPECT_EQ(runtime->CatalogForRead()->GetSchemaEpoch(), 2);

  // Session replay is part of the same state: retrying request 5 neither appends nor mutates data a second time.
  EXPECT_EQ(runtime->Commit(insert_two), response_five);
  EXPECT_EQ(runtime->LastLogIndex(), 5);
  EXPECT_EQ(runtime->GetRow(0, AccountKey(2))->first.ts_, 5);

  // A changed payload under the retained latest identity is rejected by Session before validation or append.
  // Its command would create a visible row if it reached the state machine, so the row and log are independent oracles.
  const auto conflicting = AccountTuple(3, "must-not-appear");
  TransactionCommandBatch conflicting_retry{
      2, 41,
      5, Fingerprint("INSERT INTO accounts VALUES (3, 'must-not-appear');"),
      2, {InsertRowCommand{0, AccountKey(3), TupleCodecV1::Encode(conflicting, schema)}}};
  EXPECT_THROW(runtime->Commit(conflicting_retry), std::runtime_error);
  EXPECT_EQ(runtime->LastLogIndex(), 5);
  EXPECT_EQ(runtime->CommitIndex(), 5);
  EXPECT_FALSE(runtime->GetRow(0, AccountKey(3)).has_value());
  EXPECT_EQ(runtime->Commit(insert_two), response_five);

  const auto invalid_replacement = AccountTuple(1, "must-not-commit");
  TransactionCommandBatch stale_update{
      2,
      41,
      6,
      Fingerprint("UPDATE accounts SET name = 'must-not-commit' WHERE id = 1;"),
      2,
      {UpdateRowCommand{0, AccountKey(1), 999, TupleCodecV1::Encode(replacement, schema),
                        TupleCodecV1::Encode(invalid_replacement, schema)}}};
  EXPECT_THROW(runtime->Commit(stale_update), std::runtime_error);
  EXPECT_EQ(runtime->CommitIndex(), 5);
  EXPECT_EQ(runtime->LastLogIndex(), 5);
  EXPECT_EQ(runtime->GetRow(0, AccountKey(1))->second.GetValue(&schema, 1).ToString(), "alice-updated");

  runtime.reset();
  storage->RemoveTree(root);
}

}  // namespace bustub
