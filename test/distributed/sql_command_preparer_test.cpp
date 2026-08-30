//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sql_command_preparer_test.cpp
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include <filesystem>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "recovery/single_node_runtime.h"
#include "type/value_factory.h"

namespace bustub {
namespace {

auto SqlRuntimeDirectory() -> std::filesystem::path {
  return std::filesystem::temp_directory_path() / ("bustub-sql-command-preparer-" + std::to_string(getpid()));
}

auto Key(int32_t id) -> EncodedPrimaryKeyV1 { return PrimaryKeyCodecV1::Encode(ValueFactory::GetIntegerValue(id)); }

}  // namespace

// M5-IT04: actual parser/binder/planner statements are expanded privately and only the committed FSM mutates state.
TEST(SqlCommandPreparerTest, AutocommitSqlToCanonicalBatchAndRestart) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto root = SqlRuntimeDirectory();
  storage->RemoveTree(root);
  auto runtime = SingleNodeCommandRuntime::Open(root, storage);

  EXPECT_EQ(WriteResponseCodec::Decode(
                runtime->CommitSql("CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32));", 88, 1))
                .commit_index_,
            1);
  EXPECT_EQ(
      WriteResponseCodec::Decode(runtime->CommitSql("INSERT INTO accounts VALUES (2, 'bob'), (1, 'alice');", 88, 2))
          .commit_index_,
      2);
  EXPECT_EQ(WriteResponseCodec::Decode(runtime->CommitSql("CREATE INDEX accounts_name ON accounts(name);", 88, 3))
                .commit_index_,
            3);
  EXPECT_EQ(WriteResponseCodec::Decode(runtime->CommitSql("UPDATE accounts SET name = 'updated' WHERE id <= 2;", 88, 4))
                .commit_index_,
            4);
  EXPECT_EQ(runtime->GetRow(0, Key(1))->first.ts_, 4);
  EXPECT_EQ(runtime->GetRow(0, Key(2))->first.ts_, 4);
  auto secondary = runtime->CatalogForRead()->GetIndex(1);
  ASSERT_NE(secondary, nullptr);
  Tuple updated_key({ValueFactory::GetVarcharValue("updated")}, &secondary->key_schema_);
  std::vector<RID> matching_rids;
  secondary->index_->ScanKey(updated_key, &matching_rids, nullptr);
  EXPECT_EQ(matching_rids.size(), 2);

  EXPECT_EQ(WriteResponseCodec::Decode(runtime->CommitSql("DELETE FROM accounts WHERE id = 2;", 88, 5)).commit_index_,
            5);
  EXPECT_FALSE(runtime->GetRow(0, Key(2)).has_value());
  secondary->index_->ScanKey(updated_key, &matching_rids, nullptr);
  EXPECT_EQ(matching_rids.size(), 1);

  // A zero-row statement is still a committed request so its exact response can be deduplicated.
  const auto empty_response = runtime->CommitSql("DELETE FROM accounts WHERE id = 999;", 88, 6);
  EXPECT_EQ(WriteResponseCodec::Decode(empty_response).commit_index_, 6);
  EXPECT_EQ(runtime->CommitSql("DELETE FROM accounts WHERE id = 999;", 88, 6), empty_response);
  EXPECT_EQ(runtime->LastLogIndex(), 6);

  // Ordinary failures stay entirely before proposal: no OID, log, commit, row, or SessionTable change.
  EXPECT_THROW(runtime->CommitSql("UPDATE accounts SET id = 9 WHERE id = 1;", 88, 7), std::runtime_error);
  EXPECT_THROW(runtime->CommitSql("CREATE TABLE no_key(value int);", 88, 7), std::runtime_error);
  EXPECT_THROW(runtime->CommitSql("CREATE TABLE composite(a int, b int, PRIMARY KEY(a, b));", 88, 7),
               std::runtime_error);
  EXPECT_THROW(runtime->CommitSql("CREATE TABLE decimal_key(id double PRIMARY KEY);", 88, 7), std::runtime_error);
  EXPECT_THROW(runtime->CommitSql("CREATE UNIQUE INDEX bad_unique ON accounts(name);", 88, 7), std::runtime_error);
  EXPECT_THROW(runtime->CommitSql("INSERT INTO accounts VALUES (NULL, 'bad');", 88, 7), std::runtime_error);
  EXPECT_THROW(runtime->CommitSql("INSERT INTO accounts VALUES (4, random());", 88, 7), std::runtime_error);
  EXPECT_EQ(runtime->CommitIndex(), 6);
  EXPECT_EQ(runtime->LastLogIndex(), 6);
  EXPECT_EQ(runtime->CatalogForRead()->GetNextTableOid(), 1);
  EXPECT_EQ(runtime->CatalogForRead()->GetNextIndexOid(), 2);
  EXPECT_TRUE(runtime->GetRow(0, Key(1)).has_value());

  const auto request_seven = runtime->CommitSql("INSERT INTO accounts VALUES (3, 'carol');", 88, 7);
  EXPECT_EQ(WriteResponseCodec::Decode(request_seven).commit_index_, 7);
  runtime->CreateSnapshot();
  runtime.reset();

  runtime = SingleNodeCommandRuntime::Open(root, storage);
  EXPECT_EQ(runtime->CommitIndex(), 7);
  EXPECT_EQ(runtime->CatalogForRead()->GetSchemaEpoch(), 2);
  EXPECT_NE(runtime->CatalogForRead()->GetIndex(1), nullptr);
  ASSERT_TRUE(runtime->GetRow(0, Key(1)).has_value());
  EXPECT_EQ(runtime->GetRow(0, Key(1))->second.GetValue(&runtime->CatalogForRead()->GetTable(0)->schema_, 1).ToString(),
            "updated");
  EXPECT_FALSE(runtime->GetRow(0, Key(2)).has_value());
  EXPECT_EQ(runtime->GetRow(0, Key(3))->first.ts_, 7);
  secondary = runtime->CatalogForRead()->GetIndex(1);
  ASSERT_NE(secondary, nullptr);
  Tuple recovered_updated_key({ValueFactory::GetVarcharValue("updated")}, &secondary->key_schema_);
  secondary->index_->ScanKey(recovered_updated_key, &matching_rids, nullptr);
  EXPECT_EQ(matching_rids.size(), 1);
  EXPECT_EQ(runtime->CommitSql("INSERT INTO accounts VALUES (3, 'carol');", 88, 7), request_seven);
  EXPECT_EQ(runtime->LastLogIndex(), 7);

  // Advance the database through another real client. The changed retry below is valid SQL against this newer state;
  // it must still be rejected from the retained identity/fingerprint before state-dependent prepare or log append.
  const auto advanced = runtime->CommitSql("UPDATE accounts SET name = 'advanced' WHERE id = 1;", 99, 1);
  EXPECT_EQ(WriteResponseCodec::Decode(advanced).commit_index_, 8);
  ASSERT_TRUE(runtime->GetRow(0, Key(1)).has_value());
  EXPECT_EQ(runtime->GetRow(0, Key(1))->second.GetValue(&runtime->CatalogForRead()->GetTable(0)->schema_, 1).ToString(),
            "advanced");

  const auto expect_changed_payload_rejected = [&] {
    const auto log_before = runtime->LastLogIndex();
    const auto commit_before = runtime->CommitIndex();
    const auto expect_exact_mismatch = [&](const std::string &changed_sql) {
      try {
        static_cast<void>(runtime->CommitSql(changed_sql, 88, 7));
        FAIL() << "changed payload unexpectedly reused a committed request identity";
      } catch (const std::runtime_error &error) {
        EXPECT_STREQ(error.what(), "request payload does not match request identity");
      }
    };
    expect_exact_mismatch("UPDATE accounts SET name = 'must-not-appear' WHERE id = 1;");
    // This is valid, nonempty SQL but its state-dependent prepare would fail because accounts already exists. The
    // stable mismatch error therefore proves identity classification happens before Binder/Catalog preparation.
    expect_exact_mismatch("CREATE TABLE accounts(id int PRIMARY KEY, shadow varchar(32));");
    EXPECT_EQ(runtime->LastLogIndex(), log_before);
    EXPECT_EQ(runtime->CommitIndex(), commit_before);
    EXPECT_EQ(runtime->CatalogForRead()->GetNextTableOid(), 1);
    EXPECT_EQ(runtime->CatalogForRead()->GetNextIndexOid(), 2);
    ASSERT_TRUE(runtime->GetRow(0, Key(1)).has_value());
    EXPECT_EQ(
        runtime->GetRow(0, Key(1))->second.GetValue(&runtime->CatalogForRead()->GetTable(0)->schema_, 1).ToString(),
        "advanced");
    EXPECT_EQ(runtime->CommitSql("INSERT INTO accounts VALUES (3, 'carol');", 88, 7), request_seven);
  };
  expect_changed_payload_rejected();

  runtime->CreateSnapshot();
  runtime.reset();
  runtime = SingleNodeCommandRuntime::Open(root, storage);
  expect_changed_payload_rejected();
  runtime.reset();
  runtime = SingleNodeCommandRuntime::Open(root, storage);
  expect_changed_payload_rejected();

  runtime.reset();
  storage->RemoveTree(root);
}

// M5-IT05: scalar index adapter limits are ordinary admission failures, never committed fail-stop entries.
TEST(SqlCommandPreparerTest, OversizedScalarIndexIsRejectedBeforeProposal) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto root = std::filesystem::temp_directory_path() / ("bustub-sql-index-admission-" + std::to_string(getpid()));
  storage->RemoveTree(root);
  auto runtime = SingleNodeCommandRuntime::Open(root, storage);

  runtime->CommitSql("CREATE TABLE wide(id int PRIMARY KEY, payload varchar(200));", 90, 1);
  EXPECT_THROW(runtime->CommitSql("CREATE INDEX too_wide ON wide(payload);", 90, 2), std::runtime_error);
  EXPECT_THROW(runtime->CommitSql("CREATE TABLE too_wide_pk(id varchar(200) PRIMARY KEY);", 90, 2), std::runtime_error);
  EXPECT_EQ(runtime->CommitIndex(), 1);
  EXPECT_EQ(runtime->LastLogIndex(), 1);
  EXPECT_EQ(runtime->CatalogForRead()->GetNextTableOid(), 1);
  EXPECT_EQ(runtime->CatalogForRead()->GetNextIndexOid(), 1);
  EXPECT_EQ(runtime->CatalogForRead()->GetIndex("too_wide", "wide"), nullptr);

  runtime.reset();
  storage->RemoveTree(root);
}

TEST(SqlCommandPreparerTest, BigIntPrimaryKeyIsPreparedAppliedAndRecovered) {
  auto storage = std::make_shared<PosixDurableStorage>();
  const auto root = std::filesystem::temp_directory_path() / ("bustub-sql-bigint-primary-" + std::to_string(getpid()));
  storage->RemoveTree(root);
  auto runtime = SingleNodeCommandRuntime::Open(root, storage);
  EXPECT_NO_THROW(runtime->CommitSql("CREATE TABLE ledger(code bigint PRIMARY KEY, note varchar(32));", 91, 1));
  EXPECT_NO_THROW(runtime->CommitSql("INSERT INTO ledger VALUES (10, 'ten');", 91, 2));
  const auto key = PrimaryKeyCodecV1::Encode(ValueFactory::GetBigIntValue(10));
  ASSERT_TRUE(runtime->GetRow(0, key).has_value());
  EXPECT_EQ(runtime->GetRow(0, key)->second.GetValue(&runtime->CatalogForRead()->GetTable(0)->schema_, 1).ToString(),
            "ten");
  runtime->CreateSnapshot();
  runtime.reset();

  runtime = SingleNodeCommandRuntime::Open(root, storage);
  ASSERT_TRUE(runtime->GetRow(0, key).has_value());
  EXPECT_EQ(runtime->CatalogForRead()->GetTable(0)->replicated_primary_key_->type_, TypeId::BIGINT);
  runtime.reset();
  storage->RemoveTree(root);
}

}  // namespace bustub
