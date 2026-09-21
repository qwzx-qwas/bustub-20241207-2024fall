// Regression for declared SQL payload limits across commit, replay and snapshot.
// This single-node recovery check complements the three-process acceptance suite.

#include <unistd.h>

#include <filesystem>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "recovery/single_node_runtime.h"
#include "type/value_factory.h"

namespace bustub {

TEST(SqlStorageContractTest, PayloadLimitAndResizeSurviveReplayAndSnapshot) {
  const auto root =
      std::filesystem::temp_directory_path() / ("bustub-sql-storage-contract-" + std::to_string(getpid()));
  auto storage = std::make_shared<PosixDurableStorage>();
  storage->RemoveTree(root);
  auto runtime = SingleNodeCommandRuntime::Open(root, storage);
  runtime->CommitSql("CREATE TABLE records(id int PRIMARY KEY, payload varchar(1024));", 100, 1);
  const std::string initial(256, 'a');
  const std::string grown(1024, 'b');
  const std::string shrunk(64, 'c');
  const auto key = PrimaryKeyCodecV1::Encode(ValueFactory::GetIntegerValue(7));
  const auto check = [&](const std::string &expected) {
    const auto row = runtime->GetRow(0, key);
    ASSERT_TRUE(row.has_value());
    const auto table = runtime->CatalogForRead()->GetTable(0);
    ASSERT_NE(table, nullptr);
    EXPECT_EQ(table->schema_.GetColumn(1).GetStorageSize(), 1024);
    EXPECT_EQ(row->second.GetValue(&table->schema_, 1).ToString(), expected);
  };
  runtime->CommitSql("INSERT INTO records VALUES (7, '" + initial + "');", 100, 2);
  runtime->CommitSql("UPDATE records SET payload = '" + grown + "' WHERE id = 7;", 100, 3);
  check(grown);
  runtime.reset();
  runtime = SingleNodeCommandRuntime::Open(root, storage);
  check(grown);

  // Invalid user data must fail before durable proposal, leaving the last
  // confirmed value intact and the next request identity available for reuse.
  const auto committed = runtime->CommitIndex();
  const auto logged = runtime->LastLogIndex();
  EXPECT_THROW(
      runtime->CommitSql("UPDATE records SET payload = '" + std::string(1025, 'x') + "' WHERE id = 7;", 100, 4),
      std::runtime_error);
  EXPECT_EQ(runtime->CommitIndex(), committed);
  EXPECT_EQ(runtime->LastLogIndex(), logged);
  check(grown);
  runtime->CreateSnapshot();
  runtime.reset();
  runtime = SingleNodeCommandRuntime::Open(root, storage);
  check(grown);
  runtime->CommitSql("UPDATE records SET payload = '" + shrunk + "' WHERE id = 7;", 100, 4);
  check(shrunk);
  runtime->CreateSnapshot();
  runtime.reset();
  runtime = SingleNodeCommandRuntime::Open(root, storage);
  check(shrunk);
  runtime.reset();
  storage->RemoveTree(root);
}

}  // namespace bustub
