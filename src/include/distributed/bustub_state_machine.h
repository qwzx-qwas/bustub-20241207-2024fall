//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// bustub_state_machine.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "distributed/command.h"
#include "distributed/session_table.h"
#include "distributed/state_visibility.h"
#include "recovery/log_codec.h"

namespace bustub {

/** Deterministic committed-entry Apply path for the replicated BusTub state. */
class BusTubStateMachine {
 public:
  BusTubStateMachine(Catalog *catalog, SessionTable *sessions, StateVisibilityLatch *visibility,
                     uint64_t snapshot_index = 0);

  /** Read-only leader/single-node admission check. Ordinary business failures must be rejected before logging. */
  void ValidateProposal(const TransactionCommandBatch &batch) const;
  void Apply(const ReplicatedLogEntry &entry);

  auto LastApplied() const -> uint64_t { return last_applied_; }
  auto PublishedAppliedIndex() const -> uint64_t { return published_applied_index_; }
  auto IsStopped() const -> bool { return stopped_; }

  auto GetRow(table_oid_t table_oid, const EncodedPrimaryKeyV1 &primary_key) const
      -> std::optional<std::pair<TupleMeta, Tuple>>;
  auto GetLastResponse(uint64_t client_id) const -> std::optional<std::vector<std::byte>>;

 private:
  struct LocatedRow {
    std::shared_ptr<TableInfo> table_;
    std::shared_ptr<IndexInfo> primary_index_;
    RID rid_;
    TupleMeta meta_;
    Tuple tuple_;
  };

  void ApplyBatch(const ReplicatedLogEntry &entry, const TransactionCommandBatch &batch);
  void ApplyCommand(const ReplicatedLogEntry &entry, const ReplicatedCommand &command);
  void ApplyCreateTable(const CreateTableCommand &command);
  void ApplyCreateIndex(const CreateIndexCommand &command);
  void ApplyInsert(const ReplicatedLogEntry &entry, const InsertRowCommand &command);
  void ApplyUpdate(const ReplicatedLogEntry &entry, const UpdateRowCommand &command);
  void ApplyDelete(const ReplicatedLogEntry &entry, const DeleteRowCommand &command);

  auto LocateRow(table_oid_t table_oid, const EncodedPrimaryKeyV1 &primary_key) const -> std::optional<LocatedRow>;
  auto ValidateTableKey(const std::shared_ptr<TableInfo> &table, const EncodedPrimaryKeyV1 &primary_key) const
      -> std::shared_ptr<IndexInfo>;
  auto SortedIndexes(const std::shared_ptr<TableInfo> &table) const -> std::vector<std::shared_ptr<IndexInfo>>;
  void InsertAllIndexes(const std::shared_ptr<TableInfo> &table, const Tuple &tuple, RID rid);
  void DeleteAllIndexes(const std::shared_ptr<TableInfo> &table, const Tuple &tuple, RID rid);
  static auto DecodeSchema(const std::vector<ReplicatedColumnDefinition> &columns) -> Schema;
  static void ValidateTupleKey(const std::shared_ptr<TableInfo> &table, const EncodedPrimaryKeyV1 &primary_key,
                               const Tuple &tuple);

  Catalog *catalog_;
  SessionTable *sessions_;
  StateVisibilityLatch *visibility_;
  uint64_t last_applied_;
  uint64_t published_applied_index_;
  bool stopped_{false};
};

}  // namespace bustub
