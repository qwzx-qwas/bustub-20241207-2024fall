//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// command.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "catalog/catalog.h"
#include "distributed/request_fingerprint.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

struct EncodedPrimaryKeyV1 {
  uint32_t codec_version_{1};
  TypeId type_{TypeId::INVALID};
  std::vector<std::byte> bytes_;

  friend auto operator==(const EncodedPrimaryKeyV1 &lhs, const EncodedPrimaryKeyV1 &rhs) -> bool {
    return lhs.codec_version_ == rhs.codec_version_ && lhs.type_ == rhs.type_ && lhs.bytes_ == rhs.bytes_;
  }
};

class PrimaryKeyCodecV1 {
 public:
  static constexpr uint32_t FORMAT_VERSION = 1;
  static auto IsSupported(TypeId type) -> bool;
  static auto EncodeInteger(int32_t value) -> EncodedPrimaryKeyV1;
  static auto EncodeBigInt(int64_t value) -> EncodedPrimaryKeyV1;
  static auto EncodeVarchar(std::string_view value) -> EncodedPrimaryKeyV1;
  static auto Encode(const Value &value) -> EncodedPrimaryKeyV1;
  static auto Decode(const EncodedPrimaryKeyV1 &key) -> Value;
  static auto CanonicalCompare(const EncodedPrimaryKeyV1 &lhs, const EncodedPrimaryKeyV1 &rhs) -> int;
  static void Validate(const EncodedPrimaryKeyV1 &key);
};

class TupleCodecV1 {
 public:
  static constexpr uint32_t FORMAT_VERSION = 1;
  static auto Encode(const Tuple &tuple, const Schema &schema) -> std::vector<std::byte>;
  static auto Decode(const std::vector<std::byte> &bytes, const Schema &schema) -> Tuple;
};

struct ReplicatedColumnDefinition {
  std::string name_;
  TypeId type_{TypeId::INVALID};
  uint32_t storage_size_{0};
  bool nullable_{true};

  friend auto operator==(const ReplicatedColumnDefinition &lhs, const ReplicatedColumnDefinition &rhs) -> bool {
    return lhs.name_ == rhs.name_ && lhs.type_ == rhs.type_ && lhs.storage_size_ == rhs.storage_size_ &&
           lhs.nullable_ == rhs.nullable_;
  }
};

struct CreateTableCommand {
  table_oid_t table_oid_{0};
  index_oid_t primary_index_oid_{0};
  std::string table_name_;
  std::vector<ReplicatedColumnDefinition> columns_;
  ReplicatedPrimaryKeyDefinition primary_key_{};
  friend auto operator==(const CreateTableCommand &lhs, const CreateTableCommand &rhs) -> bool {
    return lhs.table_oid_ == rhs.table_oid_ && lhs.primary_index_oid_ == rhs.primary_index_oid_ &&
           lhs.table_name_ == rhs.table_name_ && lhs.columns_ == rhs.columns_ && lhs.primary_key_ == rhs.primary_key_;
  }
};

struct CreateIndexCommand {
  index_oid_t index_oid_{0};
  table_oid_t table_oid_{0};
  std::string index_name_;
  std::vector<uint32_t> key_columns_;
  IndexType index_type_{IndexType::BPlusTreeIndex};
  IndexConstraintKind constraint_kind_{IndexConstraintKind::NON_UNIQUE_SECONDARY};
  friend auto operator==(const CreateIndexCommand &lhs, const CreateIndexCommand &rhs) -> bool {
    return lhs.index_oid_ == rhs.index_oid_ && lhs.table_oid_ == rhs.table_oid_ && lhs.index_name_ == rhs.index_name_ &&
           lhs.key_columns_ == rhs.key_columns_ && lhs.index_type_ == rhs.index_type_ &&
           lhs.constraint_kind_ == rhs.constraint_kind_;
  }
};

struct InsertRowCommand {
  table_oid_t table_oid_{0};
  EncodedPrimaryKeyV1 primary_key_;
  std::vector<std::byte> complete_tuple_;
  friend auto operator==(const InsertRowCommand &lhs, const InsertRowCommand &rhs) -> bool {
    return lhs.table_oid_ == rhs.table_oid_ && lhs.primary_key_ == rhs.primary_key_ &&
           lhs.complete_tuple_ == rhs.complete_tuple_;
  }
};

struct UpdateRowCommand {
  table_oid_t table_oid_{0};
  EncodedPrimaryKeyV1 primary_key_;
  uint64_t expected_old_commit_ts_{0};
  std::vector<std::byte> expected_old_tuple_;
  std::vector<std::byte> complete_new_tuple_;
  friend auto operator==(const UpdateRowCommand &lhs, const UpdateRowCommand &rhs) -> bool {
    return lhs.table_oid_ == rhs.table_oid_ && lhs.primary_key_ == rhs.primary_key_ &&
           lhs.expected_old_commit_ts_ == rhs.expected_old_commit_ts_ &&
           lhs.expected_old_tuple_ == rhs.expected_old_tuple_ && lhs.complete_new_tuple_ == rhs.complete_new_tuple_;
  }
};

struct DeleteRowCommand {
  table_oid_t table_oid_{0};
  EncodedPrimaryKeyV1 primary_key_;
  uint64_t expected_old_commit_ts_{0};
  std::vector<std::byte> expected_old_tuple_;
  friend auto operator==(const DeleteRowCommand &lhs, const DeleteRowCommand &rhs) -> bool {
    return lhs.table_oid_ == rhs.table_oid_ && lhs.primary_key_ == rhs.primary_key_ &&
           lhs.expected_old_commit_ts_ == rhs.expected_old_commit_ts_ &&
           lhs.expected_old_tuple_ == rhs.expected_old_tuple_;
  }
};

using ReplicatedCommand =
    std::variant<CreateTableCommand, CreateIndexCommand, InsertRowCommand, UpdateRowCommand, DeleteRowCommand>;

struct TransactionCommandBatch {
  uint32_t format_version_{2};
  uint64_t client_id_{0};
  uint64_t request_id_{0};
  RequestFingerprintV1 request_fingerprint_{};
  uint64_t expected_start_schema_epoch_{0};
  std::vector<ReplicatedCommand> commands_;

  friend auto operator==(const TransactionCommandBatch &lhs, const TransactionCommandBatch &rhs) -> bool {
    return lhs.format_version_ == rhs.format_version_ && lhs.client_id_ == rhs.client_id_ &&
           lhs.request_id_ == rhs.request_id_ && lhs.request_fingerprint_ == rhs.request_fingerprint_ &&
           lhs.expected_start_schema_epoch_ == rhs.expected_start_schema_epoch_ && lhs.commands_ == rhs.commands_;
  }
};

class CommandBatchCodec {
 public:
  static constexpr uint32_t FORMAT_VERSION = 2;
  /** The complete versioned CommandBatch frame must fit one replicated log-entry payload. */
  static constexpr size_t FRAME_OVERHEAD_BYTES = 8U + sizeof(uint32_t) * 3U;
  static constexpr size_t MAX_ENCODED_BATCH_BYTES = 64U * 1024U * 1024U;
  static constexpr size_t MAX_BATCH_BYTES = MAX_ENCODED_BATCH_BYTES - FRAME_OVERHEAD_BYTES;
  static auto Encode(const TransactionCommandBatch &batch) -> std::vector<std::byte>;
  static auto Decode(const std::vector<std::byte> &bytes) -> TransactionCommandBatch;
};

class CommandBuilder {
 public:
  static auto Build(uint64_t client_id, uint64_t request_id, const RequestFingerprintV1 &request_fingerprint,
                    uint64_t expected_start_schema_epoch, std::vector<ReplicatedCommand> commands)
      -> TransactionCommandBatch;
};

}  // namespace bustub
