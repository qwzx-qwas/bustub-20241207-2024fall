//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// command_codec.cpp
//
//===----------------------------------------------------------------------===//

#include "distributed/command.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <unordered_set>

#include "common/byte_codec.h"
#include "type/value_factory.h"

namespace bustub {
namespace {

constexpr std::array<std::byte, 8> BATCH_MAGIC{std::byte{'B'}, std::byte{'C'}, std::byte{'M'}, std::byte{'D'},
                                               std::byte{'B'}, std::byte{'A'}, std::byte{'T'}, std::byte{'1'}};
constexpr uint32_t MAX_COMMANDS = 1000000;
constexpr uint32_t MAX_COLUMNS = 65536;

enum class CommandType : uint32_t {
  CREATE_TABLE = 1,
  CREATE_INDEX = 2,
  INSERT_ROW = 3,
  UPDATE_ROW = 4,
  DELETE_ROW = 5
};

auto IsKnownType(TypeId type) -> bool {
  return type == TypeId::BOOLEAN || type == TypeId::TINYINT || type == TypeId::SMALLINT || type == TypeId::INTEGER ||
         type == TypeId::BIGINT || type == TypeId::DECIMAL || type == TypeId::VARCHAR || type == TypeId::TIMESTAMP ||
         type == TypeId::VECTOR;
}

auto IsKnownIndexType(IndexType type) -> bool {
  return type == IndexType::BPlusTreeIndex || type == IndexType::HashTableIndex || type == IndexType::STLOrderedIndex ||
         type == IndexType::STLUnorderedIndex || type == IndexType::IVFFlatIndex || type == IndexType::HNSWIndex;
}

void PutBlob(ByteWriter *writer, const std::vector<std::byte> &bytes) {
  if (bytes.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("replicated command blob is too large");
  }
  writer->PutU32(static_cast<uint32_t>(bytes.size()));
  writer->PutBytes(bytes);
}

auto ReadBlob(ByteReader *reader) -> std::vector<std::byte> {
  const auto size = reader->ReadU32();
  if (size > reader->Remaining()) {
    throw std::runtime_error("replicated command blob length exceeds its frame");
  }
  return reader->ReadBytes(size);
}

void EncodePrimaryKey(ByteWriter *writer, const EncodedPrimaryKeyV1 &key) {
  PrimaryKeyCodecV1::Validate(key);
  writer->PutU32(key.codec_version_);
  writer->PutU32(static_cast<uint32_t>(key.type_));
  PutBlob(writer, key.bytes_);
}

auto DecodePrimaryKey(ByteReader *reader) -> EncodedPrimaryKeyV1 {
  EncodedPrimaryKeyV1 key{reader->ReadU32(), static_cast<TypeId>(reader->ReadU32()), ReadBlob(reader)};
  PrimaryKeyCodecV1::Validate(key);
  return key;
}

auto CommandBody(const ReplicatedCommand &command) -> std::pair<CommandType, std::vector<std::byte>> {
  ByteWriter body;
  return std::visit(
      [&](const auto &value) -> std::pair<CommandType, std::vector<std::byte>> {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, CreateTableCommand>) {
          if (value.table_name_.empty() || value.columns_.empty() || value.columns_.size() > MAX_COLUMNS ||
              value.primary_key_.column_oid_ >= value.columns_.size() || value.primary_key_.codec_version_ != 1 ||
              value.columns_[value.primary_key_.column_oid_].nullable_ ||
              value.columns_[value.primary_key_.column_oid_].type_ != value.primary_key_.type_ ||
              !PrimaryKeyCodecV1::IsSupported(value.primary_key_.type_)) {
            throw std::runtime_error("unsupported replicated primary-key table definition");
          }
          body.PutU32(value.table_oid_);
          body.PutU32(value.primary_index_oid_);
          body.PutString(value.table_name_);
          body.PutU32(static_cast<uint32_t>(value.columns_.size()));
          for (const auto &column : value.columns_) {
            if (column.name_.empty() || !IsKnownType(column.type_) || column.storage_size_ == 0) {
              throw std::runtime_error("invalid replicated table column");
            }
            body.PutString(column.name_);
            body.PutU32(static_cast<uint32_t>(column.type_));
            body.PutU32(column.storage_size_);
            body.PutU8(column.nullable_ ? 1 : 0);
          }
          body.PutU32(value.primary_key_.column_oid_);
          body.PutU32(static_cast<uint32_t>(value.primary_key_.type_));
          body.PutU32(value.primary_key_.codec_version_);
          return {CommandType::CREATE_TABLE, body.Take()};
        } else if constexpr (std::is_same_v<T, CreateIndexCommand>) {  // NOLINT(readability/braces)
          if (value.index_name_.empty() || value.key_columns_.empty() || value.key_columns_.size() > MAX_COLUMNS ||
              !IsKnownIndexType(value.index_type_) ||
              value.constraint_kind_ != IndexConstraintKind::NON_UNIQUE_SECONDARY) {
            throw std::runtime_error("unsupported deferred unique or invalid secondary index definition");
          }
          body.PutU32(value.index_oid_);
          body.PutU32(value.table_oid_);
          body.PutString(value.index_name_);
          body.PutU32(static_cast<uint32_t>(value.index_type_));
          body.PutU32(static_cast<uint32_t>(value.constraint_kind_));
          body.PutU32(static_cast<uint32_t>(value.key_columns_.size()));
          for (const auto column : value.key_columns_) {
            body.PutU32(column);
          }
          return {CommandType::CREATE_INDEX, body.Take()};
        } else {
          body.PutU32(value.table_oid_);
          EncodePrimaryKey(&body, value.primary_key_);
          if constexpr (std::is_same_v<T, InsertRowCommand>) {
            PutBlob(&body, value.complete_tuple_);
            return {CommandType::INSERT_ROW, body.Take()};
          } else {
            body.PutU64(value.expected_old_commit_ts_);
            PutBlob(&body, value.expected_old_tuple_);
            if constexpr (std::is_same_v<T, UpdateRowCommand>) {
              PutBlob(&body, value.complete_new_tuple_);
              return {CommandType::UPDATE_ROW, body.Take()};
            } else {
              return {CommandType::DELETE_ROW, body.Take()};
            }
          }
        }
      },
      command);
}

auto DecodeCommand(CommandType type, const std::vector<std::byte> &bytes) -> ReplicatedCommand {
  ByteReader body(bytes);
  ReplicatedCommand command;
  switch (type) {
    case CommandType::CREATE_TABLE: {
      CreateTableCommand value;
      value.table_oid_ = body.ReadU32();
      value.primary_index_oid_ = body.ReadU32();
      value.table_name_ = body.ReadString();
      const auto count = body.ReadU32();
      if (count == 0 || count > MAX_COLUMNS) {
        throw std::runtime_error("invalid replicated table column count");
      }
      value.columns_.reserve(count);
      for (uint32_t index = 0; index < count; index++) {
        ReplicatedColumnDefinition column{body.ReadString(), static_cast<TypeId>(body.ReadU32()), body.ReadU32(),
                                          body.ReadU8() != 0};
        value.columns_.push_back(std::move(column));
      }
      value.primary_key_ = {body.ReadU32(), static_cast<TypeId>(body.ReadU32()), body.ReadU32()};
      command = std::move(value);
      break;
    }
    case CommandType::CREATE_INDEX: {
      CreateIndexCommand value;
      value.index_oid_ = body.ReadU32();
      value.table_oid_ = body.ReadU32();
      value.index_name_ = body.ReadString();
      value.index_type_ = static_cast<IndexType>(body.ReadU32());
      value.constraint_kind_ = static_cast<IndexConstraintKind>(body.ReadU32());
      const auto count = body.ReadU32();
      if (count == 0 || count > MAX_COLUMNS) {
        throw std::runtime_error("invalid replicated index key count");
      }
      for (uint32_t index = 0; index < count; index++) {
        value.key_columns_.push_back(body.ReadU32());
      }
      command = std::move(value);
      break;
    }
    case CommandType::INSERT_ROW:
      command = InsertRowCommand{body.ReadU32(), DecodePrimaryKey(&body), ReadBlob(&body)};
      break;
    case CommandType::UPDATE_ROW:
      command =
          UpdateRowCommand{body.ReadU32(), DecodePrimaryKey(&body), body.ReadU64(), ReadBlob(&body), ReadBlob(&body)};
      break;
    case CommandType::DELETE_ROW:
      command = DeleteRowCommand{body.ReadU32(), DecodePrimaryKey(&body), body.ReadU64(), ReadBlob(&body)};
      break;
    default:
      throw std::runtime_error("unknown replicated command type");
  }
  if (!body.Empty()) {
    throw std::runtime_error("replicated command has trailing bytes");
  }
  static_cast<void>(CommandBody(command));
  return command;
}

auto DmlIdentity(const ReplicatedCommand &command) -> std::optional<std::pair<table_oid_t, EncodedPrimaryKeyV1>> {
  return std::visit(
      [](const auto &value) -> std::optional<std::pair<table_oid_t, EncodedPrimaryKeyV1>> {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, InsertRowCommand> || std::is_same_v<T, UpdateRowCommand> ||
                      std::is_same_v<T, DeleteRowCommand>) {
          return std::pair<table_oid_t, EncodedPrimaryKeyV1>{value.table_oid_, value.primary_key_};
        }
        return std::nullopt;
      },
      command);
}

void ValidateCanonicalCommandOrder(const std::vector<ReplicatedCommand> &commands) {
  if (commands.empty()) {
    return;
  }
  const bool is_ddl = std::holds_alternative<CreateTableCommand>(commands.front()) ||
                      std::holds_alternative<CreateIndexCommand>(commands.front());
  if (is_ddl) {
    if (commands.size() != 1) {
      throw std::runtime_error("V1 permits exactly one DDL command per batch");
    }
    return;
  }
  for (size_t index = 0; index < commands.size(); index++) {
    const auto current = DmlIdentity(commands[index]);
    if (!current.has_value()) {
      throw std::runtime_error("cannot mix DDL and DML in a V1 batch");
    }
    if (index == 0) {
      continue;
    }
    const auto previous = *DmlIdentity(commands[index - 1]);
    const auto key_order = previous.first == current->first
                               ? PrimaryKeyCodecV1::CanonicalCompare(previous.second, current->second)
                               : (previous.first < current->first ? -1 : 1);
    if (key_order == 0) {
      throw std::runtime_error("duplicate logical row mutation in TransactionCommandBatch");
    }
    if (key_order > 0) {
      throw std::runtime_error("TransactionCommandBatch DML commands are not in canonical order");
    }
  }
}

}  // namespace

auto PrimaryKeyCodecV1::IsSupported(TypeId type) -> bool {
  return type == TypeId::INTEGER || type == TypeId::BIGINT || type == TypeId::VARCHAR;
}

auto PrimaryKeyCodecV1::EncodeInteger(int32_t value) -> EncodedPrimaryKeyV1 {
  ByteWriter writer;
  writer.PutU32(static_cast<uint32_t>(value));
  return {FORMAT_VERSION, TypeId::INTEGER, writer.Take()};
}

auto PrimaryKeyCodecV1::EncodeBigInt(int64_t value) -> EncodedPrimaryKeyV1 {
  ByteWriter writer;
  writer.PutU64(static_cast<uint64_t>(value));
  return {FORMAT_VERSION, TypeId::BIGINT, writer.Take()};
}

auto PrimaryKeyCodecV1::EncodeVarchar(std::string_view value) -> EncodedPrimaryKeyV1 {
  if (value.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("VARCHAR primary key exceeds V1 size limit");
  }
  ByteWriter writer;
  writer.PutU32(static_cast<uint32_t>(value.size()));
  writer.PutBytes(value.data(), value.size());
  return {FORMAT_VERSION, TypeId::VARCHAR, writer.Take()};
}

auto PrimaryKeyCodecV1::Encode(const Value &value) -> EncodedPrimaryKeyV1 {
  if (value.IsNull() || !IsSupported(value.GetTypeId())) {
    throw std::runtime_error("UNSUPPORTED_REPLICATED_PRIMARY_KEY");
  }
  if (value.GetTypeId() == TypeId::INTEGER) {
    return EncodeInteger(value.GetAs<int32_t>());
  }
  if (value.GetTypeId() == TypeId::BIGINT) {
    return EncodeBigInt(value.GetAs<int64_t>());
  }
  const auto storage_size = value.GetStorageSize();
  if (storage_size == 0) {
    throw std::runtime_error("invalid VARCHAR primary key storage");
  }
  return EncodeVarchar(std::string_view(value.GetData(), storage_size - 1));
}

void PrimaryKeyCodecV1::Validate(const EncodedPrimaryKeyV1 &key) {
  if (key.codec_version_ != FORMAT_VERSION || !IsSupported(key.type_)) {
    throw std::runtime_error("UNSUPPORTED_REPLICATED_PRIMARY_KEY");
  }
  if ((key.type_ == TypeId::INTEGER && key.bytes_.size() != 4) ||
      (key.type_ == TypeId::BIGINT && key.bytes_.size() != 8)) {
    throw std::runtime_error("invalid fixed-width primary key");
  }
  if (key.type_ == TypeId::VARCHAR) {
    if (key.bytes_.size() < 4) {
      throw std::runtime_error("invalid VARCHAR primary key");
    }
    ByteReader reader(key.bytes_);
    if (reader.ReadU32() != reader.Remaining()) {
      throw std::runtime_error("invalid VARCHAR primary-key length");
    }
  }
}

auto PrimaryKeyCodecV1::Decode(const EncodedPrimaryKeyV1 &key) -> Value {
  Validate(key);
  ByteReader reader(key.bytes_);
  if (key.type_ == TypeId::INTEGER) {
    const auto bits = reader.ReadU32();
    const auto value = bits <= static_cast<uint32_t>(std::numeric_limits<int32_t>::max())
                           ? static_cast<int64_t>(bits)
                           : -1 - static_cast<int64_t>(std::numeric_limits<uint32_t>::max() - bits);
    return ValueFactory::GetIntegerValue(static_cast<int32_t>(value));
  }
  if (key.type_ == TypeId::BIGINT) {
    const auto bits = reader.ReadU64();
    const auto value = bits <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())
                           ? static_cast<int64_t>(bits)
                           : -1 - static_cast<int64_t>(std::numeric_limits<uint64_t>::max() - bits);
    return ValueFactory::GetBigIntValue(value);
  }
  const auto length = reader.ReadU32();
  const auto raw = reader.ReadBytes(length);
  std::string value(reinterpret_cast<const char *>(raw.data()), raw.size());
  return ValueFactory::GetVarcharValue(value);
}

auto PrimaryKeyCodecV1::CanonicalCompare(const EncodedPrimaryKeyV1 &lhs, const EncodedPrimaryKeyV1 &rhs) -> int {
  Validate(lhs);
  Validate(rhs);
  if (lhs.type_ != rhs.type_) {
    throw std::runtime_error("cannot compare different primary-key types");
  }
  if (lhs.type_ == TypeId::INTEGER) {
    ByteReader left(lhs.bytes_);
    ByteReader right(rhs.bytes_);
    const auto a = left.ReadU32() ^ 0x80000000U;
    const auto b = right.ReadU32() ^ 0x80000000U;
    return a < b ? -1 : (a > b ? 1 : 0);
  }
  if (lhs.type_ == TypeId::BIGINT) {
    ByteReader left(lhs.bytes_);
    ByteReader right(rhs.bytes_);
    const auto a = left.ReadU64() ^ 0x8000000000000000ULL;
    const auto b = right.ReadU64() ^ 0x8000000000000000ULL;
    return a < b ? -1 : (a > b ? 1 : 0);
  }
  const auto first = lhs.bytes_.begin() + 4;
  const auto second = rhs.bytes_.begin() + 4;
  if (std::lexicographical_compare(first, lhs.bytes_.end(), second, rhs.bytes_.end())) {
    return -1;
  }
  if (std::lexicographical_compare(second, rhs.bytes_.end(), first, lhs.bytes_.end())) {
    return 1;
  }
  return 0;
}

auto TupleCodecV1::Encode(const Tuple &tuple, const Schema &schema) -> std::vector<std::byte> {
  ByteWriter writer;
  writer.PutU32(FORMAT_VERSION);
  writer.PutU32(schema.GetColumnCount());
  for (uint32_t column = 0; column < schema.GetColumnCount(); column++) {
    const auto type = schema.GetColumn(column).GetType();
    const auto value = tuple.GetValue(&schema, column);
    writer.PutU32(static_cast<uint32_t>(type));
    writer.PutU8(value.IsNull() ? 1 : 0);
    if (value.IsNull()) {
      continue;
    }
    switch (type) {
      case TypeId::BOOLEAN:
        writer.PutU8(static_cast<uint8_t>(value.GetAs<int8_t>()));
        break;
      case TypeId::TINYINT:
        writer.PutU8(static_cast<uint8_t>(value.GetAs<int8_t>()));
        break;
      case TypeId::SMALLINT:
        writer.PutU32(static_cast<uint16_t>(value.GetAs<int16_t>()));
        break;
      case TypeId::INTEGER:
        writer.PutU32(static_cast<uint32_t>(value.GetAs<int32_t>()));
        break;
      case TypeId::BIGINT:
        writer.PutU64(static_cast<uint64_t>(value.GetAs<int64_t>()));
        break;
      case TypeId::TIMESTAMP:
        writer.PutU64(value.GetAs<uint64_t>());
        break;
      case TypeId::DECIMAL: {
        uint64_t bits;
        const auto decimal = value.GetAs<double>();
        static_assert(sizeof(bits) == sizeof(decimal));
        std::memcpy(&bits, &decimal, sizeof(bits));
        writer.PutU64(bits);
        break;
      }
      case TypeId::VARCHAR: {
        const auto size = value.GetStorageSize();
        if (size == 0) {
          throw std::runtime_error("invalid VARCHAR tuple value");
        }
        writer.PutU32(size - 1);
        writer.PutBytes(value.GetData(), size - 1);
        break;
      }
      case TypeId::VECTOR: {
        const auto vector = value.GetVector();
        if (vector.size() > std::numeric_limits<uint32_t>::max()) {
          throw std::runtime_error("vector tuple value is too large");
        }
        writer.PutU32(static_cast<uint32_t>(vector.size()));
        for (const auto element : vector) {
          uint64_t bits;
          std::memcpy(&bits, &element, sizeof(bits));
          writer.PutU64(bits);
        }
        break;
      }
      default:
        throw std::runtime_error("unsupported tuple value type");
    }
  }
  return writer.Take();
}

auto TupleCodecV1::Decode(const std::vector<std::byte> &bytes, const Schema &schema) -> Tuple {
  if (bytes.size() > CommandBatchCodec::MAX_BATCH_BYTES) {
    throw std::runtime_error("encoded tuple exceeds V1 size limit");
  }
  ByteReader reader(bytes);
  if (reader.ReadU32() != FORMAT_VERSION || reader.ReadU32() != schema.GetColumnCount()) {
    throw std::runtime_error("encoded tuple schema mismatch");
  }
  std::vector<Value> values;
  values.reserve(schema.GetColumnCount());
  for (uint32_t column = 0; column < schema.GetColumnCount(); column++) {
    const auto type = static_cast<TypeId>(reader.ReadU32());
    const auto null_marker = reader.ReadU8();
    if (type != schema.GetColumn(column).GetType() || null_marker > 1) {
      throw std::runtime_error("encoded tuple column mismatch");
    }
    if (null_marker == 1) {
      values.emplace_back(type);
      continue;
    }
    switch (type) {
      case TypeId::BOOLEAN: {
        const auto raw = reader.ReadU8();
        if (raw > 1) {
          throw std::runtime_error("invalid encoded BOOLEAN");
        }
        values.push_back(ValueFactory::GetBooleanValue(raw != 0));
        break;
      }
      case TypeId::TINYINT: {
        const auto raw = reader.ReadU8();
        const auto value = raw <= 0x7f ? static_cast<int16_t>(raw) : -1 - static_cast<int16_t>(0xff - raw);
        values.push_back(ValueFactory::GetTinyIntValue(static_cast<int8_t>(value)));
        break;
      }
      case TypeId::SMALLINT: {
        const auto raw = reader.ReadU32();
        if (raw > std::numeric_limits<uint16_t>::max()) {
          throw std::runtime_error("invalid encoded SMALLINT");
        }
        const auto value = raw <= 0x7fffU ? static_cast<int32_t>(raw) : -1 - static_cast<int32_t>(0xffffU - raw);
        values.push_back(ValueFactory::GetSmallIntValue(static_cast<int16_t>(value)));
        break;
      }
      case TypeId::INTEGER: {
        const auto raw = reader.ReadU32();
        const auto value = raw <= static_cast<uint32_t>(std::numeric_limits<int32_t>::max())
                               ? static_cast<int64_t>(raw)
                               : -1 - static_cast<int64_t>(std::numeric_limits<uint32_t>::max() - raw);
        values.push_back(ValueFactory::GetIntegerValue(static_cast<int32_t>(value)));
        break;
      }
      case TypeId::BIGINT: {
        const auto raw = reader.ReadU64();
        const auto value = raw <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())
                               ? static_cast<int64_t>(raw)
                               : -1 - static_cast<int64_t>(std::numeric_limits<uint64_t>::max() - raw);
        values.push_back(ValueFactory::GetBigIntValue(value));
        break;
      }
      case TypeId::TIMESTAMP:
        values.emplace_back(TypeId::TIMESTAMP, reader.ReadU64());
        break;
      case TypeId::DECIMAL: {
        const auto bits = reader.ReadU64();
        double value;
        std::memcpy(&value, &bits, sizeof(value));
        values.push_back(ValueFactory::GetDecimalValue(value));
        break;
      }
      case TypeId::VARCHAR: {
        const auto size = reader.ReadU32();
        if (size > reader.Remaining() || size + 1 > schema.GetColumn(column).GetStorageSize()) {
          throw std::runtime_error("encoded VARCHAR exceeds its schema");
        }
        const auto raw = reader.ReadBytes(size);
        values.emplace_back(TypeId::VARCHAR, std::string(reinterpret_cast<const char *>(raw.data()), raw.size()));
        break;
      }
      case TypeId::VECTOR: {
        const auto count = reader.ReadU32();
        if (count > reader.Remaining() / sizeof(uint64_t) ||
            count * sizeof(double) > schema.GetColumn(column).GetStorageSize()) {
          throw std::runtime_error("encoded VECTOR exceeds its schema");
        }
        std::vector<double> vector;
        vector.reserve(count);
        for (uint32_t index = 0; index < count; index++) {
          const auto bits = reader.ReadU64();
          double element;
          std::memcpy(&element, &bits, sizeof(element));
          vector.push_back(element);
        }
        values.push_back(ValueFactory::GetVectorValue(vector));
        break;
      }
      default:
        throw std::runtime_error("unsupported encoded tuple type");
    }
  }
  if (!reader.Empty()) {
    throw std::runtime_error("encoded tuple has trailing bytes");
  }
  Tuple tuple(std::move(values), &schema);
  if (Encode(tuple, schema) != bytes) {
    throw std::runtime_error("non-canonical encoded tuple");
  }
  return tuple;
}

auto CommandBatchCodec::Encode(const TransactionCommandBatch &batch) -> std::vector<std::byte> {
  if (batch.format_version_ != FORMAT_VERSION || batch.client_id_ == 0 || batch.request_id_ == 0 ||
      batch.commands_.size() > MAX_COMMANDS) {
    throw std::runtime_error("invalid TransactionCommandBatch");
  }
  ValidateCanonicalCommandOrder(batch.commands_);
  ByteWriter payload;
  payload.PutU64(batch.client_id_);
  payload.PutU64(batch.request_id_);
  payload.PutU64(batch.expected_start_schema_epoch_);
  payload.PutU32(static_cast<uint32_t>(batch.commands_.size()));
  for (const auto &command : batch.commands_) {
    auto [type, body] = CommandBody(command);
    payload.PutU32(static_cast<uint32_t>(type));
    PutBlob(&payload, body);
  }
  if (payload.Data().size() > MAX_BATCH_BYTES) {
    throw std::runtime_error("TransactionCommandBatch exceeds V1 size limit");
  }
  return EncodeVersionedFrame(
      {BATCH_MAGIC.data(), BATCH_MAGIC.size(), FORMAT_VERSION, MAX_BATCH_BYTES, "TransactionCommandBatch"},
      payload.Data());
}

auto CommandBatchCodec::Decode(const std::vector<std::byte> &bytes) -> TransactionCommandBatch {
  const auto payload = DecodeVersionedFrame(
      {BATCH_MAGIC.data(), BATCH_MAGIC.size(), FORMAT_VERSION, MAX_BATCH_BYTES, "TransactionCommandBatch"}, bytes);
  ByteReader body(payload);
  TransactionCommandBatch batch;
  batch.format_version_ = FORMAT_VERSION;
  batch.client_id_ = body.ReadU64();
  batch.request_id_ = body.ReadU64();
  batch.expected_start_schema_epoch_ = body.ReadU64();
  const auto command_count = body.ReadU32();
  if (command_count > MAX_COMMANDS) {
    throw std::runtime_error("invalid TransactionCommandBatch command count");
  }
  for (uint32_t index = 0; index < command_count; index++) {
    batch.commands_.push_back(DecodeCommand(static_cast<CommandType>(body.ReadU32()), ReadBlob(&body)));
  }
  if (!body.Empty()) {
    throw std::runtime_error("TransactionCommandBatch has trailing bytes");
  }
  if (Encode(batch) != bytes) {
    throw std::runtime_error("non-canonical TransactionCommandBatch encoding");
  }
  return batch;
}

auto CommandBuilder::Build(uint64_t client_id, uint64_t request_id, uint64_t expected_start_schema_epoch,
                           std::vector<ReplicatedCommand> commands) -> TransactionCommandBatch {
  if (commands.empty()) {
    TransactionCommandBatch batch{1, client_id, request_id, expected_start_schema_epoch, {}};
    static_cast<void>(CommandBatchCodec::Encode(batch));
    return batch;
  }
  if (!std::holds_alternative<CreateTableCommand>(commands.front()) &&
      !std::holds_alternative<CreateIndexCommand>(commands.front())) {
    for (const auto &command : commands) {
      if (!DmlIdentity(command).has_value()) {
        throw std::runtime_error("cannot mix DDL and DML in a V1 batch");
      }
    }
    std::sort(commands.begin(), commands.end(), [](const auto &lhs, const auto &rhs) {
      const auto left = *DmlIdentity(lhs);
      const auto right = *DmlIdentity(rhs);
      if (left.first != right.first) {
        return left.first < right.first;
      }
      return PrimaryKeyCodecV1::CanonicalCompare(left.second, right.second) < 0;
    });
  }
  ValidateCanonicalCommandOrder(commands);
  TransactionCommandBatch batch{1, client_id, request_id, expected_start_schema_epoch, std::move(commands)};
  static_cast<void>(CommandBatchCodec::Encode(batch));
  return batch;
}

}  // namespace bustub
