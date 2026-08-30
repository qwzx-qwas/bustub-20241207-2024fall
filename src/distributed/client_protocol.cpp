//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// client_protocol.cpp
//
//===----------------------------------------------------------------------===//

#include "distributed/client_protocol.h"

#include <array>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "common/byte_codec.h"

namespace bustub {
namespace {

constexpr std::array<std::byte, 8> CLIENT_MAGIC{std::byte{'B'}, std::byte{'C'}, std::byte{'L'}, std::byte{'N'},
                                                std::byte{'T'}, std::byte{'0'}, std::byte{'0'}, std::byte{'1'}};
constexpr std::array<std::byte, 8> RESULT_MAGIC{std::byte{'B'}, std::byte{'Q'}, std::byte{'R'}, std::byte{'E'},
                                                std::byte{'S'}, std::byte{'0'}, std::byte{'0'}, std::byte{'1'}};
constexpr size_t MAX_LEADER_ADDRESS_BYTES = 1024;
constexpr uint32_t MAX_RESULT_COLUMNS = 10000;
constexpr uint32_t MAX_RESULT_ROWS = 1000000;

enum class ClientFrameType : uint32_t { WRITE_REQUEST = 1, READ_REQUEST = 2, STATUS_REQUEST = 3, RESPONSE = 4 };

void PutBool(ByteWriter *writer, bool value) { writer->PutU8(value ? 1 : 0); }

auto ReadBool(ByteReader *reader) -> bool {
  const auto value = reader->ReadU8();
  if (value > 1) {
    throw std::runtime_error("non-canonical client protocol boolean");
  }
  return value != 0;
}

void ValidateSql(const std::string &sql) {
  if (sql.empty() || sql.size() > ClientProtocolCodec::MAX_SQL_BYTES) {
    throw std::runtime_error("client SQL exceeds the V1 boundary");
  }
}

auto IsValidStatus(ClientResponseStatus status) -> bool {
  return status >= ClientResponseStatus::COMMITTED && status <= ClientResponseStatus::UNAVAILABLE;
}

auto Frame(ClientFrameType type, const std::vector<std::byte> &body) -> std::vector<std::byte> {
  ByteWriter payload;
  payload.PutU32(static_cast<uint32_t>(type));
  payload.PutBytes(body);
  if (payload.Data().size() > ClientProtocolCodec::MAX_PAYLOAD_BYTES) {
    throw std::runtime_error("client protocol payload exceeds the V1 limit");
  }
  return EncodeVersionedFrame({CLIENT_MAGIC.data(), CLIENT_MAGIC.size(), ClientProtocolCodec::FORMAT_VERSION,
                               ClientProtocolCodec::MAX_PAYLOAD_BYTES, "client protocol frame"},
                              payload.Data());
}

auto ReadFrame(const std::vector<std::byte> &frame) -> std::pair<ClientFrameType, std::vector<std::byte>> {
  const auto payload =
      DecodeVersionedFrame({CLIENT_MAGIC.data(), CLIENT_MAGIC.size(), ClientProtocolCodec::FORMAT_VERSION,
                            ClientProtocolCodec::MAX_PAYLOAD_BYTES, "client protocol frame"},
                           frame);
  ByteReader body(payload);
  const auto type = static_cast<ClientFrameType>(body.ReadU32());
  return {type, body.ReadBytes(body.Remaining())};
}

}  // namespace

auto ClientProtocolCodec::PayloadSizeFromPrefix(const std::vector<std::byte> &prefix) -> size_t {
  return VersionedFramePayloadSize(
      {CLIENT_MAGIC.data(), CLIENT_MAGIC.size(), FORMAT_VERSION, MAX_PAYLOAD_BYTES, "client protocol frame"}, prefix);
}

auto ClientProtocolCodec::EncodeRequest(const ClientRequestV1 &request) -> std::vector<std::byte> {
  return std::visit(
      [&](const auto &value) {
        using T = std::decay_t<decltype(value)>;
        ByteWriter body;
        if constexpr (std::is_same_v<T, ClientWriteRequestV1>) {
          if (value.client_id_ == 0 || value.request_id_ == 0) {
            throw std::runtime_error("invalid client write identity");
          }
          ValidateSql(value.sql_);
          body.PutU64(value.client_id_);
          body.PutU64(value.request_id_);
          body.PutString(value.sql_);
          return Frame(ClientFrameType::WRITE_REQUEST, body.Data());
        } else if constexpr (std::is_same_v<T, ClientReadRequestV1>) {  // NOLINT(readability/braces)
          if (value.request_id_ == 0 || (value.consistency_ != ClientReadConsistency::LINEARIZABLE &&
                                         value.consistency_ != ClientReadConsistency::STALE)) {
            throw std::runtime_error("invalid client read request");
          }
          ValidateSql(value.sql_);
          body.PutU64(value.request_id_);
          body.PutU32(static_cast<uint32_t>(value.consistency_));
          body.PutString(value.sql_);
          return Frame(ClientFrameType::READ_REQUEST, body.Data());
        } else {
          if (value.request_id_ == 0) {
            throw std::runtime_error("invalid client status request");
          }
          body.PutU64(value.request_id_);
          return Frame(ClientFrameType::STATUS_REQUEST, body.Data());
        }
      },
      request);
}

auto ClientProtocolCodec::DecodeRequest(const std::vector<std::byte> &frame) -> ClientRequestV1 {
  const auto [type, bytes] = ReadFrame(frame);
  ByteReader body(bytes);
  ClientRequestV1 request;
  switch (type) {
    case ClientFrameType::WRITE_REQUEST:
      request = ClientWriteRequestV1{body.ReadU64(), body.ReadU64(), body.ReadString()};
      break;
    case ClientFrameType::READ_REQUEST:
      request =
          ClientReadRequestV1{body.ReadU64(), static_cast<ClientReadConsistency>(body.ReadU32()), body.ReadString()};
      break;
    case ClientFrameType::STATUS_REQUEST:
      request = ClientStatusRequestV1{body.ReadU64()};
      break;
    default:
      throw std::runtime_error("client frame is not a request");
  }
  if (!body.Empty() || EncodeRequest(request) != frame) {
    throw std::runtime_error("non-canonical client request frame");
  }
  return request;
}

auto ClientProtocolCodec::EncodeResponse(const ClientResponseV1 &response) -> std::vector<std::byte> {
  if (response.request_id_ == 0 || response.node_id_ == 0 || !IsValidStatus(response.status_) ||
      response.leader_id_ == std::optional<NodeId>{0} || response.leader_address_.size() > MAX_LEADER_ADDRESS_BYTES ||
      (!response.leader_address_.empty() && !response.leader_id_.has_value()) ||
      response.published_applied_index_ > response.last_applied_ || response.last_applied_ > response.commit_index_ ||
      response.snapshot_base_index_ > response.published_applied_index_ ||
      (response.read_timestamp_.has_value() && *response.read_timestamp_ > response.published_applied_index_) ||
      response.payload_.size() > MAX_PAYLOAD_BYTES) {
    throw std::runtime_error("invalid client response");
  }
  ByteWriter body;
  body.PutU64(response.request_id_);
  body.PutU32(static_cast<uint32_t>(response.status_));
  body.PutU64(response.node_id_);
  PutBool(&body, response.leader_ready_);
  PutBool(&body, response.leader_id_.has_value());
  if (response.leader_id_.has_value()) {
    body.PutU64(*response.leader_id_);
  }
  body.PutString(response.leader_address_);
  body.PutU64(response.term_);
  body.PutU64(response.commit_index_);
  body.PutU64(response.last_applied_);
  body.PutU64(response.published_applied_index_);
  body.PutU64(response.snapshot_base_index_);
  PutBool(&body, response.read_timestamp_.has_value());
  if (response.read_timestamp_.has_value()) {
    body.PutU64(*response.read_timestamp_);
  }
  body.PutU32(static_cast<uint32_t>(response.payload_.size()));
  body.PutBytes(response.payload_);
  return Frame(ClientFrameType::RESPONSE, body.Data());
}

auto ClientProtocolCodec::DecodeResponse(const std::vector<std::byte> &frame) -> ClientResponseV1 {
  const auto [type, bytes] = ReadFrame(frame);
  if (type != ClientFrameType::RESPONSE) {
    throw std::runtime_error("client frame is not a response");
  }
  ByteReader body(bytes);
  ClientResponseV1 response;
  response.request_id_ = body.ReadU64();
  response.status_ = static_cast<ClientResponseStatus>(body.ReadU32());
  response.node_id_ = body.ReadU64();
  response.leader_ready_ = ReadBool(&body);
  if (ReadBool(&body)) {
    response.leader_id_ = body.ReadU64();
  }
  response.leader_address_ = body.ReadString();
  response.term_ = body.ReadU64();
  response.commit_index_ = body.ReadU64();
  response.last_applied_ = body.ReadU64();
  response.published_applied_index_ = body.ReadU64();
  response.snapshot_base_index_ = body.ReadU64();
  if (ReadBool(&body)) {
    response.read_timestamp_ = body.ReadU64();
  }
  const auto payload_size = body.ReadU32();
  if (payload_size > body.Remaining()) {
    throw std::runtime_error("client response payload exceeds its frame");
  }
  response.payload_ = body.ReadBytes(payload_size);
  if (!body.Empty() || EncodeResponse(response) != frame) {
    throw std::runtime_error("non-canonical client response frame");
  }
  return response;
}

auto ClientQueryResultCodec::Encode(const ClientQueryResultV1 &result) -> std::vector<std::byte> {
  if (result.columns_.size() > MAX_RESULT_COLUMNS || result.rows_.size() > MAX_RESULT_ROWS) {
    throw std::runtime_error("client query result shape exceeds the V1 limit");
  }
  ByteWriter body;
  body.PutU32(FORMAT_VERSION);
  body.PutU32(static_cast<uint32_t>(result.columns_.size()));
  for (const auto &column : result.columns_) {
    body.PutString(column);
  }
  body.PutU32(static_cast<uint32_t>(result.rows_.size()));
  for (const auto &row : result.rows_) {
    if (row.size() != result.columns_.size()) {
      throw std::runtime_error("client query result row width mismatch");
    }
    for (const auto &cell : row) {
      body.PutString(cell);
    }
  }
  if (body.Data().size() > MAX_RESULT_BYTES - RESULT_MAGIC.size() - sizeof(uint32_t)) {
    throw std::runtime_error("client query result exceeds the V1 byte limit");
  }
  return EncodeChecksummedFrame(RESULT_MAGIC.data(), RESULT_MAGIC.size(), body.Data(),
                                MAX_RESULT_BYTES - RESULT_MAGIC.size() - sizeof(uint32_t), "client query result");
}

auto ClientQueryResultCodec::Decode(const std::vector<std::byte> &bytes) -> ClientQueryResultV1 {
  if (bytes.size() < RESULT_MAGIC.size() + sizeof(uint32_t) * 4 || bytes.size() > MAX_RESULT_BYTES) {
    throw std::runtime_error("invalid client query result size");
  }
  const auto body_bytes =
      DecodeChecksummedFrame(RESULT_MAGIC.data(), RESULT_MAGIC.size(), bytes,
                             MAX_RESULT_BYTES - RESULT_MAGIC.size() - sizeof(uint32_t), "client query result");
  ByteReader body(body_bytes);
  if (body.ReadU32() != FORMAT_VERSION) {
    throw std::runtime_error("unsupported client query result version");
  }
  const auto column_count = body.ReadU32();
  if (column_count > MAX_RESULT_COLUMNS) {
    throw std::runtime_error("client query result has too many columns");
  }
  ClientQueryResultV1 result;
  result.columns_.reserve(column_count);
  for (uint32_t index = 0; index < column_count; index++) {
    result.columns_.push_back(body.ReadString());
  }
  const auto row_count = body.ReadU32();
  if (row_count > MAX_RESULT_ROWS) {
    throw std::runtime_error("client query result has too many rows");
  }
  result.rows_.reserve(row_count);
  for (uint32_t row_index = 0; row_index < row_count; row_index++) {
    std::vector<std::string> row;
    row.reserve(column_count);
    for (uint32_t column_index = 0; column_index < column_count; column_index++) {
      row.push_back(body.ReadString());
    }
    result.rows_.push_back(std::move(row));
  }
  if (!body.Empty() || Encode(result) != bytes) {
    throw std::runtime_error("non-canonical client query result");
  }
  return result;
}

}  // namespace bustub
