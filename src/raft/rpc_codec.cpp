//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// rpc_codec.cpp
//
//===----------------------------------------------------------------------===//

#include "raft/rpc_codec.h"

#include <array>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "common/byte_codec.h"

namespace bustub {
namespace {

constexpr std::array<std::byte, 8> RPC_MAGIC{std::byte{'B'}, std::byte{'R'}, std::byte{'A'}, std::byte{'F'},
                                             std::byte{'T'}, std::byte{'0'}, std::byte{'0'}, std::byte{'1'}};
constexpr uint32_t MAX_APPEND_ENTRIES = 1000000;
constexpr size_t MAX_SNAPSHOT_CHUNK_BYTES = 64U * 1024U;
constexpr size_t MAX_GROUP_ID_BYTES = 128;

enum class RpcType : uint32_t {
  REQUEST_VOTE_REQUEST = 1,
  REQUEST_VOTE_RESPONSE = 2,
  APPEND_ENTRIES_REQUEST = 3,
  APPEND_ENTRIES_RESPONSE = 4,
  INSTALL_SNAPSHOT_REQUEST = 5,
  INSTALL_SNAPSHOT_RESPONSE = 6,
};

void PutBool(ByteWriter *writer, bool value) { writer->PutU8(value ? 1 : 0); }

auto ReadBool(ByteReader *reader) -> bool {
  const auto value = reader->ReadU8();
  if (value > 1) {
    throw std::runtime_error("non-canonical Raft RPC boolean");
  }
  return value != 0;
}

void PutBlob(ByteWriter *writer, const std::vector<std::byte> &bytes) {
  if (bytes.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("Raft RPC blob is too large");
  }
  writer->PutU32(static_cast<uint32_t>(bytes.size()));
  writer->PutBytes(bytes);
}

auto ReadBlob(ByteReader *reader) -> std::vector<std::byte> {
  const auto size = reader->ReadU32();
  if (size > reader->Remaining()) {
    throw std::runtime_error("Raft RPC blob exceeds its frame");
  }
  return reader->ReadBytes(size);
}

auto EncodeMessage(const RaftMessage &message) -> std::pair<RpcType, std::vector<std::byte>> {
  ByteWriter body;
  return std::visit(
      [&](const auto &value) -> std::pair<RpcType, std::vector<std::byte>> {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, RequestVoteRequest>) {
          body.PutU64(value.term_);
          body.PutU64(value.candidate_id_);
          body.PutU64(value.last_log_index_);
          body.PutU64(value.last_log_term_);
          return {RpcType::REQUEST_VOTE_REQUEST, body.Take()};
        } else if constexpr (std::is_same_v<T, RequestVoteResponse>) {  // NOLINT(readability/braces)
          body.PutU64(value.term_);
          PutBool(&body, value.vote_granted_);
          return {RpcType::REQUEST_VOTE_RESPONSE, body.Take()};
        } else if constexpr (std::is_same_v<T, AppendEntriesRequest>) {  // NOLINT(readability/braces)
          if (value.entries_.size() > MAX_APPEND_ENTRIES) {
            throw std::runtime_error("Raft AppendEntries contains too many entries");
          }
          body.PutU64(value.term_);
          body.PutU64(value.leader_id_);
          body.PutU64(value.request_id_);
          body.PutU64(value.prev_log_index_);
          body.PutU64(value.prev_log_term_);
          body.PutU64(value.leader_commit_);
          PutBool(&body, value.read_context_.has_value());
          if (value.read_context_.has_value()) {
            if (*value.read_context_ == 0) {
              throw std::runtime_error("invalid Raft read context");
            }
            body.PutU64(*value.read_context_);
          }
          body.PutU32(static_cast<uint32_t>(value.entries_.size()));
          for (const auto &entry : value.entries_) {
            PutBlob(&body, LogCodec::Encode(entry));
          }
          return {RpcType::APPEND_ENTRIES_REQUEST, body.Take()};
        } else if constexpr (std::is_same_v<T, AppendEntriesResponse>) {  // NOLINT(readability/braces)
          body.PutU64(value.term_);
          body.PutU64(value.request_id_);
          PutBool(&body, value.success_);
          body.PutU64(value.match_index_);
          PutBool(&body, value.conflict_term_.has_value());
          if (value.conflict_term_.has_value()) {
            body.PutU64(*value.conflict_term_);
          }
          body.PutU64(value.conflict_index_);
          PutBool(&body, value.read_context_.has_value());
          if (value.read_context_.has_value()) {
            if (*value.read_context_ == 0) {
              throw std::runtime_error("invalid Raft read context acknowledgement");
            }
            body.PutU64(*value.read_context_);
          }
          return {RpcType::APPEND_ENTRIES_RESPONSE, body.Take()};
        } else if constexpr (std::is_same_v<T, InstallSnapshotRequest>) {  // NOLINT(readability/braces)
          if (value.snapshot_id_.empty() || value.data_.size() > MAX_SNAPSHOT_CHUNK_BYTES ||
              value.offset_ > value.total_size_ || value.data_.size() > value.total_size_ - value.offset_ ||
              value.done_ != (value.offset_ + value.data_.size() == value.total_size_)) {
            throw std::runtime_error("invalid Raft snapshot chunk");
          }
          body.PutU64(value.term_);
          body.PutU64(value.leader_id_);
          body.PutU64(value.request_id_);
          body.PutString(value.snapshot_id_);
          body.PutU64(value.last_included_index_);
          body.PutU64(value.last_included_term_);
          body.PutU64(value.offset_);
          body.PutU64(value.total_size_);
          body.PutU32(value.payload_checksum_);
          PutBool(&body, value.done_);
          PutBlob(&body, value.data_);
          return {RpcType::INSTALL_SNAPSHOT_REQUEST, body.Take()};
        } else {
          body.PutU64(value.term_);
          body.PutU64(value.request_id_);
          PutBool(&body, value.success_);
          PutBool(&body, value.stale_);
          PutBool(&body, value.complete_);
          body.PutU64(value.match_index_);
          body.PutU64(value.next_offset_);
          return {RpcType::INSTALL_SNAPSHOT_RESPONSE, body.Take()};
        }
      },
      message);
}

auto DecodeMessage(RpcType type, const std::vector<std::byte> &bytes) -> RaftMessage {
  ByteReader body(bytes);
  RaftMessage message;
  switch (type) {
    case RpcType::REQUEST_VOTE_REQUEST:
      message = RequestVoteRequest{body.ReadU64(), body.ReadU64(), body.ReadU64(), body.ReadU64()};
      break;
    case RpcType::REQUEST_VOTE_RESPONSE:
      message = RequestVoteResponse{body.ReadU64(), ReadBool(&body)};
      break;
    case RpcType::APPEND_ENTRIES_REQUEST: {
      AppendEntriesRequest value;
      value.term_ = body.ReadU64();
      value.leader_id_ = body.ReadU64();
      value.request_id_ = body.ReadU64();
      value.prev_log_index_ = body.ReadU64();
      value.prev_log_term_ = body.ReadU64();
      value.leader_commit_ = body.ReadU64();
      if (ReadBool(&body)) {
        value.read_context_ = body.ReadU64();
      }
      const auto count = body.ReadU32();
      if (count > MAX_APPEND_ENTRIES) {
        throw std::runtime_error("Raft AppendEntries count exceeds V1 limit");
      }
      for (uint32_t index = 0; index < count; index++) {
        const auto encoded = ReadBlob(&body);
        const auto decoded = LogCodec::DecodeOne(encoded);
        if (decoded.status_ != LogDecodeStatus::COMPLETE || !decoded.entry_.has_value() ||
            decoded.bytes_consumed_ != encoded.size()) {
          throw std::runtime_error("invalid Raft AppendEntries log frame");
        }
        value.entries_.push_back(*decoded.entry_);
      }
      message = std::move(value);
      break;
    }
    case RpcType::APPEND_ENTRIES_RESPONSE: {
      AppendEntriesResponse value;
      value.term_ = body.ReadU64();
      value.request_id_ = body.ReadU64();
      value.success_ = ReadBool(&body);
      value.match_index_ = body.ReadU64();
      if (ReadBool(&body)) {
        value.conflict_term_ = body.ReadU64();
      }
      value.conflict_index_ = body.ReadU64();
      if (ReadBool(&body)) {
        value.read_context_ = body.ReadU64();
      }
      message = value;
      break;
    }
    case RpcType::INSTALL_SNAPSHOT_REQUEST:
      message = InstallSnapshotRequest{body.ReadU64(), body.ReadU64(),  body.ReadU64(), body.ReadString(),
                                       body.ReadU64(), body.ReadU64(),  body.ReadU64(), body.ReadU64(),
                                       body.ReadU32(), ReadBool(&body), ReadBlob(&body)};
      break;
    case RpcType::INSTALL_SNAPSHOT_RESPONSE:
      message = InstallSnapshotResponse{body.ReadU64(),  body.ReadU64(), ReadBool(&body), ReadBool(&body),
                                        ReadBool(&body), body.ReadU64(), body.ReadU64()};
      break;
    default:
      throw std::runtime_error("unknown Raft RPC message type");
  }
  if (!body.Empty()) {
    throw std::runtime_error("Raft RPC message has trailing bytes");
  }
  static_cast<void>(EncodeMessage(message));
  return message;
}

}  // namespace

auto RaftRpcCodec::PayloadSizeFromPrefix(const std::vector<std::byte> &prefix) -> size_t {
  return VersionedFramePayloadSize(
      {RPC_MAGIC.data(), RPC_MAGIC.size(), FORMAT_VERSION, MAX_PAYLOAD_BYTES, "Raft RPC frame"}, prefix);
}

auto RaftRpcCodec::Encode(const RaftEnvelope &envelope) -> std::vector<std::byte> {
  if (envelope.from_ == 0 || envelope.to_ == 0 || envelope.from_ == envelope.to_ || envelope.group_id_.empty() ||
      envelope.group_id_.size() > MAX_GROUP_ID_BYTES) {
    throw std::runtime_error("invalid Raft RPC envelope endpoints");
  }
  const auto [type, message] = EncodeMessage(envelope.message_);
  ByteWriter payload;
  payload.PutU64(envelope.from_);
  payload.PutU64(envelope.to_);
  payload.PutString(envelope.group_id_);
  payload.PutU32(static_cast<uint32_t>(type));
  payload.PutU32(static_cast<uint32_t>(message.size()));
  payload.PutBytes(message);
  if (payload.Data().size() > MAX_PAYLOAD_BYTES) {
    throw std::runtime_error("Raft RPC payload exceeds V1 limit");
  }
  return EncodeVersionedFrame({RPC_MAGIC.data(), RPC_MAGIC.size(), FORMAT_VERSION, MAX_PAYLOAD_BYTES, "Raft RPC frame"},
                              payload.Data());
}

auto RaftRpcCodec::Decode(const std::vector<std::byte> &frame) -> RaftEnvelope {
  const auto payload = DecodeVersionedFrame(
      {RPC_MAGIC.data(), RPC_MAGIC.size(), FORMAT_VERSION, MAX_PAYLOAD_BYTES, "Raft RPC frame"}, frame);
  ByteReader body(payload);
  RaftEnvelope envelope{body.ReadU64(), body.ReadU64(), {}, body.ReadString()};
  const auto type = static_cast<RpcType>(body.ReadU32());
  const auto message_size = body.ReadU32();
  if (message_size > body.Remaining()) {
    throw std::runtime_error("Raft RPC message exceeds its payload");
  }
  envelope.message_ = DecodeMessage(type, body.ReadBytes(message_size));
  if (envelope.from_ == 0 || envelope.to_ == 0 || envelope.from_ == envelope.to_ || envelope.group_id_.empty() ||
      envelope.group_id_.size() > MAX_GROUP_ID_BYTES || !body.Empty()) {
    throw std::runtime_error("invalid Raft RPC envelope");
  }
  if (Encode(envelope) != frame) {
    throw std::runtime_error("non-canonical Raft RPC frame");
  }
  return envelope;
}

}  // namespace bustub
