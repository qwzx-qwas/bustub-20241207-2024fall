//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// client_protocol.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "raft/raft_types.h"

namespace bustub {

enum class ClientReadConsistency : uint32_t { LINEARIZABLE = 1, STALE = 2 };

struct ClientWriteRequestV1 {
  uint64_t client_id_{0};
  uint64_t request_id_{0};
  std::string sql_;
};

struct ClientReadRequestV1 {
  uint64_t request_id_{0};
  ClientReadConsistency consistency_{ClientReadConsistency::LINEARIZABLE};
  std::string sql_;
};

struct ClientStatusRequestV1 {
  uint64_t request_id_{0};
};

using ClientRequestV1 = std::variant<ClientWriteRequestV1, ClientReadRequestV1, ClientStatusRequestV1>;

enum class ClientResponseStatus : uint32_t {
  COMMITTED = 1,
  OK = 2,
  NOT_LEADER = 3,
  REJECTED = 4,
  TIMEOUT = 5,
  UNAVAILABLE = 6,
};

struct ClientResponseV1 {
  uint64_t request_id_{0};
  ClientResponseStatus status_{ClientResponseStatus::UNAVAILABLE};
  NodeId node_id_{0};
  bool leader_ready_{false};
  std::optional<NodeId> leader_id_;
  std::string leader_address_;
  uint64_t term_{0};
  uint64_t commit_index_{0};
  uint64_t last_applied_{0};
  uint64_t published_applied_index_{0};
  uint64_t snapshot_base_index_{0};
  std::optional<uint64_t> read_timestamp_;
  std::vector<std::byte> payload_;
};

struct ClientQueryResultV1 {
  std::vector<std::string> columns_;
  std::vector<std::vector<std::string>> rows_;
};

class ClientQueryResultCodec {
 public:
  static constexpr uint32_t FORMAT_VERSION = 1;
  static constexpr size_t MAX_RESULT_BYTES = 16U * 1024U * 1024U;
  static auto Encode(const ClientQueryResultV1 &result) -> std::vector<std::byte>;
  static auto Decode(const std::vector<std::byte> &bytes) -> ClientQueryResultV1;
};

/** Stable client framing. One TCP connection carries one request frame and one response frame. */
class ClientProtocolCodec {
 public:
  static constexpr uint32_t FORMAT_VERSION = 1;
  static constexpr size_t FRAME_PREFIX_BYTES = 16;
  static constexpr size_t MAX_PAYLOAD_BYTES = 16U * 1024U * 1024U;
  static constexpr size_t MAX_SQL_BYTES = 8U * 1024U * 1024U;

  static auto EncodeRequest(const ClientRequestV1 &request) -> std::vector<std::byte>;
  static auto DecodeRequest(const std::vector<std::byte> &frame) -> ClientRequestV1;
  static auto EncodeResponse(const ClientResponseV1 &response) -> std::vector<std::byte>;
  static auto DecodeResponse(const std::vector<std::byte> &frame) -> ClientResponseV1;
  static auto PayloadSizeFromPrefix(const std::vector<std::byte> &prefix) -> size_t;
};

}  // namespace bustub
