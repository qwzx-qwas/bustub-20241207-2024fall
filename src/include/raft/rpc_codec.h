//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// rpc_codec.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "raft/raft_types.h"

namespace bustub {

/** Stable, checksummed framing for production Raft TCP messages. */
class RaftRpcCodec {
 public:
  static constexpr uint32_t FORMAT_VERSION = 1;
  static constexpr size_t FRAME_PREFIX_BYTES = 16;
  static constexpr size_t MAX_PAYLOAD_BYTES = 128U * 1024U * 1024U;

  static auto Encode(const RaftEnvelope &envelope) -> std::vector<std::byte>;
  static auto Decode(const std::vector<std::byte> &frame) -> RaftEnvelope;

  /** Validate a 16-byte prefix and return payload bytes (the trailing CRC is not included). */
  static auto PayloadSizeFromPrefix(const std::vector<std::byte> &prefix) -> size_t;
};

}  // namespace bustub
