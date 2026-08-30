//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_codec.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace bustub {

enum class EntryType : uint32_t { COMMAND_BATCH = 1, NOOP = 2, KV_COMMAND = 3 };

struct ReplicatedLogEntry {
  uint32_t format_version_{1};
  uint64_t index_{0};
  uint64_t term_{0};
  EntryType type_{EntryType::COMMAND_BATCH};
  std::vector<std::byte> payload_;

  friend auto operator==(const ReplicatedLogEntry &lhs, const ReplicatedLogEntry &rhs) -> bool {
    return lhs.format_version_ == rhs.format_version_ && lhs.index_ == rhs.index_ && lhs.term_ == rhs.term_ &&
           lhs.type_ == rhs.type_ && lhs.payload_ == rhs.payload_;
  }
};

enum class LogDecodeStatus { COMPLETE, TRUNCATED, CORRUPT };

struct LogDecodeResult {
  LogDecodeStatus status_;
  std::optional<ReplicatedLogEntry> entry_;
  size_t bytes_consumed_{0};
  std::string error_;
};

class LogCodec {
 public:
  static constexpr uint32_t FORMAT_VERSION = 1;
  static constexpr size_t MAX_PAYLOAD_BYTES = 64U * 1024U * 1024U;
  static constexpr size_t FRAME_HEADER_BYTES = 8;
  static constexpr size_t FRAME_BODY_FIXED_BYTES = 32;

  static auto Encode(const ReplicatedLogEntry &entry) -> std::vector<std::byte>;
  static auto DecodeOne(const std::byte *data, size_t size) -> LogDecodeResult;
  static auto DecodeOne(const std::vector<std::byte> &data, size_t offset = 0) -> LogDecodeResult;
};

}  // namespace bustub
