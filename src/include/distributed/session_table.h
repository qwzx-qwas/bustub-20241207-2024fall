//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// session_table.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <shared_mutex>
#include <vector>

#include "distributed/request_fingerprint.h"

namespace bustub {

enum class WriteStatus : uint32_t { COMMITTED = 1 };

struct WriteResponseV1 {
  uint32_t format_version_{1};
  WriteStatus status_{WriteStatus::COMMITTED};
  uint64_t request_id_{0};
  uint64_t term_{0};
  uint64_t commit_index_{0};

  friend auto operator==(const WriteResponseV1 &lhs, const WriteResponseV1 &rhs) -> bool {
    return lhs.format_version_ == rhs.format_version_ && lhs.status_ == rhs.status_ &&
           lhs.request_id_ == rhs.request_id_ && lhs.term_ == rhs.term_ && lhs.commit_index_ == rhs.commit_index_;
  }
};

class WriteResponseCodec {
 public:
  static auto Encode(const WriteResponseV1 &response) -> std::vector<std::byte>;
  static auto Decode(const std::vector<std::byte> &bytes) -> WriteResponseV1;
};

enum class RequestDisposition { NEW_REQUEST, RETRY_LAST, PAYLOAD_MISMATCH, TOO_OLD, GAP };

struct SessionRecord {
  uint64_t last_request_id_{0};
  RequestFingerprintV1 request_fingerprint_{};
  std::vector<std::byte> encoded_response_;

  friend auto operator==(const SessionRecord &lhs, const SessionRecord &rhs) -> bool {
    return lhs.last_request_id_ == rhs.last_request_id_ && lhs.request_fingerprint_ == rhs.request_fingerprint_ &&
           lhs.encoded_response_ == rhs.encoded_response_;
  }
};

/** Replicated exactly-once response state. Visibility is additionally guarded by the FSM state latch. */
class SessionTable {
 public:
  auto Classify(uint64_t client_id, uint64_t request_id, const RequestFingerprintV1 &request_fingerprint) const
      -> RequestDisposition;
  auto GetLastResponse(uint64_t client_id) const -> std::optional<std::vector<std::byte>>;
  void RecordCommitted(uint64_t client_id, uint64_t request_id, const RequestFingerprintV1 &request_fingerprint,
                       const std::vector<std::byte> &encoded_response);

  /**
   * Fail closed if any cached response describes a commit beyond Snapshot@last_included_index. A mode-specific
   * publisher may additionally require every cached response to carry one exact term; distributed snapshots leave
   * this unset because a snapshot can contain committed responses from several Raft terms.
   */
  void ValidateSnapshotBoundary(uint64_t last_included_index,
                                std::optional<uint64_t> required_response_term = std::nullopt) const;

  auto SnapshotRecords() const -> std::map<uint64_t, SessionRecord>;
  void RestoreRecords(std::map<uint64_t, SessionRecord> records);

 private:
  mutable std::shared_mutex mutex_;
  std::map<uint64_t, SessionRecord> sessions_;
};

class SessionSnapshotCodec {
 public:
  static constexpr uint32_t FORMAT_VERSION = 2;
  static auto Encode(const SessionTable &sessions) -> std::vector<std::byte>;
  static void DecodeInto(const std::vector<std::byte> &bytes, SessionTable *sessions);
};

}  // namespace bustub
