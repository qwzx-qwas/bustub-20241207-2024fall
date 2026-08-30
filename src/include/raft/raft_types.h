//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// raft_types.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "recovery/log_codec.h"

namespace bustub {

using NodeId = uint64_t;

enum class RaftRole { FOLLOWER, CANDIDATE, LEADER, TERM_PERSISTING, STOPPED };

struct HardState {
  uint32_t format_version_{1};
  uint64_t generation_{0};
  uint64_t current_term_{0};
  std::optional<NodeId> voted_for_;
  uint64_t commit_index_{0};

  friend auto operator==(const HardState &lhs, const HardState &rhs) -> bool {
    return lhs.format_version_ == rhs.format_version_ && lhs.generation_ == rhs.generation_ &&
           lhs.current_term_ == rhs.current_term_ && lhs.voted_for_ == rhs.voted_for_ &&
           lhs.commit_index_ == rhs.commit_index_;
  }
};

struct RequestVoteRequest {
  uint64_t term_{0};
  NodeId candidate_id_{0};
  uint64_t last_log_index_{0};
  uint64_t last_log_term_{0};
};

struct RequestVoteResponse {
  uint64_t term_{0};
  bool vote_granted_{false};
};

struct AppendEntriesRequest {
  uint64_t term_{0};
  NodeId leader_id_{0};
  uint64_t request_id_{0};
  uint64_t prev_log_index_{0};
  uint64_t prev_log_term_{0};
  std::vector<ReplicatedLogEntry> entries_;
  uint64_t leader_commit_{0};
  std::optional<uint64_t> read_context_;
};

struct AppendEntriesResponse {
  uint64_t term_{0};
  uint64_t request_id_{0};
  bool success_{false};
  uint64_t match_index_{0};
  std::optional<uint64_t> conflict_term_;
  uint64_t conflict_index_{0};
  std::optional<uint64_t> read_context_;
};

struct InstallSnapshotRequest {
  uint64_t term_{0};
  NodeId leader_id_{0};
  uint64_t request_id_{0};
  std::string snapshot_id_;
  uint64_t last_included_index_{0};
  uint64_t last_included_term_{0};
  uint64_t offset_{0};
  uint64_t total_size_{0};
  uint32_t payload_checksum_{0};
  bool done_{false};
  std::vector<std::byte> data_;
};

struct InstallSnapshotResponse {
  uint64_t term_{0};
  uint64_t request_id_{0};
  bool success_{false};
  bool stale_{false};
  bool complete_{false};
  uint64_t match_index_{0};
  uint64_t next_offset_{0};
};

using RaftMessage = std::variant<RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse,
                                 InstallSnapshotRequest, InstallSnapshotResponse>;

struct RaftEnvelope {
  NodeId from_{0};
  NodeId to_{0};
  RaftMessage message_;
  std::string group_id_;
};

}  // namespace bustub
