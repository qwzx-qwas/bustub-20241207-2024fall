#pragma once

#include <chrono>
#include <thread>
#include "raft/raft_node.h"

namespace bustub {
// Existing deterministic schedules advance only after local work completes.
// Real asynchronous progress is checked separately through the unwrapped node.
class SynchronousTestNode : public RaftNode {
 public:
  using RaftNode::RaftNode;
  void AwaitIdle() {
    const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (Busy()) {
      Poll();
      if (std::chrono::steady_clock::now() >= end) throw std::runtime_error("protocol work did not complete");
      if (Busy()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  void Tick(uint64_t now) {
    RaftNode::Tick(now);
    AwaitIdle();
  }
  void Receive(NodeId from, const RaftMessage &message) {
    RaftNode::Receive(from, message);
    AwaitIdle();
  }
  auto Propose(EntryType type, std::vector<std::byte> payload) -> std::optional<uint64_t> {
    const auto result = RaftNode::Propose(type, std::move(payload));
    AwaitIdle();
    if (result)
      if (auto error = TakeProposalError(*result)) std::rethrow_exception(error);
    return result;
  }
  auto StartReadIndex(uint64_t context) -> bool {
    const auto result = RaftNode::StartReadIndex(context);
    AwaitIdle();
    return result;
  }
  auto CreateSnapshot() -> RaftSnapshot {
    if (!RaftNode::CreateSnapshot()) throw std::runtime_error("snapshot admission unexpectedly full");
    AwaitIdle();
    return *LatestSnapshot();
  }
};
}  // namespace bustub
