//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// in_memory_raft_transport.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>  // NOLINT(build/c++11)
#include <stdexcept>
#include <utility>
#include <vector>

#include "raft/transport.h"

namespace bustub {

/** A deterministic, explicitly pumped transport owned by test targets only. */
class InMemoryRaftTransport : public RaftTransport {
 public:
  using Receiver = std::function<void(NodeId, const RaftMessage &)>;

  void Register(NodeId node_id, Receiver receiver) {
    if (node_id == 0 || !receiver) {
      throw std::runtime_error("invalid in-memory Raft receiver");
    }
    std::lock_guard lock(mutex_);
    if (!receivers_.emplace(node_id, std::move(receiver)).second) {
      throw std::runtime_error("Raft receiver is already registered");
    }
  }

  void Unregister(NodeId node_id) {
    std::lock_guard lock(mutex_);
    receivers_.erase(node_id);
  }

  void Send(RaftEnvelope envelope) override {
    if (envelope.from_ == 0 || envelope.to_ == 0) {
      throw std::runtime_error("invalid Raft envelope endpoint");
    }
    std::lock_guard lock(mutex_);
    const auto link = link_enabled_.find({envelope.from_, envelope.to_});
    if (link != link_enabled_.end() && !link->second) {
      return;
    }
    messages_.push_back(std::move(envelope));
  }

  void SetLinkEnabled(NodeId from, NodeId to, bool enabled) {
    std::lock_guard lock(mutex_);
    link_enabled_[{from, to}] = enabled;
  }

  auto DeliverOne() -> bool {
    RaftEnvelope envelope;
    Receiver receiver;
    {
      std::lock_guard lock(mutex_);
      if (messages_.empty()) {
        return false;
      }
      envelope = std::move(messages_.front());
      messages_.pop_front();
      const auto link = link_enabled_.find({envelope.from_, envelope.to_});
      if (link != link_enabled_.end() && !link->second) {
        return true;
      }
      const auto target = receivers_.find(envelope.to_);
      if (target == receivers_.end()) {
        return true;
      }
      receiver = target->second;
    }
    receiver(envelope.from_, envelope.message_);
    return true;
  }

  auto DeliverAll(size_t maximum_messages = 100000) -> size_t {
    size_t delivered = 0;
    while (delivered < maximum_messages && DeliverOne()) {
      delivered++;
    }
    if (Pending() != 0) {
      throw std::runtime_error("in-memory Raft transport exceeded its delivery limit");
    }
    return delivered;
  }

  auto Pending() const -> size_t {
    std::lock_guard lock(mutex_);
    return messages_.size();
  }

  void Clear() {
    std::lock_guard lock(mutex_);
    messages_.clear();
  }

  auto TakeAll() -> std::vector<RaftEnvelope> {
    std::lock_guard lock(mutex_);
    std::vector<RaftEnvelope> result;
    result.reserve(messages_.size());
    while (!messages_.empty()) {
      result.push_back(std::move(messages_.front()));
      messages_.pop_front();
    }
    return result;
  }

 private:
  mutable std::mutex mutex_;
  std::map<NodeId, Receiver> receivers_;
  std::map<std::pair<NodeId, NodeId>, bool> link_enabled_;
  std::deque<RaftEnvelope> messages_;
};

}  // namespace bustub
