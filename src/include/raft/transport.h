//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transport.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include "raft/raft_types.h"

namespace bustub {

class RaftTransport {
 public:
  virtual ~RaftTransport() = default;
  virtual void Send(RaftEnvelope envelope) = 0;
};

}  // namespace bustub
