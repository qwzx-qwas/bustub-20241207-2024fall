//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// client.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

#include "distributed/client_protocol.h"
#include "raft/tcp_transport.h"

namespace bustub {

class DistributedClient {
 public:
  static auto Send(const TcpEndpoint &endpoint, const ClientRequestV1 &request, uint64_t timeout_ms = 5000)
      -> ClientResponseV1;
};

}  // namespace bustub
