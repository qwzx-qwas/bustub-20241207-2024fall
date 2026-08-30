//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// tcp_transport_test.cpp
//
//===----------------------------------------------------------------------===//

#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>              // NOLINT(build/c++11)
#include <condition_variable>  // NOLINT(build/c++11)
#include <exception>
#include <map>
#include <mutex>  // NOLINT(build/c++11)
#include <stdexcept>
#include <thread>  // NOLINT(build/c++11)
#include <vector>

#include "gtest/gtest.h"
#include "raft/tcp_transport.h"

namespace bustub {
namespace {

auto WaitForDrops(const TcpRaftTransport &transport, uint64_t expected) -> bool {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (std::chrono::steady_clock::now() < deadline) {
    if (transport.DroppedMessages() >= expected) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return transport.DroppedMessages() >= expected;
}

class BlockingRaftPeer {
 public:
  BlockingRaftPeer() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
      throw std::runtime_error("cannot create blocking Raft peer socket");
    }
    int receive_buffer_bytes = 4096;
    if (setsockopt(listen_fd_, SOL_SOCKET, SO_RCVBUF, &receive_buffer_bytes, sizeof(receive_buffer_bytes)) != 0) {
      close(listen_fd_);
      listen_fd_ = -1;
      throw std::runtime_error("cannot limit blocking Raft peer receive buffer");
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (bind(listen_fd_, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0 || listen(listen_fd_, 1) != 0) {
      close(listen_fd_);
      listen_fd_ = -1;
      throw std::runtime_error("cannot listen for blocking Raft peer");
    }
    socklen_t address_size = sizeof(address);
    if (getsockname(listen_fd_, reinterpret_cast<sockaddr *>(&address), &address_size) != 0) {
      close(listen_fd_);
      listen_fd_ = -1;
      throw std::runtime_error("cannot inspect blocking Raft peer endpoint");
    }
    endpoint_ = {"127.0.0.1", ntohs(address.sin_port)};
  }

  ~BlockingRaftPeer() {
    Release();
    if (listen_fd_ >= 0) {
      close(listen_fd_);
    }
  }
  BlockingRaftPeer(const BlockingRaftPeer &) = delete;
  auto operator=(const BlockingRaftPeer &) -> BlockingRaftPeer & = delete;

  auto Endpoint() const -> TcpEndpoint { return endpoint_; }

  auto ServeUntilStopped(const std::atomic<bool> &sender_stopped) -> size_t {
    pollfd descriptor{listen_fd_, POLLIN, 0};
    int poll_status;
    do {
      poll_status = poll(&descriptor, 1, 10000);
    } while (poll_status < 0 && errno == EINTR);
    if (poll_status <= 0) {
      throw std::runtime_error("blocking Raft peer timed out waiting for the first frame");
    }
    const auto first_connection = accept(listen_fd_, nullptr, nullptr);
    if (first_connection < 0) {
      throw std::runtime_error("blocking Raft peer could not accept the first frame");
    }
    {
      std::lock_guard lock(mutex_);
      accepted_ = true;
      cv_.notify_all();
    }
    {
      std::unique_lock lock(mutex_);
      cv_.wait(lock, [&] { return released_; });
    }
    shutdown(first_connection, SHUT_RDWR);
    close(first_connection);

    size_t accepted_connections = 1;
    while (!sender_stopped.load(std::memory_order_acquire)) {
      descriptor = {listen_fd_, POLLIN, 0};
      do {
        poll_status = poll(&descriptor, 1, 50);
      } while (poll_status < 0 && errno == EINTR);
      if (poll_status <= 0) {
        continue;
      }
      const auto connection = accept(listen_fd_, nullptr, nullptr);
      if (connection >= 0) {
        accepted_connections++;
        close(connection);
      }
    }
    return accepted_connections;
  }

  auto WaitUntilAccepted() -> bool {
    std::unique_lock lock(mutex_);
    return cv_.wait_for(lock, std::chrono::seconds(10), [&] { return accepted_; });
  }

  void Release() {
    std::lock_guard lock(mutex_);
    released_ = true;
    cv_.notify_all();
  }

 private:
  int listen_fd_{-1};
  TcpEndpoint endpoint_;
  std::mutex mutex_;
  std::condition_variable cv_;
  bool accepted_{false};
  bool released_{false};
};

}  // namespace

TEST(TcpRaftTransportTest, LoopbackFramesReconnectAndValidatePeerIdentity) {
  std::mutex mutex;
  std::condition_variable cv;
  std::vector<RaftEnvelope> received;

  TcpRaftTransport receiver(2, "demo", {"127.0.0.1", 0}, {{1, {"127.0.0.1", 1}}});
  receiver.Start([&](RaftEnvelope envelope) {
    std::lock_guard lock(mutex);
    received.push_back(std::move(envelope));
    cv.notify_all();
  });
  const auto receiver_endpoint = receiver.ListenEndpoint();
  ASSERT_NE(receiver_endpoint.port_, 0);

  TcpRaftTransport sender(1, "demo", {"127.0.0.1", 0}, {{2, receiver_endpoint}});
  sender.Start([](RaftEnvelope) {});
  sender.Send({1, 2, RequestVoteRequest{3, 1, 7, 2}, "demo"});
  sender.Send({1, 2, AppendEntriesResponse{3, 9, true, 7, std::nullopt, 0, 41}, "demo"});

  {
    std::unique_lock lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(3), [&] { return received.size() == 2; }));
  }

  EXPECT_EQ(received[0].from_, 1);
  EXPECT_EQ(received[0].to_, 2);
  EXPECT_EQ(received[0].group_id_, "demo");
  ASSERT_TRUE(std::holds_alternative<RequestVoteRequest>(received[0].message_));
  const auto &vote = std::get<RequestVoteRequest>(received[0].message_);
  EXPECT_EQ(vote.term_, 3);
  EXPECT_EQ(vote.candidate_id_, 1);
  EXPECT_EQ(vote.last_log_index_, 7);
  EXPECT_EQ(vote.last_log_term_, 2);

  EXPECT_EQ(received[1].from_, 1);
  EXPECT_EQ(received[1].to_, 2);
  EXPECT_EQ(received[1].group_id_, "demo");
  ASSERT_TRUE(std::holds_alternative<AppendEntriesResponse>(received[1].message_));
  const auto &append = std::get<AppendEntriesResponse>(received[1].message_);
  EXPECT_EQ(append.term_, 3);
  EXPECT_EQ(append.request_id_, 9);
  EXPECT_TRUE(append.success_);
  EXPECT_EQ(append.match_index_, 7);
  EXPECT_FALSE(append.conflict_term_.has_value());
  EXPECT_EQ(append.conflict_index_, 0);
  EXPECT_EQ(append.read_context_, 41);
  EXPECT_EQ(sender.DroppedMessages(), 0);

  EXPECT_THROW(sender.Send({9, 2, RequestVoteResponse{3, true}, "demo"}), std::runtime_error);
  EXPECT_THROW(sender.Send({1, 1, RequestVoteResponse{3, true}, "demo"}), std::runtime_error);
  EXPECT_THROW(sender.Send({1, 3, RequestVoteResponse{3, true}, "demo"}), std::runtime_error);
  EXPECT_THROW(sender.Send({1, 2, RequestVoteResponse{3, true}, "other-group"}), std::runtime_error);

  sender.Stop();
  receiver.Stop();
  EXPECT_THROW(sender.Send({1, 2, RequestVoteResponse{3, true}, "demo"}), std::runtime_error);
}

TEST(TcpRaftTransportTest, ReceiverDropsWrongGroupUnconfiguredPeerAndWrongDestination) {
  std::atomic<size_t> delivered{0};
  TcpRaftTransport receiver(2, "demo", {"127.0.0.1", 0}, {{1, {"127.0.0.1", 1}}});
  receiver.Start([&](RaftEnvelope) { delivered++; });
  const auto receiver_endpoint = receiver.ListenEndpoint();
  ASSERT_NE(receiver_endpoint.port_, 0);

  {
    TcpRaftTransport wrong_group(1, "other-group", {"127.0.0.1", 0}, {{2, receiver_endpoint}});
    wrong_group.Start([](RaftEnvelope) {});
    wrong_group.Send({1, 2, RequestVoteRequest{3, 1, 7, 2}, "other-group"});
    ASSERT_TRUE(WaitForDrops(receiver, 1));
    wrong_group.Stop();
  }
  {
    TcpRaftTransport unconfigured_peer(3, "demo", {"127.0.0.1", 0}, {{2, receiver_endpoint}});
    unconfigured_peer.Start([](RaftEnvelope) {});
    unconfigured_peer.Send({3, 2, RequestVoteRequest{3, 3, 7, 2}, "demo"});
    ASSERT_TRUE(WaitForDrops(receiver, 2));
    unconfigured_peer.Stop();
  }
  {
    // A valid node-1 frame is deliberately routed to node 2 while its envelope names node 3.
    TcpRaftTransport wrong_destination(1, "demo", {"127.0.0.1", 0}, {{3, receiver_endpoint}});
    wrong_destination.Start([](RaftEnvelope) {});
    wrong_destination.Send({1, 3, RequestVoteRequest{3, 1, 7, 2}, "demo"});
    ASSERT_TRUE(WaitForDrops(receiver, 3));
    wrong_destination.Stop();
  }

  EXPECT_EQ(delivered.load(), 0);
  EXPECT_EQ(receiver.DroppedMessages(), 3);
  receiver.Stop();
}

TEST(TcpRaftTransportTest, EndpointParserRejectsAmbiguity) {
  EXPECT_EQ(TcpEndpoint::Parse("127.0.0.1:7101"), (TcpEndpoint{"127.0.0.1", 7101}));
  EXPECT_THROW(TcpEndpoint::Parse("127.0.0.1"), std::runtime_error);
  EXPECT_THROW(TcpEndpoint::Parse("::1:7101"), std::runtime_error);
  EXPECT_THROW(TcpEndpoint::Parse("host:70000"), std::runtime_error);
}

TEST(TcpRaftTransportTest, StopDiscardsQueuedFramesInsteadOfDrainingStaleBacklog) {
  BlockingRaftPeer peer;
  std::atomic<bool> sender_stopped{false};
  std::exception_ptr server_error;
  size_t accepted_connections = 0;
  std::thread server([&] {
    try {
      accepted_connections = peer.ServeUntilStopped(sender_stopped);
    } catch (...) {
      server_error = std::current_exception();
    }
  });

  TcpRaftTransport sender(1, "shutdown", {"127.0.0.1", 0}, {{2, peer.Endpoint()}}, 5000);
  sender.Start([](RaftEnvelope) {});
  ReplicatedLogEntry large_entry;
  large_entry.index_ = 1;
  large_entry.term_ = 1;
  large_entry.payload_.assign(16U * 1024U * 1024U, std::byte{0x5a});
  AppendEntriesRequest large_request{1, 1, 1, 0, 0, {}, 0, std::nullopt};
  large_request.entries_.push_back(std::move(large_entry));
  EXPECT_NO_THROW(sender.Send({1, 2, std::move(large_request), "shutdown"}));

  if (!peer.WaitUntilAccepted()) {
    peer.Release();
    sender.Stop();
    sender_stopped = true;
    server.join();
    ADD_FAILURE() << "large Raft frame never reached the blocking peer";
    return;
  }

  constexpr uint64_t queued_messages = 16;
  for (uint64_t request_id = 2; request_id < queued_messages + 2; request_id++) {
    EXPECT_NO_THROW(sender.Send({1, 2, AppendEntriesRequest{1, 1, request_id, 0, 0, {}, 0, std::nullopt}, "shutdown"}));
  }

  std::exception_ptr stop_error;
  std::thread stopper([&] {
    try {
      sender.Stop();
    } catch (...) {
      stop_error = std::current_exception();
    }
    sender_stopped.store(true, std::memory_order_release);
  });
  EXPECT_TRUE(WaitForDrops(sender, queued_messages))
      << "Stop did not atomically account for and discard its queued messages";
  peer.Release();
  stopper.join();
  server.join();

  EXPECT_EQ(stop_error, nullptr);
  EXPECT_EQ(server_error, nullptr);
  EXPECT_EQ(accepted_connections, 1);
  EXPECT_GE(sender.DroppedMessages(), queued_messages);
  EXPECT_THROW(sender.Send({1, 2, RequestVoteRequest{2, 1, 1, 1}, "shutdown"}), std::runtime_error);
}

}  // namespace bustub
