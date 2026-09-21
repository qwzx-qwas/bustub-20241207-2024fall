//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// client_protocol_test.cpp
//
//===----------------------------------------------------------------------===//

#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <future>  // NOLINT(build/c++11)
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "distributed/client.h"
#include "distributed/client_protocol.h"
#include "distributed/session_table.h"
#include "gtest/gtest.h"

namespace bustub {
namespace {

auto Bytes(std::initializer_list<uint8_t> values) -> std::vector<std::byte> {
  std::vector<std::byte> result;
  result.reserve(values.size());
  for (const auto value : values) {
    result.push_back(static_cast<std::byte>(value));
  }
  return result;
}

auto Hex(std::string_view value) -> std::vector<std::byte> {
  if (value.size() % 2 != 0) {
    throw std::runtime_error("hex fixture has an odd number of digits");
  }
  const auto digit = [](char character) -> uint8_t {
    if (character >= '0' && character <= '9') {
      return static_cast<uint8_t>(character - '0');
    }
    if (character >= 'a' && character <= 'f') {
      return static_cast<uint8_t>(character - 'a' + 10);
    }
    throw std::runtime_error("hex fixture contains a non-lowercase-hex digit");
  };
  std::vector<std::byte> result;
  result.reserve(value.size() / 2);
  for (size_t offset = 0; offset < value.size(); offset += 2) {
    result.push_back(static_cast<std::byte>((digit(value[offset]) << 4U) | digit(value[offset + 1])));
  }
  return result;
}

class SocketGuard {
 public:
  explicit SocketGuard(int socket_fd) : socket_fd_(socket_fd) {}
  ~SocketGuard() {
    if (socket_fd_ >= 0) {
      close(socket_fd_);
    }
  }
  SocketGuard(const SocketGuard &) = delete;
  auto operator=(const SocketGuard &) -> SocketGuard & = delete;

 private:
  int socket_fd_;
};

void ReadSocketExact(int socket_fd, std::byte *data, size_t size) {
  size_t offset = 0;
  while (offset < size) {
    const auto count = recv(socket_fd, data + offset, size - offset, 0);
    if (count > 0) {
      offset += static_cast<size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    throw std::runtime_error("fake client server received a truncated request");
  }
}

void WriteSocketExact(int socket_fd, const std::byte *data, size_t size) {
  size_t offset = 0;
  while (offset < size) {
    const auto count = send(socket_fd, data + offset, size - offset, MSG_NOSIGNAL);
    if (count > 0) {
      offset += static_cast<size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    throw std::runtime_error("fake client server could not send its response");
  }
}

class OneShotClientServer {
 public:
  OneShotClientServer() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
      throw std::runtime_error("cannot create fake client server socket");
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (bind(listen_fd_, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
      close(listen_fd_);
      listen_fd_ = -1;
      throw std::runtime_error("cannot bind fake client server socket");
    }
    socklen_t address_size = sizeof(address);
    if (getsockname(listen_fd_, reinterpret_cast<sockaddr *>(&address), &address_size) != 0 ||
        listen(listen_fd_, 1) != 0) {
      close(listen_fd_);
      listen_fd_ = -1;
      throw std::runtime_error("cannot listen on fake client server socket");
    }
    endpoint_ = {"127.0.0.1", ntohs(address.sin_port)};
  }

  ~OneShotClientServer() {
    if (listen_fd_ >= 0) {
      close(listen_fd_);
    }
  }
  OneShotClientServer(const OneShotClientServer &) = delete;
  auto operator=(const OneShotClientServer &) -> OneShotClientServer & = delete;

  auto Endpoint() const -> TcpEndpoint { return endpoint_; }

  auto Serve(const ClientResponseV1 &response) -> ClientRequestV1 {
    pollfd descriptor{listen_fd_, POLLIN, 0};
    int poll_result;
    do {
      poll_result = poll(&descriptor, 1, 5000);
    } while (poll_result < 0 && errno == EINTR);
    if (poll_result <= 0) {
      throw std::runtime_error("fake client server timed out waiting for a request");
    }
    const auto client_fd = accept(listen_fd_, nullptr, nullptr);
    if (client_fd < 0) {
      throw std::runtime_error("fake client server could not accept a request");
    }
    SocketGuard client_guard(client_fd);
    timeval timeout{2, 0};
    if (setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0 ||
        setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0) {
      throw std::runtime_error("fake client server could not configure socket timeouts");
    }

    std::vector<std::byte> prefix(ClientProtocolCodec::FRAME_PREFIX_BYTES);
    ReadSocketExact(client_fd, prefix.data(), prefix.size());
    const auto payload_size = ClientProtocolCodec::PayloadSizeFromPrefix(prefix);
    std::vector<std::byte> frame = prefix;
    frame.resize(prefix.size() + payload_size + sizeof(uint32_t));
    ReadSocketExact(client_fd, frame.data() + prefix.size(), payload_size + sizeof(uint32_t));
    auto request = ClientProtocolCodec::DecodeRequest(frame);

    const auto response_frame = ClientProtocolCodec::EncodeResponse(response);
    WriteSocketExact(client_fd, response_frame.data(), response_frame.size());
    return request;
  }

 private:
  int listen_fd_{-1};
  TcpEndpoint endpoint_;
};

auto StatusResponse(uint64_t request_id) -> ClientResponseV1 {
  return {request_id, ClientResponseStatus::OK, 2, true, NodeId{2}, "127.0.0.1:7202", 7, 11, 11, 11, 8, std::nullopt,
          {}};
}

auto CommittedResponse(uint64_t envelope_request_id, uint64_t payload_request_id) -> ClientResponseV1 {
  return {envelope_request_id,
          ClientResponseStatus::COMMITTED,
          2,
          true,
          NodeId{2},
          "127.0.0.1:7202",
          7,
          11,
          11,
          11,
          8,
          std::nullopt,
          WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, payload_request_id, 7, 11})};
}

}  // namespace

TEST(ClientProtocolTest, DistributedClientRequiresResponseRequestCorrelation) {
  {
    OneShotClientServer server;
    auto served = std::async(std::launch::async, [&server] { return server.Serve(StatusResponse(41)); });
    const auto response = DistributedClient::Send(server.Endpoint(), ClientStatusRequestV1{41}, 2000);
    EXPECT_EQ(response.request_id_, 41);
    EXPECT_EQ(response.status_, ClientResponseStatus::OK);
    EXPECT_EQ(response.node_id_, 2);
    EXPECT_TRUE(response.leader_ready_);
    EXPECT_EQ(response.leader_id_, NodeId{2});
    EXPECT_EQ(response.leader_address_, "127.0.0.1:7202");
    EXPECT_EQ(response.term_, 7);
    EXPECT_EQ(response.commit_index_, 11);
    EXPECT_EQ(response.last_applied_, 11);
    EXPECT_EQ(response.published_applied_index_, 11);
    EXPECT_EQ(response.snapshot_base_index_, 8);
    EXPECT_FALSE(response.read_timestamp_.has_value());
    EXPECT_TRUE(response.payload_.empty());

    const auto observed = served.get();
    ASSERT_TRUE(std::holds_alternative<ClientStatusRequestV1>(observed));
    EXPECT_EQ(std::get<ClientStatusRequestV1>(observed).request_id_, 41);
  }

  {
    OneShotClientServer server;
    auto served = std::async(std::launch::async, [&server] { return server.Serve(StatusResponse(52)); });
    const ClientWriteRequestV1 request{52, 51, "INSERT INTO accounts VALUES (7, 900);"};
    try {
      static_cast<void>(DistributedClient::Send(server.Endpoint(), request, 2000));
      FAIL() << "a response for request 52 must not satisfy request 51";
    } catch (const std::runtime_error &error) {
      EXPECT_STREQ(error.what(), "distributed client response request ID does not match request");
    }

    const auto observed = served.get();
    ASSERT_TRUE(std::holds_alternative<ClientWriteRequestV1>(observed));
    const auto &write = std::get<ClientWriteRequestV1>(observed);
    EXPECT_EQ(write.client_id_, 52);
    EXPECT_EQ(write.request_id_, 51);
    EXPECT_EQ(write.sql_, "INSERT INTO accounts VALUES (7, 900);");
  }

  {
    OneShotClientServer server;
    auto served = std::async(std::launch::async, [&server] { return server.Serve(CommittedResponse(61, 61)); });
    const ClientWriteRequestV1 request{52, 61, "UPDATE accounts SET balance = balance + 7 WHERE id = 7;"};
    const auto response = DistributedClient::Send(server.Endpoint(), request, 2000);
    EXPECT_EQ(response.request_id_, 61);
    EXPECT_EQ(response.status_, ClientResponseStatus::COMMITTED);
    EXPECT_EQ(WriteResponseCodec::Decode(response.payload_).request_id_, 61);
    const auto observed = served.get();
    ASSERT_TRUE(std::holds_alternative<ClientWriteRequestV1>(observed));
    EXPECT_EQ(std::get<ClientWriteRequestV1>(observed).request_id_, 61);
  }

  {
    OneShotClientServer server;
    auto served = std::async(std::launch::async, [&server] { return server.Serve(CommittedResponse(71, 72)); });
    const ClientWriteRequestV1 request{52, 71, "DELETE FROM accounts WHERE id = 7;"};
    try {
      static_cast<void>(DistributedClient::Send(server.Endpoint(), request, 2000));
      FAIL() << "a matching envelope must not hide a committed payload for request 72";
    } catch (const std::runtime_error &error) {
      EXPECT_STREQ(error.what(), "distributed client committed response request ID does not match envelope");
    }
    static_cast<void>(served.get());
  }

  {
    OneShotClientServer server;
    auto served = std::async(std::launch::async, [&server] { return server.Serve(CommittedResponse(81, 81)); });
    try {
      static_cast<void>(DistributedClient::Send(server.Endpoint(), ClientStatusRequestV1{81}, 2000));
      FAIL() << "a committed write response must not satisfy a status request with the same ID";
    } catch (const std::runtime_error &error) {
      EXPECT_STREQ(error.what(), "distributed client received a committed response for a non-write request");
    }
    static_cast<void>(served.get());
  }

  {
    OneShotClientServer server;
    auto served = std::async(std::launch::async, [&server] { return server.Serve(StatusResponse(91)); });
    const ClientWriteRequestV1 request{52, 91, "INSERT INTO accounts VALUES (8, 901);"};
    try {
      static_cast<void>(DistributedClient::Send(server.Endpoint(), request, 2000));
      FAIL() << "an OK status response must not satisfy a write request with the same ID";
    } catch (const std::runtime_error &error) {
      EXPECT_STREQ(error.what(), "distributed client received an OK response for a write request");
    }
    static_cast<void>(served.get());
  }

  {
    OneShotClientServer server;
    auto served = std::async(std::launch::async, [&server] { return server.Serve(StatusResponse(101)); });
    const ClientReadRequestV1 request{101, ClientReadConsistency::STALE, "SELECT balance FROM accounts WHERE id = 8;"};
    try {
      static_cast<void>(DistributedClient::Send(server.Endpoint(), request, 2000));
      FAIL() << "a status response must not satisfy a read request with the same ID";
    } catch (const std::runtime_error &error) {
      EXPECT_STREQ(error.what(), "distributed client read response is missing its read timestamp");
    }
    static_cast<void>(served.get());
  }
}

TEST(ClientProtocolTest, EveryV1RequestResponseAndQueryKindMatchesFixedGoldenBytes) {
  // Fixed protocol fixtures assembled from the V1 field table. None of these expected frames calls a production
  // encoder or checksum helper while being constructed.
  const auto write_golden = Bytes({
      0x42, 0x43, 0x4c, 0x4e, 0x54, 0x30, 0x30, 0x31, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
      0x19, 0x00, 0x00, 0x00, 0x01, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x12,
      0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x00, 0x00, 0x00, 0x01, 0x51, 0xb5, 0x69, 0xc0, 0x48,
  });
  const ClientWriteRequestV1 write{0x0102030405060708ULL, 0x1112131415161718ULL, "Q"};
  EXPECT_EQ(ClientProtocolCodec::EncodeRequest(write), write_golden);
  const auto decoded_write = ClientProtocolCodec::DecodeRequest(write_golden);
  ASSERT_TRUE(std::holds_alternative<ClientWriteRequestV1>(decoded_write));
  EXPECT_EQ(std::get<ClientWriteRequestV1>(decoded_write).client_id_, 0x0102030405060708ULL);
  EXPECT_EQ(std::get<ClientWriteRequestV1>(decoded_write).request_id_, 0x1112131415161718ULL);
  EXPECT_EQ(std::get<ClientWriteRequestV1>(decoded_write).sql_, "Q");

  const auto linearizable_read_golden =
      Hex("42434c4e5430303100000001000000150000000201020304050607080000000100000001513423cdca");
  const ClientReadRequestV1 linearizable_read{0x0102030405060708ULL, ClientReadConsistency::LINEARIZABLE, "Q"};
  EXPECT_EQ(ClientProtocolCodec::EncodeRequest(linearizable_read), linearizable_read_golden);
  const auto decoded_linearizable_read = ClientProtocolCodec::DecodeRequest(linearizable_read_golden);
  ASSERT_TRUE(std::holds_alternative<ClientReadRequestV1>(decoded_linearizable_read));
  EXPECT_EQ(std::get<ClientReadRequestV1>(decoded_linearizable_read).request_id_, 0x0102030405060708ULL);
  EXPECT_EQ(std::get<ClientReadRequestV1>(decoded_linearizable_read).consistency_, ClientReadConsistency::LINEARIZABLE);
  EXPECT_EQ(std::get<ClientReadRequestV1>(decoded_linearizable_read).sql_, "Q");

  const auto stale_read_golden =
      Hex("42434c4e543030310000000100000015000000020102030405060708000000020000000151009f0610");
  const ClientReadRequestV1 stale_read{0x0102030405060708ULL, ClientReadConsistency::STALE, "Q"};
  EXPECT_EQ(ClientProtocolCodec::EncodeRequest(stale_read), stale_read_golden);
  const auto decoded_stale_read = ClientProtocolCodec::DecodeRequest(stale_read_golden);
  ASSERT_TRUE(std::holds_alternative<ClientReadRequestV1>(decoded_stale_read));
  EXPECT_EQ(std::get<ClientReadRequestV1>(decoded_stale_read).request_id_, 0x0102030405060708ULL);
  EXPECT_EQ(std::get<ClientReadRequestV1>(decoded_stale_read).consistency_, ClientReadConsistency::STALE);
  EXPECT_EQ(std::get<ClientReadRequestV1>(decoded_stale_read).sql_, "Q");

  const auto status_golden = Hex("42434c4e54303031000000010000000c00000003111213141516171819304cd9");
  const ClientStatusRequestV1 status{0x1112131415161718ULL};
  EXPECT_EQ(ClientProtocolCodec::EncodeRequest(status), status_golden);
  const auto decoded_status = ClientProtocolCodec::DecodeRequest(status_golden);
  ASSERT_TRUE(std::holds_alternative<ClientStatusRequestV1>(decoded_status));
  EXPECT_EQ(std::get<ClientStatusRequestV1>(decoded_status).request_id_, 0x1112131415161718ULL);

  const auto response_golden = Bytes({
      0x42, 0x43, 0x4c, 0x4e, 0x54, 0x30, 0x30, 0x31, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x65, 0x00, 0x00,
      0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x02, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x07,
      0x6e, 0x33, 0x3a, 0x37, 0x32, 0x30, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x68, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x67, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x66, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x60, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x65, 0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03, 0x5a, 0xc5, 0x04, 0x8e,
  });
  const ClientResponseV1 expected_response{9,
                                           ClientResponseStatus::COMMITTED,
                                           2,
                                           true,
                                           NodeId{3},
                                           "n3:7203",
                                           8,
                                           104,
                                           103,
                                           102,
                                           96,
                                           101,
                                           {std::byte{0x01}, std::byte{0x02}, std::byte{0x03}}};
  EXPECT_EQ(ClientProtocolCodec::EncodeResponse(expected_response), response_golden);
  const auto decoded_response = ClientProtocolCodec::DecodeResponse(response_golden);
  EXPECT_EQ(decoded_response.request_id_, 9);
  EXPECT_EQ(decoded_response.status_, ClientResponseStatus::COMMITTED);
  EXPECT_EQ(decoded_response.node_id_, 2);
  EXPECT_TRUE(decoded_response.leader_ready_);
  EXPECT_EQ(decoded_response.leader_id_, 3);
  EXPECT_EQ(decoded_response.leader_address_, "n3:7203");
  EXPECT_EQ(decoded_response.term_, 8);
  EXPECT_EQ(decoded_response.commit_index_, 104);
  EXPECT_EQ(decoded_response.last_applied_, 103);
  EXPECT_EQ(decoded_response.published_applied_index_, 102);
  EXPECT_EQ(decoded_response.snapshot_base_index_, 96);
  EXPECT_EQ(decoded_response.read_timestamp_, 101);
  EXPECT_EQ(decoded_response.payload_, Bytes({0x01, 0x02, 0x03}));

  const auto result_golden = Bytes({
      0x42, 0x51, 0x52, 0x45, 0x53, 0x30, 0x30, 0x31, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02,
      0x00, 0x00, 0x00, 0x02, 0x69, 0x64, 0x00, 0x00, 0x00, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x00, 0x00,
      0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x31, 0x00, 0x00, 0x00, 0x05, 0x61, 0x6c, 0x69, 0x63, 0x65,
      0x00, 0x00, 0x00, 0x01, 0x32, 0x00, 0x00, 0x00, 0x03, 0x62, 0x6f, 0x62, 0x52, 0xb9, 0xba, 0x3e,
  });
  const ClientQueryResultV1 expected_result{{"id", "name"}, {{"1", "alice"}, {"2", "bob"}}};
  EXPECT_EQ(ClientQueryResultCodec::Encode(expected_result), result_golden);
  const auto decoded_result = ClientQueryResultCodec::Decode(result_golden);
  EXPECT_EQ(decoded_result.columns_, (std::vector<std::string>{"id", "name"}));
  EXPECT_EQ(decoded_result.rows_, (std::vector<std::vector<std::string>>{{"1", "alice"}, {"2", "bob"}}));
}

TEST(ClientProtocolTest, RejectsCorruptionDirectionAndInvalidFields) {
  auto request = ClientProtocolCodec::EncodeRequest(ClientStatusRequestV1{1});
  auto corrupt = request;
  corrupt.back() ^= std::byte{1};
  EXPECT_THROW(ClientProtocolCodec::DecodeRequest(corrupt), std::runtime_error);
  EXPECT_THROW(ClientProtocolCodec::DecodeResponse(request), std::runtime_error);
  EXPECT_THROW(ClientProtocolCodec::EncodeRequest(ClientWriteRequestV1{0, 1, "SELECT 1;"}), std::runtime_error);
  EXPECT_THROW(
      ClientProtocolCodec::EncodeRequest(ClientReadRequestV1{1, static_cast<ClientReadConsistency>(99), "SELECT 1;"}),
      std::runtime_error);
  EXPECT_THROW(ClientProtocolCodec::EncodeResponse(
                   {1, ClientResponseStatus::OK, 1, false, std::nullopt, "", 1, 1, 2, 2, 0, std::nullopt, {}}),
               std::runtime_error);
  EXPECT_THROW(ClientProtocolCodec::EncodeResponse(
                   {1, ClientResponseStatus::OK, 1, false, std::nullopt, "", 1, 1, 1, 1, 2, std::nullopt, {}}),
               std::runtime_error);
  EXPECT_THROW(ClientQueryResultCodec::Encode({{"a", "b"}, {{"only-one"}}}), std::runtime_error);
}

}  // namespace bustub
