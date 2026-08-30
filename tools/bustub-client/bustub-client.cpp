//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// bustub-client.cpp
//
//===----------------------------------------------------------------------===//

#include <charconv>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>

#include "distributed/client.h"
#include "distributed/session_table.h"

namespace {

auto Usage() -> std::string {
  return R"(usage:
  bustub-client status --endpoint HOST:PORT --request-id ID
  bustub-client write  --endpoint HOST:PORT --client-id ID --request-id ID --sql SQL
  bustub-client read   --endpoint HOST:PORT --request-id ID [--consistency linearizable|stale] --sql SQL
  optional for all commands: --timeout-ms N)";
}

auto ParseUnsigned(const std::string &value, const std::string &name) -> uint64_t {
  uint64_t result = 0;
  const auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
  if (value.empty() || parsed.ec != std::errc() || parsed.ptr != value.data() + value.size()) {
    throw std::runtime_error(name + " must be an unsigned integer");
  }
  return result;
}

auto Required(const std::map<std::string, std::string> &values, const std::string &name) -> std::string {
  const auto iterator = values.find(name);
  if (iterator == values.end()) {
    throw std::runtime_error("missing required option " + name);
  }
  return iterator->second;
}

auto StatusName(bustub::ClientResponseStatus status) -> const char * {
  switch (status) {
    case bustub::ClientResponseStatus::COMMITTED:
      return "COMMITTED";
    case bustub::ClientResponseStatus::OK:
      return "OK";
    case bustub::ClientResponseStatus::NOT_LEADER:
      return "NOT_LEADER";
    case bustub::ClientResponseStatus::REJECTED:
      return "REJECTED";
    case bustub::ClientResponseStatus::TIMEOUT:
      return "TIMEOUT";
    case bustub::ClientResponseStatus::UNAVAILABLE:
      return "UNAVAILABLE";
  }
  return "UNKNOWN";
}

auto TextPayload(const std::vector<std::byte> &bytes) -> std::string {
  return {reinterpret_cast<const char *>(bytes.data()), bytes.size()};
}

auto HexPayload(const std::vector<std::byte> &bytes) -> std::string {
  std::ostringstream output;
  output << std::hex << std::setfill('0');
  for (const auto byte : bytes) {
    output << std::setw(2) << static_cast<unsigned int>(byte);
  }
  return output.str();
}

}  // namespace

// NOLINTNEXTLINE
auto main(int argc, char **argv) -> int {
  try {
    if (argc < 2 || std::string(argv[1]) == "--help") {
      std::cout << Usage() << '\n';
      return argc < 2 ? 1 : 0;
    }
    const std::string action = argv[1];
    std::map<std::string, std::string> values;
    for (int index = 2; index < argc; index++) {
      const std::string option = argv[index];
      if (option.rfind("--", 0) != 0 || index + 1 >= argc || !values.emplace(option, argv[++index]).second) {
        throw std::runtime_error("invalid or duplicate client option\n" + Usage());
      }
    }
    const auto endpoint = bustub::TcpEndpoint::Parse(Required(values, "--endpoint"));
    const auto request_id = ParseUnsigned(Required(values, "--request-id"), "request ID");
    const auto timeout = values.count("--timeout-ms") == 0 ? 5000 : ParseUnsigned(values.at("--timeout-ms"), "timeout");

    bustub::ClientRequestV1 request;
    if (action == "status") {
      request = bustub::ClientStatusRequestV1{request_id};
    } else if (action == "write") {
      request = bustub::ClientWriteRequestV1{ParseUnsigned(Required(values, "--client-id"), "client ID"), request_id,
                                             Required(values, "--sql")};
    } else if (action == "read") {
      const auto consistency = values.count("--consistency") == 0 ? "linearizable" : values.at("--consistency");
      if (consistency != "linearizable" && consistency != "stale") {
        throw std::runtime_error("--consistency must be linearizable or stale");
      }
      request = bustub::ClientReadRequestV1{request_id,
                                            consistency == "linearizable" ? bustub::ClientReadConsistency::LINEARIZABLE
                                                                          : bustub::ClientReadConsistency::STALE,
                                            Required(values, "--sql")};
    } else {
      throw std::runtime_error("unknown client action " + action + "\n" + Usage());
    }

    const auto response = bustub::DistributedClient::Send(endpoint, request, timeout);
    std::cout << "status=" << StatusName(response.status_) << " node_id=" << response.node_id_
              << " leader_ready=" << (response.leader_ready_ ? 1 : 0) << " term=" << response.term_
              << " commit_index=" << response.commit_index_ << " last_applied=" << response.last_applied_
              << " published_applied_index=" << response.published_applied_index_
              << " snapshot_base_index=" << response.snapshot_base_index_;
    if (response.leader_id_.has_value()) {
      std::cout << " leader_id=" << *response.leader_id_ << " leader_address=" << response.leader_address_;
    }
    if (response.read_timestamp_.has_value()) {
      std::cout << " read_timestamp=" << *response.read_timestamp_;
    }
    std::cout << '\n';

    if (response.status_ == bustub::ClientResponseStatus::COMMITTED) {
      const auto committed = bustub::WriteResponseCodec::Decode(response.payload_);
      std::cout << "request_id=" << committed.request_id_ << " entry_term=" << committed.term_
                << " committed_index=" << committed.commit_index_ << " response_bytes=" << HexPayload(response.payload_)
                << '\n';
    } else if (response.status_ == bustub::ClientResponseStatus::OK && response.read_timestamp_.has_value()) {
      const auto result = bustub::ClientQueryResultCodec::Decode(response.payload_);
      for (size_t column = 0; column < result.columns_.size(); column++) {
        std::cout << (column == 0 ? "" : "\t") << result.columns_[column];
      }
      std::cout << '\n';
      for (const auto &row : result.rows_) {
        for (size_t column = 0; column < row.size(); column++) {
          std::cout << (column == 0 ? "" : "\t") << row[column];
        }
        std::cout << '\n';
      }
    } else if (!response.payload_.empty()) {
      std::cout << "message=" << TextPayload(response.payload_) << '\n';
    }
    return response.status_ == bustub::ClientResponseStatus::COMMITTED ||
                   response.status_ == bustub::ClientResponseStatus::OK
               ? 0
               : 2;
  } catch (const std::exception &error) {
    std::cerr << "bustub-client: " << error.what() << '\n';
    return 1;
  }
}
