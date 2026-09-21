// Test-only persistent bridge. Business content lives in Python; network framing
// and validation remain owned by the production client. No SQL engine shortcuts.

#include <charconv>
#include <chrono>              // NOLINT(build/c++11)
#include <condition_variable>  // NOLINT(build/c++11)
#include <cstddef>
#include <cstdint>
#include <deque>
#include <iostream>
#include <mutex>  // NOLINT(build/c++11)
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>  // NOLINT(build/c++11)
#include <utility>
#include <vector>

#include "distributed/client.h"
#include "distributed/session_table.h"

namespace {

auto Now() -> uint64_t {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

auto Number(const std::string &text) -> uint64_t {
  uint64_t result = 0;
  const auto parsed = std::from_chars(text.data(), text.data() + text.size(), result);
  if (text.empty() || parsed.ec != std::errc() || parsed.ptr != text.data() + text.size()) {
    throw std::runtime_error("invalid bridge integer");
  }
  return result;
}

auto Json(const std::string &text) -> std::string {
  static constexpr char DIGITS[] = "0123456789abcdef";
  std::string result = "\"";
  for (const auto character : text) {
    const auto value = static_cast<unsigned char>(character);
    if (value == '"' || value == '\\') {
      result += '\\';
      result += character;
    } else if (value < 32) {
      result += "\\u00";
      result += DIGITS[value >> 4U];
      result += DIGITS[value & 15U];
    } else {
      result += character;
    }
  }
  return result + '"';
}

auto Unhex(const std::string &text) -> std::string {
  if (text == "-") {
    return "";
  }
  if (text.size() % 2 != 0 || text.size() / 2 > bustub::ClientProtocolCodec::MAX_SQL_BYTES) {
    throw std::runtime_error("invalid bridge SQL length");
  }
  const auto digit = [](char value) -> unsigned char {
    if (value >= '0' && value <= '9') {
      return value - '0';
    }
    if (value >= 'a' && value <= 'f') {
      return value - 'a' + 10;
    }
    throw std::runtime_error("bridge SQL is not lowercase hex");
  };
  std::string result;
  result.reserve(text.size() / 2);
  for (size_t index = 0; index < text.size(); index += 2) {
    result += static_cast<char>((digit(text[index]) << 4U) | digit(text[index + 1]));
  }
  return result;
}

auto Hex(const std::vector<std::byte> &bytes) -> std::string {
  static constexpr char DIGITS[] = "0123456789abcdef";
  std::string result;
  result.reserve(bytes.size() * 2);
  for (auto byte : bytes) {
    const auto value = static_cast<unsigned char>(byte);
    result += DIGITS[value >> 4U];
    result += DIGITS[value & 15U];
  }
  return result;
}

auto Status(bustub::ClientResponseStatus status) -> const char * {
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
  throw std::runtime_error("unrecognized production response status");
}

struct Request {
  uint64_t correlation;
  std::string kind;
  uint64_t client;
  uint64_t request;
  uint64_t timeout;
  uint64_t cutoff;
  std::string endpoint;
  std::string sql;
};

auto Parse(const std::string &line) -> Request {
  std::istringstream input(line);
  std::vector<std::string> fields;
  std::string field;
  while (std::getline(input, field, '\t')) {
    fields.push_back(std::move(field));
  }
  if (fields.size() != 9 || fields[0] != "T0A1") {
    throw std::runtime_error("bridge requires nine T0A1 fields");
  }
  Request result{Number(fields[1]), fields[2],         Number(fields[3]), Number(fields[4]),
                 Number(fields[5]), Number(fields[6]), fields[7],         Unhex(fields[8])};
  if (result.correlation == 0) {
    throw std::runtime_error("bridge correlation must be nonzero");
  }
  if (result.kind == "CLOCK") {
    return result;
  }
  if ((result.kind != "READ" && result.kind != "STALE" && result.kind != "WRITE" && result.kind != "STATUS") ||
      result.request == 0 || result.timeout == 0 || result.timeout > 60000 ||
      (result.kind == "WRITE" && result.client == 0) || (result.kind != "STATUS" && result.sql.empty())) {
    throw std::runtime_error("invalid bridge request");
  }
  static_cast<void>(bustub::TcpEndpoint::Parse(result.endpoint));
  return result;
}

class Driver {
 public:
  explicit Driver(size_t workers) {
    try {
      for (size_t index = 0; index < workers; index++) {
        workers_.emplace_back([this] { Work(); });
      }
    } catch (...) {
      {
        std::lock_guard<std::mutex> guard(queue_mutex_);
        stopping_ = true;
      }
      available_.notify_all();
      for (auto &worker : workers_) {
        worker.join();
      }
      throw;
    }
  }

  ~Driver() {
    {
      std::lock_guard<std::mutex> guard(queue_mutex_);
      stopping_ = true;
    }
    available_.notify_all();
    for (auto &worker : workers_) {
      worker.join();
    }
  }

  void Submit(Request request) {
    if (request.kind == "CLOCK") {
      Emit(Base(request, "clock") + ",\"clock_ns\":" + std::to_string(Now()) + "}");
      return;
    }
    std::unique_lock<std::mutex> lock(queue_mutex_);
    space_.wait(lock, [this] { return queue_.size() < 8; });
    queue_.push_back(std::move(request));
    available_.notify_one();
  }

 private:
  static auto Base(const Request &request, const std::string &event) -> std::string {
    return "{\"format_version\":1,\"correlation\":" + std::to_string(request.correlation) +
           ",\"event\":" + Json(event) + ",\"kind\":" + Json(request.kind) +
           ",\"client_id\":" + std::to_string(request.client) + ",\"request_id\":" + std::to_string(request.request) +
           ",\"endpoint\":" + Json(request.endpoint);
  }

  void Emit(const std::string &line) {
    std::lock_guard<std::mutex> guard(output_mutex_);
    std::cout << line << std::endl;
  }

  void Execute(const Request &request) {
    auto start = Now();
    if (request.cutoff != 0 && start >= request.cutoff) {
      Emit(Base(request, "skipped") + ",\"clock_ns\":" + std::to_string(start) + "}");
      return;
    }
    // Trace publication is part of the measured client-side overhead. No fsync is
    // introduced on the server path; a missing return remains an unknown attempt.
    Emit(Base(request, "call") + ",\"call_ns\":" + std::to_string(start) + "}");
    uint64_t returned = 0;
    try {
      bustub::ClientRequestV1 message = bustub::ClientStatusRequestV1{request.request};
      if (request.kind == "WRITE") {
        message = bustub::ClientWriteRequestV1{request.client, request.request, request.sql};
      } else if (request.kind == "READ" || request.kind == "STALE") {
        message = bustub::ClientReadRequestV1{request.request,
                                              request.kind == "STALE" ? bustub::ClientReadConsistency::STALE
                                                                      : bustub::ClientReadConsistency::LINEARIZABLE,
                                              request.sql};
      }
      const auto response =
          bustub::DistributedClient::Send(bustub::TcpEndpoint::Parse(request.endpoint), message, request.timeout);
      returned = Now();
      std::ostringstream out;
      out << Base(request, "return") << ",\"call_ns\":" << start << ",\"return_ns\":" << returned
          << ",\"status\":" << Json(Status(response.status_)) << ",\"node_id\":" << response.node_id_
          << ",\"leader_ready\":" << (response.leader_ready_ ? "true" : "false") << ",\"leader_id\":";
      if (response.leader_id_.has_value()) {
        out << *response.leader_id_;
      } else {
        out << "null";
      }
      out << ",\"leader_address\":" << Json(response.leader_address_) << ",\"term\":" << response.term_
          << ",\"commit_index\":" << response.commit_index_ << ",\"last_applied\":" << response.last_applied_
          << ",\"published_applied_index\":" << response.published_applied_index_
          << ",\"snapshot_base_index\":" << response.snapshot_base_index_
          << ",\"payload_hex\":" << Json(Hex(response.payload_));
      if (response.status_ == bustub::ClientResponseStatus::COMMITTED) {
        const auto committed = bustub::WriteResponseCodec::Decode(response.payload_);
        out << ",\"committed_request_id\":" << committed.request_id_
            << ",\"committed_index\":" << committed.commit_index_ << ",\"committed_term\":" << committed.term_;
      } else if (response.status_ == bustub::ClientResponseStatus::OK &&
                 (request.kind == "READ" || request.kind == "STALE")) {
        const auto result = bustub::ClientQueryResultCodec::Decode(response.payload_);
        out << ",\"read_timestamp\":" << *response.read_timestamp_ << ",\"columns\":[";
        for (size_t index = 0; index < result.columns_.size(); index++) {
          out << (index == 0 ? "" : ",") << Json(result.columns_[index]);
        }
        out << "],\"rows\":[";
        for (size_t row = 0; row < result.rows_.size(); row++) {
          out << (row == 0 ? "[" : ",[");
          for (size_t column = 0; column < result.rows_[row].size(); column++) {
            out << (column == 0 ? "" : ",") << Json(result.rows_[row][column]);
          }
          out << "]";
        }
        out << "]";
      }
      Emit(out.str() + "}");
    } catch (const std::exception &error) {
      if (returned == 0) {
        returned = Now();
      }
      Emit(Base(request, "error") + ",\"call_ns\":" + std::to_string(start) +
           ",\"return_ns\":" + std::to_string(returned) + ",\"error\":" + Json(error.what()) + "}");
    }
  }

  void Work() {
    while (true) {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      available_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
      if (queue_.empty()) {
        return;
      }
      auto request = std::move(queue_.front());
      queue_.pop_front();
      space_.notify_one();
      lock.unlock();
      Execute(request);
    }
  }

  std::mutex queue_mutex_;
  std::mutex output_mutex_;
  std::condition_variable available_;
  std::condition_variable space_;
  std::deque<Request> queue_;
  std::vector<std::thread> workers_;
  bool stopping_{false};
};

}  // namespace

// NOLINTNEXTLINE
auto main(int argc, char **argv) -> int {
  try {
    if (argc != 2) {
      throw std::runtime_error("usage: storage-e2e-driver WORKERS (1..8); T0A1 commands on stdin");
    }
    const auto workers = Number(argv[1]);
    if (workers == 0 || workers > 8) {
      throw std::runtime_error("driver requires 1..8 workers");
    }
    std::cout.exceptions(std::ios::badbit | std::ios::failbit);
    Driver driver(workers);
    std::string line;
    while (std::getline(std::cin, line)) {
      driver.Submit(Parse(line));
    }
    if (!std::cin.eof()) {
      throw std::runtime_error("bridge input failed");
    }
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "storage-e2e-driver: " << error.what() << '\n';
    return 1;
  }
}
