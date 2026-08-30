//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// bustub-node.cpp
//
//===----------------------------------------------------------------------===//

#include <csignal>

#include <charconv>
#include <chrono>  // NOLINT(build/c++11)
#include <cstdint>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>  // NOLINT(build/c++11)
#include <vector>

#include "distributed/node.h"

namespace {

volatile std::sig_atomic_t stop_requested = 0;

void HandleSignal(int signal) { stop_requested = signal; }

auto ParseUnsigned(const std::string &value, const std::string &name) -> uint64_t {
  uint64_t result = 0;
  const auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
  if (value.empty() || parsed.ec != std::errc() || parsed.ptr != value.data() + value.size()) {
    throw std::runtime_error(name + " must be an unsigned integer");
  }
  return result;
}

auto Usage() -> std::string {
  return R"(usage: bustub-node --node-id ID --group-id GROUP --data-dir DIR
                   --raft-listen HOST:PORT --client-listen HOST:PORT
                  --peer ID=RAFT_HOST:PORT,CLIENT_HOST:PORT --peer ID=RAFT_HOST:PORT,CLIENT_HOST:PORT
                  [--election-timeout-min-ms N] [--election-timeout-max-ms N]
                  [--heartbeat-interval-ms N]
                  [--tick-interval-ms N] [--client-timeout-ms N] [--buffer-pool-size N]
                  [--snapshot-threshold-entries N])";
}

auto Required(const std::map<std::string, std::string> &values, const std::string &name) -> std::string {
  const auto iterator = values.find(name);
  if (iterator == values.end()) {
    throw std::runtime_error("missing required option " + name);
  }
  return iterator->second;
}

auto ParsePeer(const std::string &value) -> std::pair<bustub::NodeId, bustub::DistributedPeerConfig> {
  const auto equals = value.find('=');
  const auto comma = value.find(',', equals == std::string::npos ? 0 : equals + 1);
  if (equals == std::string::npos || equals == 0 || comma == std::string::npos || comma + 1 == value.size() ||
      value.find('=', equals + 1) != std::string::npos || value.find(',', comma + 1) != std::string::npos) {
    throw std::runtime_error("--peer must use ID=RAFT_HOST:PORT,CLIENT_HOST:PORT");
  }
  const auto id = ParseUnsigned(value.substr(0, equals), "peer ID");
  return {id,
          {bustub::TcpEndpoint::Parse(value.substr(equals + 1, comma - equals - 1)),
           bustub::TcpEndpoint::Parse(value.substr(comma + 1))}};
}

}  // namespace

// NOLINTNEXTLINE
auto main(int argc, char **argv) -> int {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);
  try {
    std::map<std::string, std::string> values;
    std::vector<std::string> peers;
    for (int index = 1; index < argc; index++) {
      const std::string option = argv[index];
      if (option == "--help") {
        std::cout << Usage() << '\n';
        return 0;
      }
      if (option.empty() || option.rfind("--", 0) != 0 || index + 1 >= argc) {
        throw std::runtime_error("invalid command line\n" + Usage());
      }
      const std::string argument = argv[++index];
      if (option == "--peer") {
        peers.push_back(argument);
      } else if (!values.emplace(option, argument).second) {
        throw std::runtime_error("duplicate option " + option);
      }
    }

    bustub::DistributedNodeConfig config;
    config.node_id_ = ParseUnsigned(Required(values, "--node-id"), "node ID");
    config.group_id_ = Required(values, "--group-id");
    config.data_directory_ = Required(values, "--data-dir");
    config.raft_listen_ = bustub::TcpEndpoint::Parse(Required(values, "--raft-listen"));
    config.client_listen_ = bustub::TcpEndpoint::Parse(Required(values, "--client-listen"));
    for (const auto &peer_value : peers) {
      const auto peer = ParsePeer(peer_value);
      if (!config.peers_.emplace(peer).second) {
        throw std::runtime_error("duplicate peer ID");
      }
    }
    if (values.count("--election-timeout-min-ms") != 0) {
      config.election_timeout_min_ms_ =
          ParseUnsigned(values.at("--election-timeout-min-ms"), "minimum election timeout");
    }
    if (values.count("--election-timeout-max-ms") != 0) {
      config.election_timeout_max_ms_ =
          ParseUnsigned(values.at("--election-timeout-max-ms"), "maximum election timeout");
    }
    if (values.count("--heartbeat-interval-ms") != 0) {
      config.heartbeat_interval_ms_ = ParseUnsigned(values.at("--heartbeat-interval-ms"), "heartbeat interval");
    }
    if (values.count("--tick-interval-ms") != 0) {
      config.tick_interval_ms_ = ParseUnsigned(values.at("--tick-interval-ms"), "tick interval");
    }
    if (values.count("--client-timeout-ms") != 0) {
      config.client_timeout_ms_ = ParseUnsigned(values.at("--client-timeout-ms"), "client timeout");
    }
    if (values.count("--buffer-pool-size") != 0) {
      config.buffer_pool_size_ = ParseUnsigned(values.at("--buffer-pool-size"), "buffer pool size");
    }
    if (values.count("--snapshot-threshold-entries") != 0) {
      config.snapshot_threshold_entries_ =
          ParseUnsigned(values.at("--snapshot-threshold-entries"), "snapshot threshold entries");
    }
    const std::set<std::string> known{"--node-id",
                                      "--group-id",
                                      "--data-dir",
                                      "--raft-listen",
                                      "--client-listen",
                                      "--election-timeout-min-ms",
                                      "--election-timeout-max-ms",
                                      "--heartbeat-interval-ms",
                                      "--tick-interval-ms",
                                      "--client-timeout-ms",
                                      "--buffer-pool-size",
                                      "--snapshot-threshold-entries"};
    for (const auto &[option, argument] : values) {
      static_cast<void>(argument);
      if (known.count(option) == 0) {
        throw std::runtime_error("unknown option " + option);
      }
    }
    if (config.raft_listen_.port_ == 0 || config.client_listen_.port_ == 0) {
      throw std::runtime_error("production listen ports must be non-zero");
    }

    auto node = bustub::DistributedNode::Open(std::move(config));
    node->Start();
    std::cout << "bustub-node started raft=" << node->RaftEndpoint().ToString()
              << " client=" << node->ClientEndpoint().ToString() << std::endl;
    while (stop_requested == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    node->Stop();
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "bustub-node: " << error.what() << '\n';
    return 1;
  }
}
