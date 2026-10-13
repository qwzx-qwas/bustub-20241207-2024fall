#pragma once

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <vector>

#include "storage/disk/resource_budget.h"

namespace bustub {

// Framing only: protocol admission, execution and retries remain with the caller.
// One poll turn moves at most 256 KiB so a large frame cannot monopolize a loop.
class FramedSocket {
 public:
  enum class Result { Waiting, Ready, Closed };
  FramedSocket(int fd, size_t prefix, std::shared_ptr<ResourceAccount> memory)
      : fd_(fd), prefix_(prefix), memory_(std::move(memory)), input_(prefix) {
    fcntl(fd_, F_SETFL, fcntl(fd_, F_GETFL, 0) | O_NONBLOCK);
    int enabled = 1;
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled));
  }
  ~FramedSocket() { close(fd_); }
  FramedSocket(const FramedSocket &) = delete;
  auto operator=(const FramedSocket &) -> FramedSocket & = delete;
  auto Fd() const -> int { return fd_; }
  auto Input() const -> const std::vector<std::byte> & { return input_; }
  auto Partial() const -> bool { return received_ != 0; }
  auto Started() const -> std::chrono::steady_clock::time_point { return started_; }

  template <typename Size>
  auto Receive(Size payload_size, size_t copies) -> Result {
    size_t budget = 256 * 1024;
    while (budget) {
      if (received_ == prefix_ && input_.size() == prefix_) {
        const auto size = prefix_ + payload_size(input_) + sizeof(uint32_t);
        const auto bytes = 4096 + copies * size;
        if (!memory_->Reserve(bytes, false)) return Result::Closed;
        input_charge_ = ResourceCharge(memory_, bytes, false);
        input_.resize(size);
      }
      if (received_ == input_.size()) return Result::Ready;
      auto count = recv(fd_, input_.data() + received_, std::min(budget, input_.size() - received_), 0);
      if (count > 0) {
        if (received_ == 0) started_ = std::chrono::steady_clock::now();
        received_ += count;
        budget -= count;
      } else if (count < 0 && errno == EINTR) {
        continue;
      } else if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return Result::Waiting;
      } else {
        return Result::Closed;
      }
    }
    return received_ == input_.size() && input_.size() > prefix_ ? Result::Ready : Result::Waiting;
  }
  auto Consume() -> ResourceCharge {
    // The handoff retains the decoded request's allowance, not a second body copy.
    std::vector<std::byte>(prefix_).swap(input_);
    received_ = 0;
    return std::move(input_charge_);
  }
  void Reply(std::vector<std::byte> output, ResourceCharge charge) {
    output_ = std::move(output);
    output_charge_ = std::move(charge);
    sent_ = 0;
    output_started_ = std::chrono::steady_clock::now();
  }
  auto OutputStarted() const -> std::chrono::steady_clock::time_point { return output_started_; }
  auto HasOutput() const -> bool { return !output_.empty(); }
  auto Send() -> Result {
    const auto end = std::min(output_.size(), sent_ + 256 * 1024);
    while (sent_ < end) {
      auto count = send(fd_, output_.data() + sent_, end - sent_, MSG_NOSIGNAL);
      if (count > 0) sent_ += count;
      else if (count < 0 && errno == EINTR) continue;
      else if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return Result::Waiting;
      else return Result::Closed;
    }
    if (sent_ != output_.size()) return Result::Waiting;
    std::vector<std::byte>().swap(output_);
    output_charge_ = {};
    return Result::Ready;
  }

 private:
  int fd_;
  size_t prefix_;
  std::shared_ptr<ResourceAccount> memory_;
  std::vector<std::byte> input_, output_;
  size_t received_{0}, sent_{0};
  ResourceCharge input_charge_, output_charge_;
  std::chrono::steady_clock::time_point started_, output_started_;
};
}  // namespace bustub
