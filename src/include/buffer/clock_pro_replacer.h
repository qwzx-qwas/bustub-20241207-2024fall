#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>
#include "common/config.h"
#include "storage/disk/resource_budget.h"

namespace bustub {
// Replacement history is scoped to ONE opened cache. Keys identify pages (B:
// page/generation/LSN), never a reusable frame or a physical device address.
class ClockProReplacer {
 public:
  using Key = std::array<uint64_t, 3>;
  explicit ClockProReplacer(size_t frames, std::shared_ptr<ResourceBudget> budget);
  void Admit(frame_id_t frame, Key key);
  // The cache owner protects frame reuse while recording hits. These operations
  // do not take the policy mutex or look up a page key.
  void RecordAccess(frame_id_t frame);
  void SetEvictable(frame_id_t frame, bool evictable);
  auto Candidate() -> std::optional<frame_id_t>;
  // Evicted retains bounded history; Remove forgets deleted/failed/private pages.
  void Evicted(frame_id_t frame);
  void Remove(frame_id_t frame);
  void Rekey(frame_id_t frame, Key key);

 private:
  struct Node {
    Key key{};
    int prev{-1}, next{-1};
    frame_id_t frame{INVALID_FRAME_ID};
    bool hot{false}, test{true};
  };
  struct Flags {
    std::atomic<bool> referenced{false}, evictable{false};
  };
  static auto Hash(Key key) -> size_t;
  auto FindGhost(Key key) const -> int;
  void InsertGhost(int node);
  void EraseGhost(Key key);
  void Link(int node);
  void Unlink(int node);
  void Drop(int node);
  void Expire(int node);
  void HotStep();
  void TestStep();
  void RemoveLocked(frame_id_t frame);
  const size_t capacity_;
  ResourceCharge memory_;
  std::vector<Node> nodes_;
  std::unique_ptr<Flags[]> flags_;
  std::vector<int> frame_nodes_, ghosts_, free_;
  std::mutex mutex_;
  int hot_hand_{-1}, cold_hand_{-1}, test_hand_{-1};
  size_t hot_{0}, tests_{0}, cold_target_;
};
}  // namespace bustub
