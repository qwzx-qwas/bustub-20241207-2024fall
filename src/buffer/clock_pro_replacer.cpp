#include "buffer/clock_pro_replacer.h"
#include <algorithm>
#include <limits>
#include <stdexcept>

namespace bustub {
ClockProReplacer::ClockProReplacer(size_t frames, std::shared_ptr<ResourceBudget> budget)
    : capacity_(frames), cold_target_(std::max<size_t>(1, frames / 2)) {
  if (frames == 0 || frames > static_cast<size_t>(std::numeric_limits<int>::max()) / 8)
    throw std::invalid_argument("invalid CLOCK-Pro capacity");
  size_t hash_size = 8;
  while (hash_size < frames * 4) hash_size *= 2;
  auto account = ResourceAccount::Create(std::move(budget));
  const auto bytes = frames * (2 * sizeof(Node) + sizeof(Flags) + 3 * sizeof(int)) + hash_size * sizeof(int);
  if (!account->Reserve(bytes, false)) throw std::bad_alloc();
  memory_ = ResourceCharge(account, bytes, false);
  nodes_.resize(2 * frames);
  flags_ = std::make_unique<Flags[]>(frames);
  frame_nodes_.resize(frames, -1);
  ghosts_.resize(hash_size, -1);
  free_.reserve(2 * frames);
  for (size_t i = 2 * frames; i != 0; --i) free_.push_back(static_cast<int>(i - 1));
}
auto ClockProReplacer::Hash(Key key) -> size_t {
  uint64_t h = 0;
  for (auto v : key) {
    v += 0x9e3779b97f4a7c15ULL;
    v = (v ^ (v >> 30)) * 0xbf58476d1ce4e5b9ULL;
    v = (v ^ (v >> 27)) * 0x94d049bb133111ebULL;
    h ^= v ^ (v >> 31) ^ (h << 7);
  }
  return h;
}
auto ClockProReplacer::FindGhost(Key key) const -> int {
  size_t p = Hash(key) & (ghosts_.size() - 1);
  while (ghosts_[p] != -1) {
    if (nodes_[ghosts_[p]].key == key) return ghosts_[p];
    p = (p + 1) & (ghosts_.size() - 1);
  }
  return -1;
}
void ClockProReplacer::InsertGhost(int id) {
  size_t p = Hash(nodes_[id].key) & (ghosts_.size() - 1);
  while (ghosts_[p] != -1) p = (p + 1) & (ghosts_.size() - 1);
  ghosts_[p] = id;
}
void ClockProReplacer::EraseGhost(Key key) {
  size_t p = Hash(key) & (ghosts_.size() - 1);
  while (ghosts_[p] != -1 && nodes_[ghosts_[p]].key != key) p = (p + 1) & (ghosts_.size() - 1);
  if (ghosts_[p] == -1) return;
  ghosts_[p] = -1;
  // Reinsert the remainder of this cluster; no tombstones or allocation.
  p = (p + 1) & (ghosts_.size() - 1);
  while (ghosts_[p] != -1) {
    const int id = ghosts_[p];
    ghosts_[p] = -1;
    InsertGhost(id);
    p = (p + 1) & (ghosts_.size() - 1);
  }
}
void ClockProReplacer::Link(int id) {
  auto &n = nodes_[id];
  if (hot_hand_ == -1) {
    n.prev = n.next = id;
    hot_hand_ = cold_hand_ = test_hand_ = id;
  } else {
    n.next = hot_hand_;
    n.prev = nodes_[hot_hand_].prev;
    nodes_[n.prev].next = id;
    nodes_[hot_hand_].prev = id;
  }
}
void ClockProReplacer::Unlink(int id) {
  const auto n = nodes_[id];
  const int next = n.next == id ? -1 : n.next;
  if (hot_hand_ == id) hot_hand_ = next;
  if (cold_hand_ == id) cold_hand_ = next;
  if (test_hand_ == id) test_hand_ = next;
  if (next != -1) {
    nodes_[n.prev].next = n.next;
    nodes_[n.next].prev = n.prev;
  }
}
void ClockProReplacer::Drop(int id) {
  Unlink(id);
  nodes_[id] = Node{};
  free_.push_back(id);
}
void ClockProReplacer::Expire(int id) {
  auto &n = nodes_[id];
  if (!n.test || n.hot) return;
  n.test = false;
  if (cold_target_ > 1) --cold_target_;
  if (n.frame == INVALID_FRAME_ID) {
    EraseGhost(n.key);
    --tests_;
    Drop(id);
  }
}
void ClockProReplacer::TestStep() {
  if (test_hand_ == -1) return;
  const int id = test_hand_;
  test_hand_ = nodes_[id].next;
  Expire(id);
}
void ClockProReplacer::HotStep() {
  if (hot_hand_ == -1) return;
  const int id = hot_hand_;
  hot_hand_ = nodes_[id].next;
  auto &n = nodes_[id];
  if (n.hot) {
    if (!flags_[n.frame].referenced.exchange(false, std::memory_order_relaxed)) {
      n.hot = false;
      n.test = false;
      --hot_;
    }
  } else {
    Expire(id);
  }
}
void ClockProReplacer::Admit(frame_id_t frame, Key key) {
  std::lock_guard lock(mutex_);
  if (frame_nodes_.at(frame) != -1) throw std::logic_error("CLOCK-Pro frame already resident");
  int id = FindGhost(key);
  if (id != -1) {
    EraseGhost(key);
    --tests_;
    Unlink(id);
    nodes_[id].hot = true;
    nodes_[id].test = false;
    ++hot_;
    cold_target_ = std::min(capacity_, cold_target_ + 1);
  } else {
    // At most C resident + C ghost records. A free frame leaves a free node.
    id = free_.back();
    free_.pop_back();
    nodes_[id] = Node{};
    nodes_[id].key = key;
  }
  nodes_[id].frame = frame;
  frame_nodes_[frame] = id;
  flags_[frame].referenced.store(false, std::memory_order_relaxed);
  flags_[frame].evictable.store(false, std::memory_order_relaxed);
  Link(id);
}
void ClockProReplacer::RecordAccess(frame_id_t frame) {
  // A repeat hit on an already referenced frame need not write its cache line.
  if (!flags_[frame].referenced.load(std::memory_order_relaxed))
    flags_[frame].referenced.store(true, std::memory_order_relaxed);
}
void ClockProReplacer::SetEvictable(frame_id_t frame, bool value) {
  flags_[frame].evictable.store(value, std::memory_order_relaxed);
}
auto ClockProReplacer::Candidate() -> std::optional<frame_id_t> {
  std::lock_guard lock(mutex_);
  // Database pins can temporarily make every frame unavailable. Do bounded
  // work, preserve hand progress, and let the owner apply backpressure.
  for (size_t work = 0; work < 8 * capacity_ && cold_hand_ != -1; ++work) {
    // Cold pages may all be pinned. After one ring pass, age hot pages
    // even below the target, so an eligible hot frame remains reachable.
    if (hot_ > capacity_ - cold_target_ || work >= 2 * capacity_) HotStep();
    if (cold_hand_ == -1) break;
    const int id = cold_hand_;
    auto &n = nodes_[id];
    cold_hand_ = n.next;
    if (n.frame == INVALID_FRAME_ID || n.hot) continue;
    if (!flags_[n.frame].evictable.load(std::memory_order_relaxed)) continue;
    if (flags_[n.frame].referenced.exchange(false, std::memory_order_relaxed)) {
      if (n.test) {
        n.hot = true;
        n.test = false;
        ++hot_;
        cold_target_ = std::min(capacity_, cold_target_ + 1);
      }
      Unlink(id);
      Link(id);
    } else {
      return n.frame;
    }
  }
  return std::nullopt;
}
void ClockProReplacer::Evicted(frame_id_t frame) {
  std::lock_guard lock(mutex_);
  int id = frame_nodes_.at(frame);
  if (id == -1) throw std::logic_error("CLOCK-Pro eviction lacks resident frame");
  auto &n = nodes_[id];
  frame_nodes_[frame] = -1;
  flags_[frame].evictable.store(false, std::memory_order_relaxed);
  if (n.hot) {
    --hot_;
    n.hot = false;
  }
  n.frame = INVALID_FRAME_ID;
  if (!n.test) {
    Drop(id);
    return;
  }
  InsertGhost(id);
  ++tests_;
  while (tests_ > capacity_) TestStep();
}
void ClockProReplacer::RemoveLocked(frame_id_t frame) {
  int id = frame_nodes_.at(frame);
  if (id == -1) return;
  if (nodes_[id].hot) --hot_;
  frame_nodes_[frame] = -1;
  flags_[frame].evictable.store(false, std::memory_order_relaxed);
  Drop(id);
}
void ClockProReplacer::Remove(frame_id_t frame) {
  std::lock_guard lock(mutex_);
  RemoveLocked(frame);
}
void ClockProReplacer::Rekey(frame_id_t frame, Key key) {
  std::lock_guard lock(mutex_);
  nodes_[frame_nodes_.at(frame)].key = key;
}
}  // namespace bustub
