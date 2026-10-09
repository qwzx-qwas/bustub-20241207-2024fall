#pragma once
#include <mutex>
#include <vector>
#include "raft/object_storage.h"
#include "recovery/log_codec.h"
namespace bustub {
class ObjectLogStore {
 public:
  ObjectLogStore(std::shared_ptr<RaftObjectStorage> storage, uint64_t commit, std::shared_ptr<std::mutex> mutex,
                 bool verified_rebuild = false);
  ~ObjectLogStore();
  // One bounded F29 step, scheduled by the existing Store maintenance owner.
  auto Maintain() -> bool;
  void Replace(uint64_t from, const std::vector<ReplicatedLogEntry> &entries);
  void Base(uint64_t index, uint64_t term, bool retain);
  void Rebuild(uint64_t index, uint64_t term);
  void Advance(uint64_t index);
  auto Term(uint64_t index) const -> std::optional<uint64_t>;
  auto Entries(uint64_t first, uint64_t last, size_t maximum_entries = SIZE_MAX, size_t maximum_bytes = SIZE_MAX) const
      -> std::vector<ReplicatedLogEntry>;
  auto Last() const -> uint64_t { return base_ + index_.size(); }
  uint64_t base_{0}, base_term_{0}, commit_{0};

 private:
  struct Segment {
    uint64_t object_, begin_, end_;
  };
  struct Location {
    uint64_t term_, object_, offset_, size_;
  };
  struct Cleaning;
  auto PlanCleaning() -> std::unique_ptr<Cleaning>;
  auto ReadEntry(const Location &location, const std::vector<Segment> &segments) const -> ReplicatedLogEntry;
  void Publish(std::vector<Segment> segments, std::vector<Location> index, uint64_t base, uint64_t term);
  auto Controls(const std::vector<Segment> &segments, size_t entries, uint64_t base, uint64_t term)
      -> std::vector<ObjectControlMutation>;
  std::shared_ptr<RaftObjectStorage> storage_;
  uint64_t generation_{0};
  std::vector<Segment> segments_;
  std::vector<Location> index_;
  // Foreground is serialized by LogStore using this same mutex.
  std::shared_ptr<std::mutex> mutex_;
  std::unique_ptr<Cleaning> cleaning_;
  size_t clean_cursor_{0};
};
}  // namespace bustub
