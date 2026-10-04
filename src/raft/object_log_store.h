#pragma once
#include <mutex>
#include <vector>
#include "raft/object_storage.h"
#include "recovery/log_codec.h"
namespace bustub {
class ObjectLogStore {
 public:
  ObjectLogStore(std::shared_ptr<RaftObjectStorage> storage, uint64_t commit, bool verified_rebuild = false);
  void Replace(uint64_t from, const std::vector<ReplicatedLogEntry> &entries);
  void Base(uint64_t index, uint64_t term, bool retain);
  void Rebuild(uint64_t index, uint64_t term);
  void Advance(uint64_t index);
  auto Term(uint64_t index) const -> std::optional<uint64_t>;
  auto Entries(uint64_t first, uint64_t last) const -> std::vector<ReplicatedLogEntry>;
  auto Last() const -> uint64_t { return base_ + index_.size(); }
  uint64_t base_{0}, base_term_{0}, commit_{0};

 private:
  struct Segment {
    uint64_t object_, begin_, end_;
  };
  struct Location {
    uint64_t term_, object_, offset_, size_;
  };
  auto ReadEntry(const Location &location, const std::vector<Segment> &segments) const -> ReplicatedLogEntry;
  void Publish(std::vector<Segment> segments, std::vector<Location> index, uint64_t base, uint64_t term);
  auto Controls(const std::vector<Segment> &segments, size_t entries, uint64_t base, uint64_t term)
      -> std::vector<ObjectControlMutation>;
  std::shared_ptr<RaftObjectStorage> storage_;
  uint64_t generation_{0};
  std::vector<Segment> segments_;
  std::vector<Location> index_;
};
}  // namespace bustub
