#pragma once
#include "raft/object_storage.h"
#include "raft/snapshot_store.h"
#include "raft/state_machine.h"
namespace bustub {
class ObjectSnapshotStore {
 public:
  explicit ObjectSnapshotStore(std::shared_ptr<RaftObjectStorage> storage);
  auto Latest() const -> std::optional<RaftSnapshot>;
  auto Oldest() const -> std::optional<RaftSnapshot>;
  void RetainOnlyLatest();
  auto Capture(uint64_t index, uint64_t term, RaftStateMachine &machine) -> RaftSnapshot;
  auto Input(const RaftSnapshot &snapshot) -> SnapshotInput;
  auto PlanDelta(const RaftSnapshot &target) -> std::optional<SnapshotDelta>;
  auto BeginDelta(const RaftSnapshot &target, const RaftSnapshot &base, uint64_t session) -> bool;
  auto Stage(const SnapshotChunk &chunk) -> SnapshotStageResult;
  auto Staged(std::string_view id) const -> std::optional<RaftSnapshot>;
  auto StagedInput(std::string_view id) -> std::optional<SnapshotInput>;
  void Cancel(std::string_view id);
  auto PublishStaged(std::string_view id, bool retain) -> RaftSnapshot;

 private:
  struct Body {
    RaftSnapshot info_;
    uint64_t object_;
  };
  struct Part {
    uint64_t offset_, size_, source_;
    bool reuse_;
  };
  struct DeltaReceive {
    uint64_t session_, scratch_, base_object_, written_{0};
    SnapshotInput base_input_;
    std::vector<Part> parts_;
  };
  struct Download {
    Body body_;
    uint64_t received_;
    bool complete_;
    std::optional<DeltaReceive> delta_{std::nullopt};
  };
  auto StageDelta(const SnapshotChunk &chunk) -> SnapshotStageResult;
  void FinishDelta();
  auto Source(const Body &body) -> SnapshotInput;
  auto Checksum(const SnapshotInput &input) const -> uint32_t;
  auto Encode(const std::optional<Body> &latest, const std::optional<Body> &previous) const -> std::vector<std::byte>;
  auto Publish(Body body, bool retain) -> RaftSnapshot;
  std::shared_ptr<RaftObjectStorage> storage_;
  std::optional<Body> latest_, previous_;
  std::optional<Download> download_;
};
}  // namespace bustub
