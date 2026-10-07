#include "object_snapshot_store.h"
#include <algorithm>
#include <stdexcept>
#include "common/byte_codec.h"
namespace bustub {
ObjectSnapshotStore::ObjectSnapshotStore(std::shared_ptr<RaftObjectStorage> storage) : storage_(std::move(storage)) {
  const auto root = storage_->Control(3, 0);
  if (!root) {
    return;
  }
  ByteReader r(*root);
  if (r.ReadU64() != 1) {
    throw std::runtime_error("unknown snapshot manifest");
  }
  for (auto *slot : {&latest_, &previous_}) {
    const auto present = r.ReadU8();
    if (present > 1) {
      throw std::runtime_error("invalid snapshot manifest presence");
    }
    if (present == 0) {
      continue;
    }
    Body body{};
    body.object_ = r.ReadU64();
    body.info_.generation_ = r.ReadU64();
    body.info_.snapshot_id_ = r.ReadString();
    body.info_.last_included_index_ = r.ReadU64();
    body.info_.last_included_term_ = r.ReadU64();
    body.info_.payload_size_ = r.ReadU64();
    body.info_.payload_checksum_ = r.ReadU32();
    if (body.info_.generation_ == 0 || body.info_.snapshot_id_.empty() ||
        body.info_.payload_size_ > storage_->options_.max_snapshot_bytes_ ||
        Checksum(Source(body)) != body.info_.payload_checksum_) {
      throw std::runtime_error("invalid published snapshot");
    }
    *slot = std::move(body);
  }
  if (!r.Empty() ||
      (previous_ && (!latest_ || previous_->info_.last_included_index_ >= latest_->info_.last_included_index_))) {
    throw std::runtime_error("invalid snapshot recovery chain");
  }
}
auto ObjectSnapshotStore::Encode(const std::optional<Body> &latest, const std::optional<Body> &previous) const
    -> std::vector<std::byte> {
  ByteWriter w;
  w.PutU64(1);
  for (const auto *slot : {&latest, &previous}) {
    w.PutU8(slot->has_value());
    if (!*slot) {
      continue;
    }
    const auto &b = **slot;
    w.PutU64(b.object_);
    w.PutU64(b.info_.generation_);
    w.PutString(b.info_.snapshot_id_);
    w.PutU64(b.info_.last_included_index_);
    w.PutU64(b.info_.last_included_term_);
    w.PutU64(b.info_.payload_size_);
    w.PutU32(b.info_.payload_checksum_);
  }
  return w.Take();
}
auto ObjectSnapshotStore::Source(const Body &body) -> SnapshotInput {
  auto use = storage_->Lease(body.object_);
  auto storage = storage_;
  return {0, body.info_.payload_size_, [storage, use, object = body.object_](uint64_t offset, size_t size) {
            return storage->Read(object, offset, size);
          }};
}
auto ObjectSnapshotStore::Checksum(const SnapshotInput &input) const -> uint32_t {
  uint32_t crc = 0;
  for (uint64_t offset = 0; offset < input.size_;) {
    auto bytes = input.Read(offset, std::min<uint64_t>(storage_->options_.io_chunk_bytes_, input.size_ - offset));
    crc = Crc32cExtend(crc, bytes.data(), bytes.size());
    offset += bytes.size();
  }
  return crc;
}
auto ObjectSnapshotStore::Latest() const -> std::optional<RaftSnapshot> {
  return latest_ ? std::optional<RaftSnapshot>(latest_->info_) : std::nullopt;
}
auto ObjectSnapshotStore::Oldest() const -> std::optional<RaftSnapshot> {
  return previous_ ? std::optional<RaftSnapshot>(previous_->info_) : Latest();
}
auto ObjectSnapshotStore::Input(const RaftSnapshot &snapshot) -> SnapshotInput {
  for (const auto *body : {&latest_, &previous_}) {
    if (*body && (*body)->info_.generation_ == snapshot.generation_ &&
        (*body)->info_.snapshot_id_ == snapshot.snapshot_id_) {
      return Source(**body);
    }
  }
  throw std::runtime_error("snapshot is no longer retained; acquire a session before retirement");
}
auto ObjectSnapshotStore::Publish(Body body, bool retain) -> RaftSnapshot {
  if (latest_ && body.info_.last_included_index_ <= latest_->info_.last_included_index_) {
    throw std::invalid_argument("non-monotonic snapshot publication");
  }
  body.info_.generation_ = latest_ ? latest_->info_.generation_ + 1 : 1;
  auto next = std::optional<Body>(body);
  auto previous = retain ? latest_ : std::nullopt;
  std::vector<ObjectControlMutation> controls{storage_->Change(3, 0, Encode(next, previous)),
                                              storage_->Own(body.object_, 2, 1)};
  for (const auto *old : {&latest_, &previous_}) {
    if (*old && (!previous || (*old)->object_ != previous->object_)) {
      controls.push_back(storage_->Own((*old)->object_, 2, 2));
    }
  }
  auto result = body.info_;
  storage_->Check(controls);
  storage_->Commit({{}, std::move(controls)});
  latest_.swap(next);
  previous_.swap(previous);
  return result;
}
void ObjectSnapshotStore::RetainOnlyLatest() {
  if (!previous_) {
    return;
  }
  storage_->Commit(
      {{}, {storage_->Change(3, 0, Encode(latest_, std::nullopt)), storage_->Own(previous_->object_, 2, 2)}});
  previous_.reset();
}
auto ObjectSnapshotStore::Capture(uint64_t index, uint64_t term, RaftStateMachine &machine) -> RaftSnapshot {
  if ((index == 0 && term != 0) || (latest_ && index <= latest_->info_.last_included_index_)) {
    throw std::invalid_argument("invalid capture boundary");
  }
  Body body{{1, latest_ ? latest_->info_.generation_ + 1 : 1,
             std::to_string(index) + "-" + std::to_string(term) + "-" + std::to_string(UINT32_MAX), index, term, 0, 0},
            0};
  std::vector<ObjectControlMutation> check{storage_->Change(3, 0, Encode(body, latest_)), storage_->Own(16, 2, 1)};
  if (previous_) {
    check.push_back(storage_->Own(previous_->object_, 2, 2));
  }
  storage_->Check(check);
  body.object_ = storage_->Candidate(2);
  try {
    const auto shared = machine.WriteSharedSnapshot(*storage_->storage_, storage_->Key(body.object_), index, term,
                                                    storage_->options_.max_snapshot_bytes_);
    if (shared) {
      body.info_.payload_size_ = *shared;
    } else {
      machine.WriteSnapshot([&](const auto &bytes) {
        if (bytes.size() > storage_->options_.max_snapshot_bytes_ - body.info_.payload_size_) {
          throw std::invalid_argument("snapshot body budget exceeded");
        }
        if (!bytes.empty()) {
          storage_->Append(body.object_, bytes);
        }
        body.info_.payload_size_ += bytes.size();
      });
    }
    auto input = Source(body);
    body.info_.payload_checksum_ = Checksum(input);
    body.info_.snapshot_id_ =
        std::to_string(index) + "-" + std::to_string(term) + "-" + std::to_string(body.info_.payload_checksum_);
    // The shared producer captured a validated business checkpoint. Rebuilding
    // a temporary database here would undo sharing's purpose. Receivers still
    // validate/install through the production state-machine path.
    if (!shared) machine.ValidateSnapshot(input, index);
    return Publish(body, true);
  } catch (...) {
    const auto error = std::current_exception();
    // A poisoned context rejects this cleanup too; uncertain publication is
    // never converted to a cancelled candidate or deleted on that path.
    try {
      storage_->Commit({{}, {storage_->Own(body.object_, 2, 2)}});
    } catch (...) {
    }
    std::rethrow_exception(error);
  }
}
auto ObjectSnapshotStore::Stage(const SnapshotChunk &chunk) -> SnapshotStageResult {
  if (chunk.snapshot_id_.empty() || chunk.snapshot_id_.size() > 256 ||
      chunk.total_size_ > storage_->options_.max_snapshot_bytes_ || chunk.offset_ > chunk.total_size_ ||
      chunk.data_.size() > chunk.total_size_ - chunk.offset_ ||
      (chunk.last_included_index_ == 0 && chunk.last_included_term_ != 0) ||
      (chunk.done_ && chunk.offset_ + chunk.data_.size() != chunk.total_size_)) {
    throw std::invalid_argument("invalid snapshot chunk");
  }
  if (!download_ || download_->body_.info_.snapshot_id_ != chunk.snapshot_id_) {
    if (chunk.offset_ != 0) {
      throw std::invalid_argument("snapshot receive must start from zero");
    }
    Body body{{1, latest_ ? latest_->info_.generation_ + 1 : 1, chunk.snapshot_id_, chunk.last_included_index_,
               chunk.last_included_term_, chunk.total_size_, chunk.payload_checksum_},
              0};
    auto check =
        std::vector<ObjectControlMutation>{storage_->Change(3, 0, Encode(body, latest_)), storage_->Own(16, 2, 1)};
    if (previous_) {
      check.push_back(storage_->Own(previous_->object_, 2, 2));
    }
    if (latest_) {
      check.push_back(storage_->Own(latest_->object_, 2, 2));
    }
    storage_->Check(check);
    if (download_) {
      Cancel(download_->body_.info_.snapshot_id_);
    }
    body.object_ = storage_->Candidate(2);
    download_ = Download{std::move(body), 0, false};
  }
  auto &d = *download_;
  const auto &info = d.body_.info_;
  if (info.last_included_index_ != chunk.last_included_index_ ||
      info.last_included_term_ != chunk.last_included_term_ || info.payload_size_ != chunk.total_size_ ||
      info.payload_checksum_ != chunk.payload_checksum_) {
    throw std::invalid_argument("snapshot chunk metadata drift");
  }
  if (chunk.offset_ < d.received_ || d.complete_) {
    if (chunk.offset_ > d.received_ || chunk.data_.size() > d.received_ - chunk.offset_ ||
        storage_->Read(d.body_.object_, chunk.offset_, chunk.data_.size()) != chunk.data_) {
      throw std::runtime_error("conflicting duplicate snapshot chunk");
    }
    if (d.complete_) {
      return {SnapshotStageStatus::DUPLICATE_COMPLETE, d.received_};
    }
  } else {
    if (chunk.offset_ != d.received_) {
      throw std::invalid_argument("out of order snapshot chunk");
    }
    if (!chunk.data_.empty()) {
      storage_->Append(d.body_.object_, chunk.data_);
    }
    d.received_ += chunk.data_.size();
  }
  if (chunk.done_) {
    if (d.received_ != info.payload_size_ || Checksum(Source(d.body_)) != info.payload_checksum_) {
      throw std::runtime_error("received snapshot checksum mismatch");
    }
    d.complete_ = true;
    return {SnapshotStageStatus::COMPLETE, d.received_};
  }
  if (d.received_ == info.payload_size_) {
    throw std::invalid_argument("final snapshot chunk missing done");
  }
  return {SnapshotStageStatus::IN_PROGRESS, d.received_};
}
auto ObjectSnapshotStore::Staged(std::string_view id) const -> std::optional<RaftSnapshot> {
  return download_ && download_->complete_ && download_->body_.info_.snapshot_id_ == id
             ? std::optional<RaftSnapshot>(download_->body_.info_)
             : std::nullopt;
}
auto ObjectSnapshotStore::StagedInput(std::string_view id) -> std::optional<SnapshotInput> {
  if (!Staged(id)) {
    return std::nullopt;
  }
  return Source(download_->body_);
}
void ObjectSnapshotStore::Cancel(std::string_view id) {
  if (download_ && download_->body_.info_.snapshot_id_ == id) {
    storage_->Commit({{}, {storage_->Own(download_->body_.object_, 2, 2)}});
    download_.reset();
  }
}
auto ObjectSnapshotStore::PublishStaged(std::string_view id, bool retain) -> RaftSnapshot {
  if (!Staged(id)) {
    throw std::runtime_error("snapshot candidate is incomplete");
  }
  auto result = Publish(download_->body_, retain);
  download_.reset();
  return result;
}
}  // namespace bustub
