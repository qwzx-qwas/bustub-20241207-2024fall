#include "object_snapshot_store.h"
#include <algorithm>
#include <stdexcept>
#include "common/byte_codec.h"
#include "recovery/page_snapshot.h"
namespace bustub {
ObjectSnapshotStore::ObjectSnapshotStore(std::shared_ptr<RaftObjectStorage> storage) : storage_(std::move(storage)) {
  const auto root = storage_->Control(3, 0);
  if (!root) {
    return;
  }
  ByteReader r(*root);
  const auto version = r.ReadU64();
  if (version != 1 && version != 2) {
    throw std::runtime_error("unknown snapshot manifest");
  }
  for (auto *slot : {&latest_, &previous_, &transfer_base_}) {
    if (slot == &transfer_base_ && version == 1) break;
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
      (previous_ && (!latest_ || previous_->info_.last_included_index_ >= latest_->info_.last_included_index_)) ||
      (transfer_base_ &&
       (!latest_ || transfer_base_->info_.last_included_index_ >= latest_->info_.last_included_index_ ||
        (previous_ && transfer_base_->object_ == previous_->object_)))) {
    throw std::runtime_error("invalid snapshot recovery chain");
  }
}
auto ObjectSnapshotStore::Encode(const std::optional<Body> &latest, const std::optional<Body> &previous,
                                 const std::optional<Body> &base) const -> std::vector<std::byte> {
  ByteWriter w;
  w.PutU64(2);
  for (const auto *slot : {&latest, &previous, &base}) {
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
  return {0, body.info_.payload_size_,
          [storage, use, object = body.object_](uint64_t offset, size_t size) {
            return storage->Read(object, offset, size);
          },
          [storage, use, object = body.object_](uint64_t offset, std::byte *data, size_t size,
                                                std::shared_ptr<void> owner) {
            auto read =
                storage->storage_->ReadObjectInto(storage->storage_->Objects(), storage->Key(object), offset, size,
                                                  {reinterpret_cast<char *>(data), size, std::move(owner)});
            read.Finish();
            if (read.Size() != size) throw std::runtime_error("snapshot body was truncated");
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
  for (const auto *body : {&latest_, &previous_, &transfer_base_}) {
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
  // Rotation preserves at most one retired recovery image, under the existing
  // per-snapshot byte and namespace ownership budgets. No body is copied.
  auto base = previous_ ? previous_ : transfer_base_;
  if (!retain && latest_) base = latest_;
  if (base) {
    const std::vector<std::byte> magic(PAGE_SNAPSHOT_MAGIC.begin(), PAGE_SNAPSHOT_MAGIC.end());
    auto input = Source(*base);
    if (input.size_ < magic.size() || input.Read(0, magic.size()) != magic) base.reset();
  }
  std::vector<ObjectControlMutation> controls{storage_->Change(3, 0, Encode(next, previous, base)),
                                              storage_->Own(body.object_, 2, 1)};
  for (const auto *old : {&latest_, &previous_, &transfer_base_}) {
    if (*old && (!previous || (*old)->object_ != previous->object_) && (!base || (*old)->object_ != base->object_)) {
      controls.push_back(storage_->Own((*old)->object_, 2, 2));
    }
  }
  if (download_ && download_->delta_ && download_->body_.object_ == body.object_) {
    controls.push_back(storage_->Own(download_->delta_->scratch_, 2, 2));
  }
  auto result = body.info_;
  storage_->Check(controls);
  storage_->Commit({{}, std::move(controls)});
  latest_.swap(next);
  previous_.swap(previous);
  transfer_base_.swap(base);
  plan_cache_.reset();
  return result;
}
void ObjectSnapshotStore::RetainOnlyLatest() {
  if (!previous_) {
    return;
  }
  storage_->Commit({{},
                    {storage_->Change(3, 0, Encode(latest_, std::nullopt, transfer_base_)),
                     storage_->Own(previous_->object_, 2, 2)}});
  previous_.reset();
  plan_cache_.reset();
}
auto ObjectSnapshotStore::Capture(uint64_t index, uint64_t term, RaftStateMachine &machine) -> RaftSnapshot {
  if ((index == 0 && term != 0) || (latest_ && index <= latest_->info_.last_included_index_)) {
    throw std::invalid_argument("invalid capture boundary");
  }
  Body body{{1, latest_ ? latest_->info_.generation_ + 1 : 1,
             std::to_string(index) + "-" + std::to_string(term) + "-" + std::to_string(UINT32_MAX), index, term, 0, 0},
            0};
  std::vector<ObjectControlMutation> check{
      storage_->Change(3, 0, Encode(body, latest_, previous_ ? previous_ : transfer_base_)), storage_->Own(16, 2, 1)};
  if (previous_) {
    check.push_back(storage_->Own(previous_->object_, 2, 2));
  }
  if (transfer_base_) check.push_back(storage_->Own(transfer_base_->object_, 2, 2));
  storage_->Check(check);
  body.object_ = storage_->Candidate(2);
  try {
    const auto shared = machine.WriteSharedSnapshot(*storage_->storage_, storage_->Key(body.object_), index, term,
                                                    storage_->options_.max_snapshot_bytes_);
    if (shared) {
      body.info_.payload_size_ = shared->size_;
      body.info_.payload_checksum_ = shared->checksum_;
    } else {
      machine.WriteSnapshot([&](const auto &bytes) {
        if (bytes.size() > storage_->options_.max_snapshot_bytes_ - body.info_.payload_size_) {
          throw std::invalid_argument("snapshot body budget exceeded");
        }
        if (!bytes.empty()) {
          storage_->Append(body.object_, bytes);
        }
        body.info_.payload_checksum_ = Crc32cExtend(body.info_.payload_checksum_, bytes.data(), bytes.size());
        body.info_.payload_size_ += bytes.size();
      });
    }
    auto input = Source(body);
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
namespace {
auto SameSnapshot(const RaftSnapshot &a, const RaftSnapshot &b) -> bool {
  return a.snapshot_id_ == b.snapshot_id_ && a.last_included_index_ == b.last_included_index_ &&
         a.last_included_term_ == b.last_included_term_ && a.payload_size_ == b.payload_size_ &&
         a.payload_checksum_ == b.payload_checksum_ && a.format_version_ == b.format_version_;
}
auto SameContent(const ObjectMappingSnapshot &view, ObjectKey a, uint64_t x, ObjectKey b, uint64_t y, uint64_t size)
    -> bool {
  while (size != 0) {
    const auto left = view.Resolve(a, x, size), right = view.Resolve(b, y, size);
    if (left.spans_.empty() || right.spans_.empty()) throw std::runtime_error("missing immutable snapshot mapping");
    const auto &l = left.spans_.front(), &r = right.spans_.front();
    if (!l.data_ || !r.data_) return false;
    const auto lo = l.data_->owner_.value_or(a), ro = r.data_->owner_.value_or(b);
    if (lo.space_ != ro.space_ || lo.number_ != ro.number_ || l.data_->allocation_ != r.data_->allocation_ ||
        l.data_->offset_ != r.data_->offset_)
      return false;
    const auto take = std::min(l.size_, r.size_);
    x += take;
    y += take;
    size -= take;
  }
  return true;
}
}  // namespace

struct ObjectSnapshotStore::DeltaPlanCache {
  std::function<std::optional<SnapshotDelta>()> build_;
  std::optional<SnapshotDelta> result_;
  bool ready_{false};
  auto Get() -> std::optional<SnapshotDelta> {
    if (!ready_) {
      result_ = build_();
      ready_ = true;
      build_ = {};
    }
    return result_;
  }
};

auto ObjectSnapshotStore::OfferDelta(const RaftSnapshot &target) -> std::optional<SnapshotDeltaOffer> {
  const auto &base = transfer_base_ ? transfer_base_ : previous_;
  if (!latest_ || !base || !SameSnapshot(target, latest_->info_)) return std::nullopt;
  if (!plan_cache_) {
    auto input = Source(*latest_), base_input = Source(*base);
    // Only the fixed header is needed to recognize the native format.
    const std::vector<std::byte> magic(PAGE_SNAPSHOT_MAGIC.begin(), PAGE_SNAPSHOT_MAGIC.end());
    if (input.size_ < magic.size() || base_input.size_ < magic.size() || input.Read(0, magic.size()) != magic ||
        base_input.Read(0, magic.size()) != magic)
      return std::nullopt;
    auto cache = std::make_shared<DeltaPlanCache>();
    cache->build_ = [storage = storage_, target_body = *latest_, base_body = *base, target, input,
                     base_input]() -> std::optional<SnapshotDelta> {
      const auto dir = ReadPageSnapshotDirectory(input, target.last_included_index_);
      const auto old = ReadPageSnapshotDirectory(base_input, base_body.info_.last_included_index_);
      std::vector<uint64_t> reused(dir->pages_.size(), UINT64_MAX);
      const auto view = storage->storage_->Objects();
      size_t j = 0, count = 0;
      for (size_t i = 0; i < dir->pages_.size(); ++i) {
        while (j < old->pages_.size() && old->pages_[j] < dir->pages_[i]) ++j;
        if (j < old->pages_.size() && old->pages_[j] == dir->pages_[i] &&
            SameContent(view, storage->Key(target_body.object_), dir->header_size_ + i * BUSTUB_PAGE_SIZE,
                        storage->Key(base_body.object_), old->header_size_ + j * BUSTUB_PAGE_SIZE, BUSTUB_PAGE_SIZE)) {
          reused[i] = old->header_size_ + j * BUSTUB_PAGE_SIZE;
          ++count;
        }
      }
      if (count == 0) return std::nullopt;
      constexpr uint64_t chunk_bytes = 64U * 1024U;
      uint64_t messages = (dir->header_size_ + chunk_bytes - 1) / chunk_bytes;
      for (size_t first = 0; first < reused.size();) {
        size_t last = first + 1;
        while (last < reused.size() && (last - first) * BUSTUB_PAGE_SIZE < chunk_bytes &&
               (reused[first] == UINT64_MAX ? reused[last] == UINT64_MAX
                                            : reused[last] == reused[first] + (last - first) * BUSTUB_PAGE_SIZE))
          ++last;
        ++messages;
        first = last;
      }
      const auto full_messages = (target.payload_size_ + chunk_bytes - 1) / chunk_bytes;
      const auto saved = uint64_t{count} * BUSTUB_PAGE_SIZE;
      // REUSE's fixed descriptor is charged too. A fragmented plan must save an
      // extra page-equivalent for each additional stop-and-wait exchange.
      const auto overhead =
          messages * 32 + (messages > full_messages ? messages - full_messages : 0) * BUSTUB_PAGE_SIZE;
      if (saved <= overhead || saved - overhead < target.payload_size_ / 5) return std::nullopt;
      return SnapshotDelta{
          base_body.info_,
          [target, input, base_input, header = dir->header_size_,
           reused = std::make_shared<const std::vector<uint64_t>>(std::move(reused))](uint64_t offset, size_t maximum) {
            (void)base_input;  // Keeps the base body leased even when no body read is needed.
            if (offset >= target.payload_size_ || offset % BUSTUB_PAGE_SIZE != 0 || maximum < BUSTUB_PAGE_SIZE)
              throw std::invalid_argument("invalid delta read range");
            auto size =
                std::min<uint64_t>(maximum / BUSTUB_PAGE_SIZE * BUSTUB_PAGE_SIZE, target.payload_size_ - offset);
            std::optional<SnapshotReuse> reuse;
            if (offset < header) {
              size = std::min(size, header - offset);
            } else {
              const auto first = (offset - header) / BUSTUB_PAGE_SIZE;
              size = BUSTUB_PAGE_SIZE;
              while (size + BUSTUB_PAGE_SIZE <= maximum && offset + size < target.payload_size_) {
                const auto next = (*reused)[first + size / BUSTUB_PAGE_SIZE];
                if ((*reused)[first] == UINT64_MAX ? next != UINT64_MAX : next != (*reused)[first] + size) break;
                size += BUSTUB_PAGE_SIZE;
              }
              if ((*reused)[first] != UINT64_MAX) reuse = SnapshotReuse{(*reused)[first], size};
            }
            SnapshotChunk chunk{target.snapshot_id_,
                                target.last_included_index_,
                                target.last_included_term_,
                                offset,
                                target.payload_size_,
                                target.payload_checksum_,
                                offset + size == target.payload_size_,
                                {}};
            if (reuse)
              chunk.reuse_ = reuse;
            else
              chunk.data_ = input.Read(offset, size);
            return chunk;
          }};
    };
    plan_cache_ = std::move(cache);
  }
  return SnapshotDeltaOffer{base->info_, [cache = plan_cache_] { return cache->Get(); }};
}

auto ObjectSnapshotStore::BeginDelta(const RaftSnapshot &target, const RaftSnapshot &base, uint64_t session) -> bool {
  if (session == 0 || target.snapshot_id_.empty() || target.snapshot_id_.size() > 256 || target.payload_size_ == 0 ||
      target.payload_size_ > storage_->options_.max_snapshot_bytes_ || target.payload_size_ % BUSTUB_PAGE_SIZE != 0 ||
      target.last_included_index_ <= base.last_included_index_)
    throw std::invalid_argument("invalid delta offer");
  std::optional<Body> source;
  for (const auto *b : {&latest_, &previous_, &transfer_base_})
    if (*b && SameSnapshot((*b)->info_, base)) source = **b;
  if (!source) return false;
  auto input = Source(*source);
  if (!ReadPageSnapshotDirectory(input, base.last_included_index_)) return false;
  // Like full reception, reject a publication that can never fit before
  // allocating candidates. Include scratch retirement and both old roots:
  // Raft decides whether to keep a recovery base only after validation.
  // Fixed-width object IDs below are sizing placeholders, never allocations.
  const Body planned{target, 16};
  std::vector<ObjectControlMutation> check{
      storage_->Change(3, 0, Encode(planned, latest_, previous_ ? previous_ : transfer_base_)), storage_->Own(16, 2, 1),
      storage_->Own(17, 2, 2)};
  for (const auto *old : {&latest_, &previous_, &transfer_base_})
    if (*old) check.push_back(storage_->Own((*old)->object_, 2, 2));
  storage_->Check(check);
  if (download_) Cancel(download_->body_.info_.snapshot_id_);
  Body body{target, storage_->Candidate(2)};
  try {
    const auto scratch = storage_->Candidate(2);
    download_ = Download{body, 0, false, DeltaReceive{session, scratch, source->object_, 0, std::move(input), {}}};
  } catch (...) {
    const auto error = std::current_exception();
    try {
      storage_->Commit({{}, {storage_->Own(body.object_, 2, 2)}});
    } catch (...) {
    }
    std::rethrow_exception(error);
  }
  return true;
}

auto ObjectSnapshotStore::StageDelta(const SnapshotChunk &chunk) -> SnapshotStageResult {
  if (!download_ || !download_->delta_ || download_->delta_->session_ != chunk.delta_session_)
    throw std::runtime_error("delta session is not accepted");
  auto &d = *download_;
  auto &delta = *d.delta_;
  const auto &info = d.body_.info_;
  const auto size = chunk.reuse_ ? chunk.reuse_->length_ : chunk.data_.size();
  if (chunk.snapshot_id_ != info.snapshot_id_ || chunk.last_included_index_ != info.last_included_index_ ||
      chunk.last_included_term_ != info.last_included_term_ || chunk.total_size_ != info.payload_size_ ||
      chunk.payload_checksum_ != info.payload_checksum_ || size == 0 || size > 64U * 1024U ||
      size % BUSTUB_PAGE_SIZE != 0 || chunk.offset_ % BUSTUB_PAGE_SIZE != 0 || chunk.offset_ > info.payload_size_ ||
      size > info.payload_size_ - chunk.offset_ || chunk.done_ != (chunk.offset_ + size == info.payload_size_) ||
      (chunk.reuse_ && (!chunk.data_.empty() || chunk.reuse_->offset_ > delta.base_input_.size_ ||
                        size > delta.base_input_.size_ - chunk.reuse_->offset_)))
    throw std::invalid_argument("invalid delta range or target identity");
  if (chunk.offset_ < d.received_) {
    const auto it = std::lower_bound(delta.parts_.begin(), delta.parts_.end(), chunk.offset_,
                                     [](const Part &p, uint64_t offset) { return p.offset_ < offset; });
    if (it == delta.parts_.end() || it->offset_ != chunk.offset_ || it->size_ != size ||
        it->reuse_ != chunk.reuse_.has_value() ||
        (it->reuse_ ? it->source_ != chunk.reuse_->offset_
                    : storage_->Read(delta.scratch_, it->source_, size) != chunk.data_))
      throw std::runtime_error("conflicting duplicate delta chunk");
    return {d.complete_ ? SnapshotStageStatus::DUPLICATE_COMPLETE : SnapshotStageStatus::IN_PROGRESS, d.received_};
  }
  if (chunk.offset_ != d.received_ || d.complete_) throw std::runtime_error("out of order delta chunk");
  // Reserve the descriptor before durable IO; no acknowledged range can lack its in-memory source.
  if (delta.parts_.size() == delta.parts_.capacity()) {
    const auto limit = info.payload_size_ / BUSTUB_PAGE_SIZE;
    delta.parts_.reserve(std::min<uint64_t>(limit, std::max<size_t>(16, delta.parts_.capacity() * 2)));
  }
  const auto source = chunk.reuse_ ? chunk.reuse_->offset_ : delta.written_;
  if (!chunk.reuse_) {
    storage_->Append(delta.scratch_, chunk.data_);
    delta.written_ += size;
  }
  delta.parts_.push_back({chunk.offset_, size, source, chunk.reuse_.has_value()});
  d.received_ += size;
  if (chunk.done_) {
    FinishDelta();
    d.complete_ = true;
    return {SnapshotStageStatus::COMPLETE, d.received_};
  }
  return {SnapshotStageStatus::IN_PROGRESS, d.received_};
}

void ObjectSnapshotStore::FinishDelta() {
  auto &d = *download_;
  auto &delta = *d.delta_;
  // The final object receives immutable mappings for scratch and base, without
  // another body copy or an expanded Common write into neighboring shared ranges.
  storage_->Commit({{{ObjectOperation::Resize,
                      storage_->Key(d.body_.object_),
                      d.body_.info_.payload_size_,
                      ObjectSizeMode::Variable,
                      {}}},
                    {}});
  for (const auto &part : delta.parts_) {
    const auto object = part.reuse_ ? delta.base_object_ : delta.scratch_;
    storage_->storage_->ShareObjectRangeBatched(storage_->storage_->Objects(), storage_->Key(object),
                                                storage_->Key(d.body_.object_), part.source_, part.size_, part.offset_);
  }
  const auto input = Source(d.body_);
  if (Checksum(input) != d.body_.info_.payload_checksum_ ||
      !ReadPageSnapshotDirectory(input, d.body_.info_.last_included_index_))
    throw std::runtime_error("reconstructed delta snapshot checksum or format mismatch");
  // Keep scratch readable until publication/cancellation so a repeated DATA can still be compared.
}

auto ObjectSnapshotStore::Stage(const SnapshotChunk &chunk) -> SnapshotStageResult {
  if (chunk.delta_session_ != 0) return StageDelta(chunk);
  if (chunk.reuse_) throw std::invalid_argument("reuse without a delta session");
  if (download_ && download_->delta_) Cancel(download_->body_.info_.snapshot_id_);
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
    auto check = std::vector<ObjectControlMutation>{
        storage_->Change(3, 0, Encode(body, latest_, previous_ ? previous_ : transfer_base_)), storage_->Own(16, 2, 1)};
    if (previous_) {
      check.push_back(storage_->Own(previous_->object_, 2, 2));
    }
    if (latest_) {
      check.push_back(storage_->Own(latest_->object_, 2, 2));
    }
    if (transfer_base_) check.push_back(storage_->Own(transfer_base_->object_, 2, 2));
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
    d.checksum_ = Crc32cExtend(d.checksum_, chunk.data_.data(), chunk.data_.size());
    d.received_ += chunk.data_.size();
  }
  if (chunk.done_) {
    if (d.received_ != info.payload_size_ || d.checksum_ != info.payload_checksum_) {
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
    std::vector<ObjectControlMutation> retire{storage_->Own(download_->body_.object_, 2, 2)};
    if (download_->delta_) retire.push_back(storage_->Own(download_->delta_->scratch_, 2, 2));
    storage_->Commit({{}, std::move(retire)});
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
