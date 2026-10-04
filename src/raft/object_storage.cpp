#include "raft/object_storage.h"

#include <algorithm>
#include <map>
#include <stdexcept>

#include "common/byte_codec.h"

namespace bustub {
namespace {
constexpr uint64_t MAGIC = 0x4253545241465431ULL;
constexpr uint64_t OWNERS = 4;
auto Words(std::initializer_list<uint64_t> words) -> std::vector<std::byte> {
  ByteWriter out;
  for (auto word : words) {
    out.PutU64(word);
  }
  return out.Take();
}
}  // namespace
struct RaftObjectStorage::State {
  std::mutex mutex_;
  std::map<uint64_t, size_t> users_;
  std::exception_ptr error_;
};
RaftObjectStorage::RaftObjectStorage(std::shared_ptr<NodeStorage> storage, uint64_t space, RaftObjectOptions options)
    : storage_(std::move(storage)), space_(space), options_(options), state_(std::make_shared<State>()) {
  if (!storage_ || space == 0 || options.segment_bytes_ < 64 || options.segment_bytes_ > UINT32_MAX ||
      options.max_owned_objects_ == 0 || options.io_chunk_bytes_ == 0 ||
      options.io_chunk_bytes_ > options.segment_bytes_ || options.max_batch_entries_ == 0 ||
      options.max_batch_entries_ > options.max_log_entries_ || options.max_batch_bytes_ == 0 ||
      options.max_batch_bytes_ > options.max_log_bytes_ || options.max_snapshot_bytes_ == 0) {
    throw std::invalid_argument("invalid Raft object storage configuration");
  }
}
auto RaftObjectStorage::Create(std::shared_ptr<NodeStorage> storage, uint64_t space, RaftObjectOptions options)
    -> std::shared_ptr<RaftObjectStorage> {
  auto out = std::shared_ptr<RaftObjectStorage>(new RaftObjectStorage(std::move(storage), space, options));
  if (out->Control(0, 0)) {
    throw std::runtime_error("Raft object namespace already initialized");
  }
  // The fixed root is also the explicit presence marker; Open cannot create it.
  out->Commit({{}, {out->Change(0, 0, Words({MAGIC, 1, 16, options.segment_bytes_}))}});
  return out;
}
auto RaftObjectStorage::Open(std::shared_ptr<NodeStorage> storage, uint64_t space, RaftObjectOptions options)
    -> std::shared_ptr<RaftObjectStorage> {
  auto out = std::shared_ptr<RaftObjectStorage>(new RaftObjectStorage(std::move(storage), space, options));
  const auto root = out->Control(0, 0);
  if (!root) {
    throw std::runtime_error("Raft object namespace is not initialized");
  }
  ByteReader in(*root);
  if (in.ReadU64() != MAGIC || in.ReadU64() != 1) {
    throw std::runtime_error("unknown Raft object format");
  }
  out->next_object_ = in.ReadU64();
  if (in.ReadU64() != options.segment_bytes_ || !in.Empty() || out->next_object_ < 16) {
    throw std::runtime_error("Raft segment layout mismatch");
  }
  // A process restart cancels preparation; published objects have status Live.
  const auto owners = out->Scan(OWNERS);
  if (owners.size() > options.max_owned_objects_) {
    throw std::runtime_error("Raft ownership exceeds configured budget");
  }
  out->owned_objects_ = owners.size();
  for (const auto &e : owners) {
    ByteReader r(e.value_);
    const auto kind = r.ReadU64();
    const auto status = r.ReadU64();
    if (!r.Empty() || kind > 2 || kind == 0 || status > 2) {
      throw std::runtime_error("invalid object owner");
    }
    if (status == 0) {
      out->Commit({{}, {out->Own(e.item_, kind, 2)}});
    }
  }
  return out;
}
void RaftObjectStorage::EnsureIdentity(uint64_t node, const std::string &group, const std::vector<uint64_t> &voters) {
  ByteWriter out;
  out.PutU64(node);
  out.PutString(group);
  out.PutU64(voters.size());
  for (auto voter : voters) {
    out.PutU64(voter);
  }
  auto previous = Control(0, 1);
  if (previous) {
    if (*previous != out.Data()) {
      throw std::runtime_error("Raft object namespace belongs to another member");
    }
    return;
  }
  Commit({{}, {Change(0, 1, out.Take())}});
}
auto RaftObjectStorage::Control(uint64_t owner, uint64_t item) -> std::optional<std::vector<std::byte>> {
  return storage_->Objects().Control(Key(owner), item);
}
auto RaftObjectStorage::Scan(uint64_t owner) -> std::vector<ObjectControlEntry> {
  auto view = storage_->Objects();
  std::vector<ObjectControlEntry> result;
  uint64_t cursor = 0;
  for (;;) {
    auto page = view.Controls(Key(owner), cursor, 1);
    if (page.empty()) {
      return result;
    }
    cursor = page.back().item_ + 1;
    result.push_back(std::move(page.front()));
    if (result.size() > options_.max_owned_objects_ + 1) {
      throw std::runtime_error("Raft control directory exceeds configured budget");
    }
  }
}
auto RaftObjectStorage::Change(uint64_t owner, uint64_t item, std::optional<std::vector<std::byte>> value)
    -> ObjectControlMutation {
  return {Key(owner), item, std::move(value)};
}
void RaftObjectStorage::Check(const std::vector<ObjectControlMutation> &controls) const {
  storage_->CheckControlBatch(controls);
}
void RaftObjectStorage::Commit(ObjectTransaction transaction) {
  {
    std::lock_guard lock(state_->mutex_);
    if (state_->error_) {
      std::rethrow_exception(state_->error_);
    }
  }
  auto submitted = storage_->SubmitObjects(transaction);
  if (submitted.admission_ != IOAdmission::Accepted) {
    throw std::runtime_error("Raft object submission not admitted");
  }
  submitted.ticket_->Wait();
  const auto result = submitted.ticket_->Result();
  if (result.outcome_ != JournalOutcome::Durable) {
    auto error =
        result.error_ ? result.error_ : std::make_exception_ptr(std::runtime_error("Raft object commit failed"));
    if (result.outcome_ == JournalOutcome::Indeterminate) {
      std::lock_guard lock(state_->mutex_);
      state_->error_ = error;
    }
    std::rethrow_exception(error);
  }
}
auto RaftObjectStorage::Own(uint64_t object, uint64_t kind, uint64_t status) -> ObjectControlMutation {
  return Change(OWNERS, object, Words({kind, status}));
}
auto RaftObjectStorage::Candidate(uint64_t kind) -> uint64_t {
  std::lock_guard lock(allocation_mutex_);
  if (next_object_ == UINT64_MAX) {
    throw std::runtime_error("Raft object identities exhausted");
  }
  if (owned_objects_ >= options_.max_owned_objects_) {
    throw std::runtime_error("Raft object ownership budget exhausted; maintenance or retention must make progress");
  }
  const auto id = next_object_;
  Commit({{{ObjectOperation::Create, Key(id), 0, ObjectSizeMode::Variable, {}}},
          {Change(0, 0, Words({MAGIC, 1, id + 1, options_.segment_bytes_})), Own(id, kind, 0)}});
  ++next_object_;
  ++owned_objects_;
  return id;
}
auto RaftObjectStorage::Lease(uint64_t object) -> std::shared_ptr<void> {
  std::lock_guard lock(state_->mutex_);
  if (state_->error_) {
    std::rethrow_exception(state_->error_);
  }
  const auto owner = Control(OWNERS, object);
  if (!owner) {
    throw std::runtime_error("Raft body has been deleted");
  }
  ByteReader in(*owner);
  in.ReadU64();
  if (in.ReadU64() == 2) {
    throw std::runtime_error("Raft body is retired");
  }
  struct Use {
    std::shared_ptr<State> state_;
    uint64_t object_;
    bool registered_{false};
    ~Use() {
      if (!registered_) {
        return;
      }
      std::lock_guard guard(state_->mutex_);
      if (--state_->users_.at(object_) == 0) {
        state_->users_.erase(object_);
      }
    }
  };
  auto use = std::make_shared<Use>();
  use->state_ = state_;
  use->object_ = object;
  ++state_->users_[object];
  use->registered_ = true;
  return use;
}
void RaftObjectStorage::Append(uint64_t object, const std::vector<std::byte> &bytes) {
  size_t offset = 0;
  while (offset < bytes.size()) {
    const auto size = std::min<uint64_t>(options_.io_chunk_bytes_, bytes.size() - offset);
    std::vector<std::byte> part(bytes.begin() + static_cast<ptrdiff_t>(offset),
                                bytes.begin() + static_cast<ptrdiff_t>(offset + size));
    Commit({{{ObjectOperation::Append, Key(object), 0, ObjectSizeMode::Variable, std::move(part)}}, {}});
    offset += size;
  }
}
auto RaftObjectStorage::Read(uint64_t object, uint64_t offset, size_t size) -> std::vector<std::byte> {
  std::vector<std::byte> out(size);
  size_t done = 0;
  while (done < size) {
    const auto amount = std::min<uint64_t>(options_.io_chunk_bytes_, size - done);
    // Published byte ranges are immutable. A fresh mapping permits tail COW
    // without keeping obsolete physical mappings pinned between requests.
    auto read = storage_->ReadObject(storage_->Objects(), Key(object), offset + done, amount);
    read.Wait();
    if (read.Size() != amount) {
      throw std::runtime_error("Raft object body truncated");
    }
    read.CopyTo(out.data() + done, amount);
    done += amount;
  }
  return out;
}
auto RaftObjectStorage::Collect(size_t limit) -> size_t {
  size_t count = 0;
  // Bound examined records as well as deletes. The owning node serializes
  // maintenance; cursor is only a search hint, durable ownership is authority.
  for (size_t examined = 0; examined < limit; ++examined) {
    auto page = storage_->Objects().Controls(Key(OWNERS), gc_cursor_, 1);
    if (page.empty()) {
      gc_cursor_ = 0;
      break;
    }
    const auto &e = page.front();
    gc_cursor_ = e.item_ + 1;
    ByteReader in(e.value_);
    in.ReadU64();
    if (in.ReadU64() != 2) {
      continue;
    }
    {
      std::lock_guard lock(state_->mutex_);
      if (state_->users_.count(e.item_) != 0) {
        continue;
      }
      // Retired state already prevents acquiring a new lease.
    }
    const auto remaining = storage_->Objects().PlanTailTrim(Key(e.item_), options_.io_chunk_bytes_);
    if (remaining != 0) {
      // Small receive chunks can create many mappings within one IO chunk.
      // Bound both bytes and mapping work; persisted length is the restart
      // cursor. Keep ownership until the final remove, so no orphan is lost.
      Commit({{{ObjectOperation::Resize, Key(e.item_), remaining, ObjectSizeMode::Variable, {}}}, {}});
      gc_cursor_ = e.item_;
      continue;
    }
    Commit({{{ObjectOperation::Remove, Key(e.item_), 0, ObjectSizeMode::Variable, {}}},
            {Change(OWNERS, e.item_, std::nullopt)}});
    --owned_objects_;
    ++count;
  }
  return count;
}
}  // namespace bustub
