//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// node_directory.cpp
//
//===----------------------------------------------------------------------===//

#include "recovery/node_directory.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#include <array>
#include <cerrno>
#include <limits>
#include <stdexcept>
#include <system_error>  // NOLINT(build/c++11)

#include "common/byte_codec.h"

namespace bustub {
namespace {

constexpr std::array<std::byte, 8> NODE_IDENTITY_MAGIC{std::byte{'B'}, std::byte{'S'}, std::byte{'N'}, std::byte{'O'},
                                                       std::byte{'D'}, std::byte{'E'}, std::byte{'0'}, std::byte{'1'}};
constexpr uint32_t NODE_IDENTITY_VERSION = 1;
constexpr size_t MAX_NODE_IDENTITY_BYTES = 4096;

auto NodeIdentityFrameSpec() -> VersionedFrameSpec {
  return {NODE_IDENTITY_MAGIC.data(), NODE_IDENTITY_MAGIC.size(), NODE_IDENTITY_VERSION, MAX_NODE_IDENTITY_BYTES,
          "node identity"};
}

auto EncodeNodeIdentity(uint64_t node_id, const std::string &group_id, const std::vector<uint64_t> &voters)
    -> std::vector<std::byte> {
  if (node_id == 0 || group_id.empty() || group_id.size() > 128 || voters.empty() ||
      voters.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("invalid node identity");
  }
  ByteWriter payload;
  payload.PutU64(node_id);
  payload.PutString(group_id);
  payload.PutU32(static_cast<uint32_t>(voters.size()));
  for (const auto voter : voters) {
    payload.PutU64(voter);
  }
  return EncodeVersionedFrame(NodeIdentityFrameSpec(), payload.Take());
}

void DecodeAndMatchNodeIdentity(const std::vector<std::byte> &frame, uint64_t expected_node_id,
                                const std::string &expected_group_id, const std::vector<uint64_t> &expected_voters) {
  const auto payload = DecodeVersionedFrame(NodeIdentityFrameSpec(), frame);
  ByteReader reader(payload);
  const auto node_id = reader.ReadU64();
  const auto group_id = reader.ReadString();
  const auto voter_count = reader.ReadU32();
  std::vector<uint64_t> voters;
  voters.reserve(voter_count);
  for (uint32_t offset = 0; offset < voter_count; offset++) {
    voters.push_back(reader.ReadU64());
  }
  if (!reader.Empty()) {
    throw std::runtime_error("node identity contains trailing bytes");
  }
  if (node_id != expected_node_id || group_id != expected_group_id || voters != expected_voters) {
    throw std::runtime_error("node directory identity does not match configured node_id/group/voters");
  }
}

void CreateDirectoryTreeDurably(const std::filesystem::path &root, DurableStorage *storage) {
  std::vector<std::filesystem::path> missing;
  auto existing_ancestor = root;
  while (!storage->Exists(existing_ancestor)) {
    missing.push_back(existing_ancestor);
    const auto parent = existing_ancestor.parent_path();
    if (parent.empty() || parent == existing_ancestor) {
      throw std::runtime_error("node directory has no existing ancestor");
    }
    existing_ancestor = parent;
  }
  if (missing.empty()) {
    return;
  }

  storage->CreateDirectories(root);
  // create_directories may add several path components. Sync from the new leaf back to the first existing ancestor so
  // every child directory entry, including the top-level root entry, is durable when Open returns.
  for (const auto &directory : missing) {
    storage->SyncDirectory(directory);
  }
  storage->SyncDirectory(existing_ancestor);
}

}  // namespace

auto NodeDirectory::Open(std::filesystem::path root, std::shared_ptr<DurableStorage> storage)
    -> std::unique_ptr<NodeDirectory> {
  if (root.empty() || storage == nullptr) {
    throw std::runtime_error("invalid node directory");
  }
  root = std::filesystem::absolute(root).lexically_normal();
  CreateDirectoryTreeDurably(root, storage.get());
  storage->CreateDirectories(root / "raft" / "log");
  storage->CreateDirectories(root / "state");
  storage->CreateDirectories(root / "working");
  storage->SyncDirectory(root / "raft");
  storage->SyncDirectory(root);

  const auto lock_path = root / "LOCK";
  const auto lock_fd = open(lock_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
  if (lock_fd < 0) {
    throw std::system_error(errno, std::generic_category(), "open node LOCK: " + lock_path.string());
  }
  if (flock(lock_fd, LOCK_EX | LOCK_NB) != 0) {
    const auto error = errno;
    close(lock_fd);
    throw std::system_error(error, std::generic_category(), "node directory is already locked: " + root.string());
  }
  storage->SyncFile(lock_path);
  storage->SyncDirectory(root);
  return std::unique_ptr<NodeDirectory>(new NodeDirectory(std::move(root), std::move(storage), lock_fd));
}

NodeDirectory::~NodeDirectory() {
  if (lock_fd_ >= 0) {
    flock(lock_fd_, LOCK_UN);
    close(lock_fd_);
  }
}

void NodeDirectory::EnsureIdentity(uint64_t node_id, const std::string &group_id, const std::vector<uint64_t> &voters) {
  const auto identity_path = root_ / "node.conf";
  if (storage_->Exists(identity_path)) {
    DecodeAndMatchNodeIdentity(storage_->ReadFile(identity_path, MAX_NODE_IDENTITY_BYTES), node_id, group_id, voters);
    return;
  }

  const auto temporary_path = root_ / "node.conf.tmp";
  storage_->WriteFile(temporary_path, EncodeNodeIdentity(node_id, group_id, voters));
  storage_->SyncFile(temporary_path);
  storage_->Rename(temporary_path, identity_path);
  storage_->SyncDirectory(root_);
}

}  // namespace bustub
