#include "storage_config.h"
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#include <algorithm>
#include <charconv>
#include <fstream>
#include <limits>
#include <map>
#include <stdexcept>
#include <system_error>
#include <thread>
#include "raft/object_storage.h"
namespace bustub::cli {
namespace {
auto Trim(const std::string &s) -> std::string {
  const auto first = s.find_first_not_of(" \t\r");
  return first == std::string::npos ? "" : s.substr(first, s.find_last_not_of(" \t\r") - first + 1);
}
class Fields {
 public:
  explicit Fields(const std::filesystem::path &path) {
    std::ifstream input(path);
    if (!input) throw std::runtime_error("cannot read storage config: " + path.string());
    std::string line;
    while (std::getline(input, line)) {
      line = Trim(line.substr(0, line.find('#')));
      if (line.empty()) continue;
      const auto split = line.find('=');
      if (split == std::string::npos || !values_.emplace(Trim(line.substr(0, split)), Trim(line.substr(split + 1))).second)
        throw std::runtime_error("malformed or duplicate storage config field: " + line);
    }
    if (!input.eof()) throw std::runtime_error("storage config read failed");
  }
  auto Take(const std::string &key) -> std::string {
    const auto found = values_.find(key);
    if (found == values_.end() || found->second.empty()) throw std::runtime_error("missing storage config field: " + key);
    auto value = std::move(found->second);
    values_.erase(found);
    return value;
  }
  template <class T> void Number(const std::string &key, T &target) {
    const auto value = Take(key);
    uint64_t number{};
    const auto parsed = std::from_chars(value.data(), value.data() + value.size(), number);
    if (parsed.ec != std::errc() || parsed.ptr != value.data() + value.size() || number > std::numeric_limits<T>::max())
      throw std::runtime_error("invalid storage config integer: " + key);
    target = static_cast<T>(number);
  }
  auto Range(const std::string &key) -> StorageByteRange {
    uint64_t offset{}, bytes{};
    Number(key + ".offset", offset);
    Number(key + ".bytes", bytes);
    const auto range = StorageByteRange::Create(offset, bytes);
    if (!range) throw std::runtime_error("storage config range overflows: " + key);
    return *range;
  }
  auto Identity(const std::string &key) -> std::array<uint8_t, 16> {
    const auto value = Take(key);
    if (value.size() != 32) throw std::runtime_error("identity requires 32 hex digits: " + key);
    std::array<uint8_t, 16> id{};
    for (size_t i = 0; i < id.size(); ++i) {
      unsigned byte{};
      const auto parsed = std::from_chars(value.data() + 2 * i, value.data() + 2 * i + 2, byte, 16);
      if (parsed.ec != std::errc() || parsed.ptr != value.data() + 2 * i + 2) throw std::runtime_error("invalid identity: " + key);
      id[i] = static_cast<uint8_t>(byte);
    }
    return id;
  }
  void Done() const {
    if (!values_.empty()) throw std::runtime_error("unknown storage config field: " + values_.begin()->first);
  }
 private:
  std::map<std::string, std::string> values_;
};
void Durable(const JournalResult &result) {
  if (result.outcome_ != JournalOutcome::Durable) std::rethrow_exception(result.error_);
}
}  // namespace
auto ReadStorageConfig(const std::filesystem::path &path) -> StorageConfig {
  Fields f(path);
  if (f.Take("version") != "1") throw std::runtime_error("unsupported storage config version");
  RaftObjectDeployment d{};
  auto &s = d.storage_;
  s.device_path_ = f.Take("device.path");
  if (s.device_path_.is_relative()) s.device_path_ = std::filesystem::absolute(path).parent_path() / s.device_path_;
  const auto access = f.Take("device.access");
  if (access != "direct" && access != "buffered") throw std::runtime_error("device.access must be direct or buffered");
  s.device_.access_mode_ = access == "direct" ? DeviceAccessMode::Direct : DeviceAccessMode::Buffered;
  s.identity_ = {f.Identity("identity.storage"), f.Identity("identity.device")};
  s.journal_identity_ = f.Identity("identity.journal");
  uint64_t capacity{};
  f.Number("create.capacity", capacity);
  BootstrapLayout layout{s.identity_, capacity, {f.Range("create.metadata"), f.Range("create.journal"), f.Range("create.data")}};
  s.objects_.emplace();
  s.transactions_.emplace();
  s.memory_budget_.emplace();
  d.pages_.emplace();
  f.Number("io.worker_count", s.io_.worker_count_);
  f.Number("io.max_operations", s.io_.max_operations_);
  f.Number("io.max_buffer_bytes", s.io_.max_buffer_bytes_);
  f.Number("io.progress_operations", s.io_.progress_operations_);
  f.Number("io.progress_buffer_bytes", s.io_.progress_buffer_bytes_);
  f.Number("journal.segment_bytes", s.journal_.segment_bytes_);
  f.Number("journal.unit_bytes", s.journal_.unit_bytes_);
  f.Number("journal.max_record_bytes", s.journal_.max_record_bytes_);
  f.Number("journal.max_batch_bytes", s.journal_.max_batch_bytes_);
  f.Number("journal.max_records_per_batch", s.journal_.max_records_per_batch_);
  f.Number("journal.max_group_bytes", s.journal_.max_group_bytes_);
  f.Number("journal.max_group_batches", s.journal_.max_group_batches_);
  f.Number("journal.max_pending_batches", s.journal_.max_pending_batches_);
  f.Number("journal.max_pending_bytes", s.journal_.max_pending_bytes_);
  f.Number("metadata.page_limit", s.metadata_.page_limit_);
  f.Number("metadata.max_live_pages", s.metadata_.max_live_pages_);
  f.Number("metadata.max_value_bytes", s.metadata_.max_value_bytes_);
  f.Number("metadata.max_batch_bytes", s.metadata_.max_batch_bytes_);
  f.Number("memory.bytes", (*s.memory_budget_).bytes_);
  f.Number("memory.grant_bytes", (*s.memory_budget_).grant_bytes_);
  f.Number("memory.progress_bytes", (*s.memory_budget_).progress_bytes_);
  f.Number("allocator.allocation_bytes", s.objects_->allocator_.allocation_bytes_);
  f.Number("allocator.bitmap_record_bytes", s.objects_->allocator_.bitmap_record_bytes_);
  f.Number("allocator.max_bitmap_bytes", s.objects_->allocator_.max_bitmap_bytes_);
  f.Number("allocator.max_request_bytes", s.objects_->allocator_.max_request_bytes_);
  f.Number("allocator.max_extents", s.objects_->allocator_.max_extents_);
  f.Number("allocator.max_reservations", s.objects_->allocator_.max_reservations_);
  f.Number("allocator.max_search_nodes", s.objects_->allocator_.max_search_nodes_);
  f.Number("mapping.max_query_spans", s.objects_->mapping_.max_query_spans_);
  f.Number("mapping.max_update_entries", s.objects_->mapping_.max_update_entries_);
  f.Number("mapping.max_update_bytes", s.objects_->mapping_.max_update_bytes_);
  f.Number("mapping.max_retired_records", s.objects_->mapping_.max_retired_records_);
  f.Number("mapping.max_active_operations", s.objects_->mapping_.max_active_operations_);
  f.Number("references.max_leases", s.objects_->references_.max_leases_);
  f.Number("references.max_ranges_per_lease", s.objects_->references_.max_ranges_per_lease_);
  f.Number("references.max_scan_entries", s.objects_->references_.max_scan_entries_);
  f.Number("references.max_reclaim_ranges", s.objects_->references_.max_reclaim_ranges_);
  f.Number("references.max_reclaim_bytes", s.objects_->references_.max_reclaim_bytes_);
  f.Number("references.max_active_operations", s.objects_->references_.max_active_operations_);
  f.Number("object_io.max_read_bytes", (*s.objects_).max_read_bytes_);
  f.Number("object_io.max_write_bytes", (*s.objects_).max_write_bytes_);
  f.Number("transactions.max_requests", (*s.transactions_).max_requests_);
  f.Number("transactions.max_operations", (*s.transactions_).max_operations_);
  f.Number("transactions.max_request_bytes", (*s.transactions_).max_request_bytes_);
  f.Number("transactions.max_pending_bytes", (*s.transactions_).max_pending_bytes_);
  f.Number("transactions.deferred_max_bytes", (*s.transactions_).deferred_max_bytes_);
  f.Number("transactions.deferred_pending_bytes", (*s.transactions_).deferred_pending_bytes_);
  f.Number("transactions.deferred_pending_tasks", (*s.transactions_).deferred_pending_tasks_);
  f.Number("transactions.integrity_scan", (*s.transactions_).integrity_scan_);
  f.Number("transactions.metadata_writeback_pages", (*s.transactions_).metadata_writeback_pages_);
  f.Number("transactions.metadata_checkpoint_rounds", (*s.transactions_).metadata_checkpoint_rounds_);
  f.Number("raft.segment_bytes", d.raft_.segment_bytes_);
  f.Number("raft.io_chunk_bytes", d.raft_.io_chunk_bytes_);
  f.Number("raft.max_log_bytes", d.raft_.max_log_bytes_);
  f.Number("raft.max_log_entries", d.raft_.max_log_entries_);
  f.Number("raft.max_batch_entries", d.raft_.max_batch_entries_);
  f.Number("raft.max_batch_bytes", d.raft_.max_batch_bytes_);
  f.Number("raft.max_snapshot_bytes", d.raft_.max_snapshot_bytes_);
  f.Number("raft.max_owned_objects", d.raft_.max_owned_objects_);
  f.Number("pages.directory_bytes", d.pages_->cache_.directory_bytes_);
  f.Number("pages.arena_bytes", d.pages_->cache_.arena_bytes_);
  f.Number("pages.max_inflight_pages", d.pages_->cache_.max_inflight_pages_);
  f.Number("pages.advise_huge_pages", d.pages_->cache_.advise_huge_pages_);
  f.Number("pages.prefetch_pages", d.pages_->cache_.prefetch_pages_);
  f.Number("space", d.space_);
  f.Number("pages.pages_per_object", d.pages_->pages_per_object_);
  f.Number("external_buffer_bytes", s.external_buffer_bytes_);
  f.Number("repair.max_attempts", s.repair_.max_attempts_);
  { int64_t ms{}; f.Number("repair.retry_delay_ms", ms); s.repair_.retry_delay_ = std::chrono::milliseconds(ms); }
  { int64_t ms{}; f.Number("repair.admission_timeout_ms", ms); s.repair_.admission_timeout_ = std::chrono::milliseconds(ms); }
  { int64_t ms{}; f.Number("transactions.retry_interval_ms", ms); s.transactions_->retry_interval_ = std::chrono::milliseconds(ms); }
  { int64_t ms{}; f.Number("transactions.gc_interval_ms", ms); s.transactions_->gc_interval_ = std::chrono::milliseconds(ms); }
  f.Done();
  if (s.transactions_->metadata_writeback_pages_ == 0 || s.transactions_->metadata_checkpoint_rounds_ == 0)
    throw std::runtime_error("node deployment requires periodic metadata maintenance");
  return {std::move(d), std::move(layout)};
}
void InitializeStorage(const StorageConfig &config, const DistributedNodeConfig &node) {
  if (config.deployment_.space_ != 1) throw std::runtime_error("fresh deployment creates space 1");
  auto storage = std::make_shared<NodeStorage>(config.deployment_.storage_);
  storage->Create(config.layout_);
  for (;;) {
    try {
      const auto space = storage->CreateObjectSpace(storage->Objects());
      Durable(space.result_);
      if (space.space_ != config.deployment_.space_) throw std::runtime_error("unexpected fresh object space");
      break;
    } catch (const MetadataCommitBusy &) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  auto raft = RaftObjectStorage::Create(storage, config.deployment_.space_, config.deployment_.raft_);
  std::vector<uint64_t> voters{node.node_id_};
  for (const auto &[id, peer] : node.peers_) voters.push_back(id);
  std::sort(voters.begin(), voters.end());
  raft->EnsureIdentity(node.node_id_, node.group_id_, voters);
  raft.reset();
  storage->Close();
}
DeviceLock::DeviceLock(const std::filesystem::path &path) : fd_(::open(path.c_str(), O_RDWR | O_CLOEXEC)) {
  if (fd_ < 0) throw std::system_error(errno, std::generic_category(), "open existing storage device");
  if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) {
    const auto error = errno;
    ::close(fd_);
    throw std::system_error(error, std::generic_category(), "storage device already owned");
  }
}
DeviceLock::~DeviceLock() { ::close(fd_); }
}  // namespace bustub::cli
