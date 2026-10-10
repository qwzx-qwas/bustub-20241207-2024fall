// Replicated session results; all mutations are driven by ordered Raft Apply.
#include "distributed/session_table.h"

#include <array>
#include <limits>
#include <mutex>
#include <stdexcept>

#include "common/byte_codec.h"

namespace bustub {
namespace {
constexpr std::array<std::byte, 8> SESSION_MAGIC{std::byte{'B'}, std::byte{'S'}, std::byte{'T'}, std::byte{'S'},
                                                 std::byte{'E'}, std::byte{'S'}, std::byte{'0'}, std::byte{'1'}};
constexpr size_t MAX_SESSION_SNAPSHOT_BYTES = 64U * 1024U * 1024U;
void ValidateResponse(const WriteResponseV1 &r) {
  if ((r.format_version_ != 1 && r.format_version_ != 2) || r.request_id_ == 0 || r.commit_index_ == 0 ||
      (r.status_ != WriteStatus::COMMITTED && r.status_ != WriteStatus::REJECTED) || r.error_.size() > 512 ||
      (r.format_version_ == 1 && (r.status_ != WriteStatus::COMMITTED || !r.error_.empty())) ||
      (r.status_ == WriteStatus::COMMITTED && !r.error_.empty()))
    throw std::runtime_error("invalid write response");
}
void CheckControl(const SessionRecord &r, uint64_t ack, bool close) {
  if (!r.window_ || ack > r.last_request_id_ || (close && ack != r.last_request_id_))
    throw std::invalid_argument("session confirmation must cover a completed contiguous prefix");
}
}  // namespace

auto WriteResponseCodec::Encode(const WriteResponseV1 &r) -> std::vector<std::byte> {
  ValidateResponse(r);
  ByteWriter w;
  w.PutU32(r.format_version_);
  w.PutU32(static_cast<uint32_t>(r.status_));
  w.PutU64(r.request_id_);
  w.PutU64(r.term_);
  w.PutU64(r.commit_index_);
  if (r.format_version_ == 2) w.PutString(r.error_);
  return w.Take();
}
auto WriteResponseCodec::Decode(const std::vector<std::byte> &bytes) -> WriteResponseV1 {
  ByteReader r(bytes);
  WriteResponseV1 result{r.ReadU32(), static_cast<WriteStatus>(r.ReadU32()), r.ReadU64(), r.ReadU64(), r.ReadU64()};
  if (result.format_version_ == 2) result.error_ = r.ReadString();
  if (!r.Empty()) throw std::runtime_error("write response has trailing bytes");
  ValidateResponse(result);
  return result;
}

auto SessionTable::Classify(uint64_t client, uint64_t request, const RequestFingerprintV1 &fp, bool window) const
    -> RequestDisposition {
  fp.Validate();
  if (request == 0) return RequestDisposition::TOO_OLD;
  std::shared_lock lock(mutex_);
  auto it = sessions_.find(client);
  if (it == sessions_.end()) {
    if (window && window_sessions_ == MAX_WINDOW_SESSIONS) return RequestDisposition::WINDOW_FULL;
    return request == 1 ? RequestDisposition::NEW_REQUEST : RequestDisposition::GAP;
  }
  const auto &r = it->second;
  if (r.window_ != window) return RequestDisposition::MODE_MISMATCH;
  if (r.closed_) return RequestDisposition::CLOSED;
  if (window) {
    if (request <= r.retired_through_) return RequestDisposition::TOO_OLD;
    if (auto old = r.results_.find(request); old != r.results_.end())
      return old->second.fingerprint_ == fp ? RequestDisposition::RETRY_LAST : RequestDisposition::PAYLOAD_MISMATCH;
    if (r.results_.size() == WINDOW_RESULTS) return RequestDisposition::WINDOW_FULL;
  } else if (request == r.last_request_id_) {
    return r.request_fingerprint_ == fp ? RequestDisposition::RETRY_LAST : RequestDisposition::PAYLOAD_MISMATCH;
  }
  if (request <= r.last_request_id_) return RequestDisposition::TOO_OLD;
  return request == r.last_request_id_ + 1 ? RequestDisposition::NEW_REQUEST : RequestDisposition::GAP;
}

auto SessionTable::GetResponse(uint64_t client, uint64_t request) const -> std::optional<std::vector<std::byte>> {
  std::shared_lock lock(mutex_);
  auto it = sessions_.find(client);
  if (it == sessions_.end()) return std::nullopt;
  const auto &r = it->second;
  if (!r.window_) return request == r.last_request_id_ ? std::optional{r.encoded_response_} : std::nullopt;
  auto result = r.results_.find(request);
  return result == r.results_.end() ? std::nullopt : std::optional{result->second.response_};
}
auto SessionTable::GetLastResponse(uint64_t client) const -> std::optional<std::vector<std::byte>> {
  std::shared_lock lock(mutex_);
  auto it = sessions_.find(client);
  if (it == sessions_.end()) return std::nullopt;
  const auto &r = it->second;
  if (!r.window_) return r.encoded_response_;
  auto result = r.results_.find(r.last_request_id_);
  return result == r.results_.end() ? std::nullopt : std::optional{result->second.response_};
}
void SessionTable::RecordCommitted(uint64_t client, uint64_t request, const RequestFingerprintV1 &fp,
                                   const std::vector<std::byte> &bytes, bool window) {
  fp.Validate();
  const auto response = WriteResponseCodec::Decode(bytes);
  if (client == 0 || request == 0 || response.request_id_ != request || (response.format_version_ == 2) != window)
    throw std::runtime_error("committed response does not match session request");
  const auto disposition = Classify(client, request, fp, window);
  if (disposition == RequestDisposition::RETRY_LAST) {
    if (GetResponse(client, request) != bytes) throw std::runtime_error("retry response differs from original");
    return;
  }
  if (disposition != RequestDisposition::NEW_REQUEST) throw std::runtime_error("invalid committed session sequence");
  std::unique_lock lock(mutex_);
  auto [it, inserted] = sessions_.try_emplace(client);
  auto &r = it->second;
  if (window) {
    r.results_.emplace(request, SessionResult{fp, bytes});
    if (inserted) ++window_sessions_;
    r.window_ = true;
    r.last_request_id_ = request;
    r.state_index_ = response.commit_index_;
    r.state_term_ = response.term_;
  } else {
    r = SessionRecord{request, fp, bytes};
  }
}
auto SessionTable::ValidateControl(uint64_t client, uint64_t ack, bool close) const -> bool {
  std::shared_lock lock(mutex_);
  auto it = sessions_.find(client);
  if (it == sessions_.end()) throw std::invalid_argument("unknown window session");
  CheckControl(it->second, ack, close);
  return ack > it->second.retired_through_ || (close && !it->second.closed_);
}
void SessionTable::ApplyControl(uint64_t client, uint64_t ack, bool close, uint64_t index, uint64_t term) {
  std::unique_lock lock(mutex_);
  auto &r = sessions_.at(client);
  CheckControl(r, ack, close);
  r.retired_through_ = std::max(r.retired_through_, ack);
  r.results_.erase(r.results_.begin(), r.results_.upper_bound(r.retired_through_));
  r.closed_ = r.closed_ || close;
  r.state_index_ = index;
  r.state_term_ = term;
}
void SessionTable::ValidateSnapshotBoundary(uint64_t index, std::optional<uint64_t> term) const {
  std::shared_lock lock(mutex_);
  for (const auto &[client, r] : sessions_) {
    auto check = [&](const auto &bytes) {
      const auto response = WriteResponseCodec::Decode(bytes);
      if (response.commit_index_ > index || (term && response.term_ != *term))
        throw std::runtime_error("session result exceeds snapshot boundary");
    };
    if (r.window_ && (r.state_index_ > index || (term && r.state_term_ != *term)))
      throw std::runtime_error("session confirmation exceeds snapshot boundary");
    if (!r.window_) check(r.encoded_response_);
    for (const auto &[id, result] : r.results_) check(result.response_);
  }
}
auto SessionTable::SnapshotRecords() const -> std::map<uint64_t, SessionRecord> {
  std::shared_lock lock(mutex_);
  return sessions_;
}
void SessionTable::RestoreRecords(std::map<uint64_t, SessionRecord> records) {
  size_t windows = 0;
  for (const auto &[client, r] : records) {
    if (client == 0 || r.last_request_id_ == 0) throw std::runtime_error("invalid session identity");
    if (!r.window_) {
      r.request_fingerprint_.Validate();
      if (WriteResponseCodec::Decode(r.encoded_response_).request_id_ != r.last_request_id_ || r.closed_ ||
          r.retired_through_ || !r.results_.empty())
        throw std::runtime_error("invalid legacy session");
      continue;
    }
    if (++windows > MAX_WINDOW_SESSIONS || r.state_index_ == 0 || r.retired_through_ > r.last_request_id_ ||
        r.last_request_id_ - r.retired_through_ != r.results_.size() || r.results_.size() > WINDOW_RESULTS ||
        (r.closed_ && !r.results_.empty()) || !r.encoded_response_.empty())
      throw std::runtime_error("invalid session result window");
    uint64_t expected = r.retired_through_;
    for (const auto &[id, result] : r.results_) {
      result.fingerprint_.Validate();
      const auto decoded = WriteResponseCodec::Decode(result.response_);
      if (id != ++expected || decoded.request_id_ != id || decoded.format_version_ != 2 ||
          decoded.commit_index_ > r.state_index_)
        throw std::runtime_error("invalid window result identity");
    }
  }
  std::unique_lock lock(mutex_);
  sessions_ = std::move(records);
  window_sessions_ = windows;
}
auto SessionSnapshotCodec::Encode(const SessionTable &sessions) -> std::vector<std::byte> {
  const auto records = sessions.SnapshotRecords();
  uint32_t version = 2;
  for (const auto &[client, r] : records)
    if (r.window_) version = 3;
  ByteWriter w;
  w.PutU32(static_cast<uint32_t>(records.size()));
  auto result = [&](const auto &fp, const auto &bytes) {
    w.PutBytes(RequestFingerprintCodec::Encode(fp));
    w.PutU32(static_cast<uint32_t>(bytes.size()));
    w.PutBytes(bytes);
  };
  for (const auto &[client, r] : records) {
    w.PutU64(client);
    w.PutU64(r.last_request_id_);
    if (version == 3) w.PutU32(r.window_ ? 1 : 0);
    if (!r.window_) {
      result(r.request_fingerprint_, r.encoded_response_);
      continue;
    }
    w.PutU64(r.retired_through_);
    w.PutU32(r.closed_ ? 1 : 0);
    w.PutU64(r.state_index_);
    w.PutU64(r.state_term_);
    w.PutU32(static_cast<uint32_t>(r.results_.size()));
    for (const auto &[id, value] : r.results_) {
      w.PutU64(id);
      result(value.fingerprint_, value.response_);
    }
  }
  return EncodeVersionedFrame(
      {SESSION_MAGIC.data(), SESSION_MAGIC.size(), version, MAX_SESSION_SNAPSHOT_BYTES, "session snapshot"}, w.Data());
}
void SessionSnapshotCodec::DecodeInto(const std::vector<std::byte> &bytes, SessionTable *sessions) {
  ByteReader header(bytes);
  header.ReadBytes(8);
  const auto version = header.ReadU32();
  if (sessions == nullptr || (version != 2 && version != 3)) throw std::runtime_error("unsupported session snapshot");
  auto payload = DecodeVersionedFrame(
      {SESSION_MAGIC.data(), SESSION_MAGIC.size(), version, MAX_SESSION_SNAPSHOT_BYTES, "session snapshot"}, bytes);
  ByteReader r(payload);
  const auto count = r.ReadU32();
  std::map<uint64_t, SessionRecord> records;
  auto result = [&]() {
    auto fp = RequestFingerprintCodec::Decode(r.ReadBytes(RequestFingerprintCodec::ENCODED_BYTES));
    auto size = r.ReadU32();
    return SessionResult{fp, r.ReadBytes(size)};
  };
  for (uint32_t n = 0; n < count; ++n) {
    auto client = r.ReadU64();
    SessionRecord record;
    record.last_request_id_ = r.ReadU64();
    auto mode = version == 3 ? r.ReadU32() : 0;
    if (mode > 1) throw std::runtime_error("invalid session mode");
    record.window_ = mode == 1;
    if (!record.window_) {
      auto value = result();
      record.request_fingerprint_ = value.fingerprint_;
      record.encoded_response_ = std::move(value.response_);
    } else {
      record.retired_through_ = r.ReadU64();
      auto closed = r.ReadU32();
      if (closed > 1) throw std::runtime_error("invalid session close state");
      record.closed_ = closed == 1;
      record.state_index_ = r.ReadU64();
      record.state_term_ = r.ReadU64();
      auto size = r.ReadU32();
      if (size > SessionTable::WINDOW_RESULTS) throw std::runtime_error("session window exceeds limit");
      for (uint32_t i = 0; i < size; ++i) {
        auto id = r.ReadU64();
        if (!record.results_.emplace(id, result()).second) throw std::runtime_error("duplicate session result");
      }
    }
    if (!records.emplace(client, std::move(record)).second) throw std::runtime_error("duplicate session identity");
  }
  if (!r.Empty()) throw std::runtime_error("session snapshot has trailing bytes");
  sessions->RestoreRecords(std::move(records));
}
}  // namespace bustub
