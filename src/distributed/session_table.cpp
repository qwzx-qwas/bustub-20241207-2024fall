//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// session_table.cpp
//
//===----------------------------------------------------------------------===//

#include "distributed/session_table.h"

#include <array>
#include <limits>
#include <mutex>  // NOLINT(build/c++11)
#include <stdexcept>

#include "common/byte_codec.h"

namespace bustub {
namespace {

constexpr std::array<std::byte, 8> SESSION_MAGIC{std::byte{'B'}, std::byte{'S'}, std::byte{'T'}, std::byte{'S'},
                                                 std::byte{'E'}, std::byte{'S'}, std::byte{'0'}, std::byte{'1'}};
constexpr size_t MAX_SESSION_SNAPSHOT_BYTES = 64U * 1024U * 1024U;
constexpr size_t WRITE_RESPONSE_SIZE = sizeof(uint32_t) * 2 + sizeof(uint64_t) * 3;

}  // namespace

auto WriteResponseCodec::Encode(const WriteResponseV1 &response) -> std::vector<std::byte> {
  if (response.format_version_ != 1 || response.status_ != WriteStatus::COMMITTED || response.request_id_ == 0 ||
      response.commit_index_ == 0) {
    throw std::runtime_error("invalid WriteResponseV1");
  }
  ByteWriter writer;
  writer.PutU32(response.format_version_);
  writer.PutU32(static_cast<uint32_t>(response.status_));
  writer.PutU64(response.request_id_);
  writer.PutU64(response.term_);
  writer.PutU64(response.commit_index_);
  return writer.Take();
}

auto WriteResponseCodec::Decode(const std::vector<std::byte> &bytes) -> WriteResponseV1 {
  if (bytes.size() != WRITE_RESPONSE_SIZE) {
    throw std::runtime_error("invalid WriteResponseV1 length");
  }
  ByteReader reader(bytes);
  WriteResponseV1 response{reader.ReadU32(), static_cast<WriteStatus>(reader.ReadU32()), reader.ReadU64(),
                           reader.ReadU64(), reader.ReadU64()};
  if (response.format_version_ != 1 || response.status_ != WriteStatus::COMMITTED || response.request_id_ == 0 ||
      response.commit_index_ == 0) {
    throw std::runtime_error("unsupported WriteResponseV1");
  }
  return response;
}

auto SessionTable::Classify(uint64_t client_id, uint64_t request_id) const -> RequestDisposition {
  std::shared_lock lock(mutex_);
  const auto iterator = sessions_.find(client_id);
  const uint64_t last_request_id = iterator == sessions_.end() ? 0 : iterator->second.last_request_id_;
  if (request_id == last_request_id && request_id != 0) {
    return RequestDisposition::RETRY_LAST;
  }
  if (request_id <= last_request_id) {
    return RequestDisposition::TOO_OLD;
  }
  if (request_id == last_request_id + 1) {
    return RequestDisposition::NEW_REQUEST;
  }
  return RequestDisposition::GAP;
}

auto SessionTable::GetLastResponse(uint64_t client_id) const -> std::optional<std::vector<std::byte>> {
  std::shared_lock lock(mutex_);
  const auto iterator = sessions_.find(client_id);
  if (iterator == sessions_.end()) {
    return std::nullopt;
  }
  return iterator->second.encoded_response_;
}

void SessionTable::RecordCommitted(uint64_t client_id, uint64_t request_id,
                                   const std::vector<std::byte> &encoded_response) {
  const auto response = WriteResponseCodec::Decode(encoded_response);
  if (client_id == 0 || request_id == 0 || response.request_id_ != request_id) {
    throw std::runtime_error("committed response does not match its session request");
  }
  std::unique_lock lock(mutex_);
  const auto iterator = sessions_.find(client_id);
  const uint64_t last_request_id = iterator == sessions_.end() ? 0 : iterator->second.last_request_id_;
  if (request_id == last_request_id && request_id != 0) {
    if (iterator->second.encoded_response_ != encoded_response) {
      throw std::runtime_error("retry response bytes differ from the committed response");
    }
    return;
  }
  if (request_id != last_request_id + 1) {
    throw std::runtime_error("session request sequence has a gap");
  }
  sessions_.insert_or_assign(client_id, SessionRecord{request_id, encoded_response});
}

void SessionTable::ValidateSnapshotBoundary(uint64_t last_included_index) const {
  std::shared_lock lock(mutex_);
  for (const auto &[client_id, record] : sessions_) {
    const auto response = WriteResponseCodec::Decode(record.encoded_response_);
    if (client_id == 0 || record.last_request_id_ == 0 || response.request_id_ != record.last_request_id_ ||
        response.commit_index_ > last_included_index) {
      throw std::runtime_error("session response lies beyond the snapshot boundary");
    }
  }
}

auto SessionTable::SnapshotRecords() const -> std::map<uint64_t, SessionRecord> {
  std::shared_lock lock(mutex_);
  return sessions_;
}

void SessionTable::RestoreRecords(std::map<uint64_t, SessionRecord> records) {
  for (const auto &[client_id, record] : records) {
    if (client_id == 0 || record.last_request_id_ == 0 ||
        WriteResponseCodec::Decode(record.encoded_response_).request_id_ != record.last_request_id_) {
      throw std::runtime_error("invalid session snapshot record");
    }
  }
  std::unique_lock lock(mutex_);
  sessions_ = std::move(records);
}

auto SessionSnapshotCodec::Encode(const SessionTable &sessions) -> std::vector<std::byte> {
  ByteWriter payload;
  const auto records = sessions.SnapshotRecords();
  payload.PutU32(static_cast<uint32_t>(records.size()));
  for (const auto &[client_id, record] : records) {
    payload.PutU64(client_id);
    payload.PutU64(record.last_request_id_);
    payload.PutU32(static_cast<uint32_t>(record.encoded_response_.size()));
    payload.PutBytes(record.encoded_response_);
  }
  if (payload.Data().size() > MAX_SESSION_SNAPSHOT_BYTES) {
    throw std::runtime_error("session snapshot exceeds the V1 size limit");
  }
  return EncodeVersionedFrame(
      {SESSION_MAGIC.data(), SESSION_MAGIC.size(), FORMAT_VERSION, MAX_SESSION_SNAPSHOT_BYTES, "session snapshot"},
      payload.Data());
}

void SessionSnapshotCodec::DecodeInto(const std::vector<std::byte> &bytes, SessionTable *sessions) {
  if (sessions == nullptr) {
    throw std::runtime_error("invalid session snapshot size");
  }
  const auto payload = DecodeVersionedFrame(
      {SESSION_MAGIC.data(), SESSION_MAGIC.size(), FORMAT_VERSION, MAX_SESSION_SNAPSHOT_BYTES, "session snapshot"},
      bytes);

  ByteReader body(payload);
  const auto count = body.ReadU32();
  std::map<uint64_t, SessionRecord> records;
  for (uint32_t i = 0; i < count; i++) {
    const auto client_id = body.ReadU64();
    const auto request_id = body.ReadU64();
    const auto response_size = body.ReadU32();
    if (response_size > body.Remaining() || !records.emplace(client_id, SessionRecord{}).second) {
      throw std::runtime_error("invalid or duplicate session snapshot record");
    }
    records[client_id] = {request_id, body.ReadBytes(response_size)};
  }
  if (!body.Empty()) {
    throw std::runtime_error("session snapshot has trailing bytes");
  }
  sessions->RestoreRecords(std::move(records));
}

}  // namespace bustub
