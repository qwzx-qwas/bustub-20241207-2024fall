//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_codec.cpp
//
//===----------------------------------------------------------------------===//

#include "recovery/log_codec.h"

#include <stdexcept>

#include "common/byte_codec.h"

namespace bustub {
namespace {

constexpr uint32_t LOG_MAGIC = 0x42524c47U;  // "BRLG"

auto IsKnownEntryType(EntryType type) -> bool {
  return type == EntryType::COMMAND_BATCH || type == EntryType::NOOP || type == EntryType::KV_COMMAND;
}

}  // namespace

auto LogCodec::Encode(const ReplicatedLogEntry &entry) -> std::vector<std::byte> {
  if (entry.format_version_ != FORMAT_VERSION || entry.index_ == 0 || !IsKnownEntryType(entry.type_) ||
      entry.payload_.size() > MAX_PAYLOAD_BYTES || (entry.type_ == EntryType::NOOP && !entry.payload_.empty())) {
    throw std::runtime_error("invalid replicated log entry");
  }
  ByteWriter protected_body;
  protected_body.PutU32(entry.format_version_);
  protected_body.PutU64(entry.index_);
  protected_body.PutU64(entry.term_);
  protected_body.PutU32(static_cast<uint32_t>(entry.type_));
  protected_body.PutU32(static_cast<uint32_t>(entry.payload_.size()));
  protected_body.PutBytes(entry.payload_);

  ByteWriter frame;
  frame.PutU32(LOG_MAGIC);
  frame.PutU32(static_cast<uint32_t>(protected_body.Data().size() + sizeof(uint32_t)));
  frame.PutBytes(protected_body.Data());
  frame.PutU32(Crc32c(protected_body.Data()));
  return frame.Take();
}

auto LogCodec::DecodeOne(const std::byte *data, size_t size) -> LogDecodeResult {
  if (size < FRAME_HEADER_BYTES) {
    return {LogDecodeStatus::TRUNCATED, std::nullopt, 0, "partial log frame header"};
  }
  try {
    ByteReader header(data, size);
    if (header.ReadU32() != LOG_MAGIC) {
      return {LogDecodeStatus::CORRUPT, std::nullopt, 0, "invalid log frame magic"};
    }
    const auto body_size = header.ReadU32();
    if (body_size < FRAME_BODY_FIXED_BYTES || body_size > MAX_PAYLOAD_BYTES + FRAME_BODY_FIXED_BYTES) {
      return {LogDecodeStatus::CORRUPT, std::nullopt, 0, "invalid log frame length"};
    }
    if (size < FRAME_HEADER_BYTES + body_size) {
      return {LogDecodeStatus::TRUNCATED, std::nullopt, 0, "partial log frame payload"};
    }

    const auto protected_size = body_size - sizeof(uint32_t);
    ByteReader body(data + FRAME_HEADER_BYTES, protected_size);
    ReplicatedLogEntry entry;
    entry.format_version_ = body.ReadU32();
    entry.index_ = body.ReadU64();
    entry.term_ = body.ReadU64();
    entry.type_ = static_cast<EntryType>(body.ReadU32());
    const auto payload_size = body.ReadU32();
    if (payload_size != body.Remaining() || payload_size > MAX_PAYLOAD_BYTES) {
      return {LogDecodeStatus::CORRUPT, std::nullopt, 0, "log payload length mismatch"};
    }
    entry.payload_ = body.ReadBytes(payload_size);
    ByteReader checksum_reader(data + FRAME_HEADER_BYTES + protected_size, sizeof(uint32_t));
    if (Crc32c(data + FRAME_HEADER_BYTES, protected_size) != checksum_reader.ReadU32()) {
      return {LogDecodeStatus::CORRUPT, std::nullopt, 0, "log frame checksum mismatch"};
    }
    if (entry.format_version_ != FORMAT_VERSION || entry.index_ == 0 || !IsKnownEntryType(entry.type_) ||
        (entry.type_ == EntryType::NOOP && !entry.payload_.empty())) {
      return {LogDecodeStatus::CORRUPT, std::nullopt, 0, "unsupported replicated log entry"};
    }
    return {LogDecodeStatus::COMPLETE, std::move(entry), FRAME_HEADER_BYTES + body_size, {}};
  } catch (const std::exception &exception) {
    return {LogDecodeStatus::CORRUPT, std::nullopt, 0, exception.what()};
  }
}

auto LogCodec::DecodeOne(const std::vector<std::byte> &data, size_t offset) -> LogDecodeResult {
  if (offset > data.size()) {
    return {LogDecodeStatus::CORRUPT, std::nullopt, 0, "log decode offset exceeds buffer"};
  }
  return DecodeOne(data.data() + offset, data.size() - offset);
}

}  // namespace bustub
