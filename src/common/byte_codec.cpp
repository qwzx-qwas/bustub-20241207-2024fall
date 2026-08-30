//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// byte_codec.cpp
//
//===----------------------------------------------------------------------===//

#include "common/byte_codec.h"

#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>

namespace bustub {

void ByteWriter::PutU8(uint8_t value) { data_.push_back(static_cast<std::byte>(value)); }

void ByteWriter::PutU32(uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    PutU8(static_cast<uint8_t>((value >> shift) & 0xffU));
  }
}

void ByteWriter::PutU64(uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    PutU8(static_cast<uint8_t>((value >> shift) & 0xffU));
  }
}

void ByteWriter::PutI64(int64_t value) { PutU64(static_cast<uint64_t>(value)); }

void ByteWriter::PutBytes(const void *data, size_t size) {
  const auto *first = static_cast<const std::byte *>(data);
  data_.insert(data_.end(), first, first + size);
}

void ByteWriter::PutBytes(const std::vector<std::byte> &bytes) { PutBytes(bytes.data(), bytes.size()); }

void ByteWriter::PutString(std::string_view value) {
  if (value.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::length_error("string is too large for the durable format");
  }
  PutU32(static_cast<uint32_t>(value.size()));
  PutBytes(value.data(), value.size());
}

void ByteReader::Require(size_t size) const {
  if (size > Remaining()) {
    throw std::runtime_error("truncated binary record");
  }
}

auto ByteReader::ReadU8() -> uint8_t {
  Require(1);
  return static_cast<uint8_t>(data_[offset_++]);
}

auto ByteReader::ReadU32() -> uint32_t {
  Require(4);
  uint32_t value = 0;
  for (size_t i = 0; i < 4; i++) {
    value = (value << 8U) | ReadU8();
  }
  return value;
}

auto ByteReader::ReadU64() -> uint64_t {
  Require(8);
  uint64_t value = 0;
  for (size_t i = 0; i < 8; i++) {
    value = (value << 8U) | ReadU8();
  }
  return value;
}

auto ByteReader::ReadI64() -> int64_t { return static_cast<int64_t>(ReadU64()); }

auto ByteReader::ReadBytes(size_t size) -> std::vector<std::byte> {
  Require(size);
  std::vector<std::byte> result(data_ + offset_, data_ + offset_ + size);
  offset_ += size;
  return result;
}

auto ByteReader::ReadString() -> std::string {
  const auto size = ReadU32();
  Require(size);
  std::string result(reinterpret_cast<const char *>(data_ + offset_), size);
  offset_ += size;
  return result;
}

void ByteReader::Skip(size_t size) {
  Require(size);
  offset_ += size;
}

auto Crc32cExtend(uint32_t previous_crc, const std::byte *data, size_t size) -> uint32_t {
  uint32_t crc = ~previous_crc;
  for (size_t i = 0; i < size; i++) {
    crc ^= static_cast<uint8_t>(data[i]);
    for (int bit = 0; bit < 8; bit++) {
      const uint32_t mask = 0U - (crc & 1U);
      crc = (crc >> 1U) ^ (0x82f63b78U & mask);
    }
  }
  return ~crc;
}

auto Crc32c(const std::byte *data, size_t size) -> uint32_t { return Crc32cExtend(0, data, size); }

auto Crc32c(const std::vector<std::byte> &data) -> uint32_t { return Crc32c(data.data(), data.size()); }

namespace {

void ValidateMagic(const std::byte *magic, size_t magic_size, std::string_view name) {
  if (magic == nullptr || magic_size == 0 || name.empty()) {
    throw std::runtime_error("invalid binary frame specification");
  }
}

auto FrameError(std::string_view name, std::string_view detail) -> std::runtime_error {
  return std::runtime_error(std::string(name) + " " + std::string(detail));
}

}  // namespace

auto EncodeVersionedFrame(const VersionedFrameSpec &spec, const std::vector<std::byte> &payload)
    -> std::vector<std::byte> {
  ValidateMagic(spec.magic_, spec.magic_size_, spec.name_);
  if (payload.size() > spec.maximum_payload_size_ || payload.size() > std::numeric_limits<uint32_t>::max()) {
    throw FrameError(spec.name_, "payload exceeds its limit");
  }
  ByteWriter writer;
  writer.PutBytes(spec.magic_, spec.magic_size_);
  writer.PutU32(spec.version_);
  writer.PutU32(static_cast<uint32_t>(payload.size()));
  writer.PutBytes(payload);
  writer.PutU32(Crc32c(payload));
  return writer.Take();
}

auto VersionedFramePayloadSize(const VersionedFrameSpec &spec, const std::vector<std::byte> &prefix) -> size_t {
  ValidateMagic(spec.magic_, spec.magic_size_, spec.name_);
  if (prefix.size() != spec.magic_size_ + sizeof(uint32_t) * 2) {
    throw FrameError(spec.name_, "prefix length mismatch");
  }
  ByteReader reader(prefix);
  if (reader.ReadBytes(spec.magic_size_) != std::vector<std::byte>(spec.magic_, spec.magic_ + spec.magic_size_)) {
    throw FrameError(spec.name_, "magic mismatch");
  }
  if (reader.ReadU32() != spec.version_) {
    throw FrameError(spec.name_, "version is unsupported");
  }
  const auto payload_size = reader.ReadU32();
  if (payload_size > spec.maximum_payload_size_) {
    throw FrameError(spec.name_, "payload exceeds its limit");
  }
  return payload_size;
}

auto DecodeVersionedFrame(const VersionedFrameSpec &spec, const std::vector<std::byte> &frame)
    -> std::vector<std::byte> {
  const auto prefix_size = spec.magic_size_ + sizeof(uint32_t) * 2;
  if (frame.size() < prefix_size + sizeof(uint32_t)) {
    throw FrameError(spec.name_, "is truncated");
  }
  const std::vector<std::byte> prefix(frame.begin(), frame.begin() + static_cast<ptrdiff_t>(prefix_size));
  const auto payload_size = VersionedFramePayloadSize(spec, prefix);
  if (frame.size() != prefix_size + payload_size + sizeof(uint32_t)) {
    throw FrameError(spec.name_, "length mismatch");
  }
  ByteReader reader(frame.data() + prefix_size, frame.size() - prefix_size);
  auto payload = reader.ReadBytes(payload_size);
  if (reader.ReadU32() != Crc32c(payload)) {
    throw FrameError(spec.name_, "checksum mismatch");
  }
  return payload;
}

auto EncodeChecksummedFrame(const std::byte *magic, size_t magic_size, const std::vector<std::byte> &body,
                            size_t maximum_body_size, std::string_view name) -> std::vector<std::byte> {
  ValidateMagic(magic, magic_size, name);
  if (body.size() > maximum_body_size) {
    throw FrameError(name, "body exceeds its limit");
  }
  ByteWriter writer;
  writer.PutBytes(magic, magic_size);
  writer.PutBytes(body);
  writer.PutU32(Crc32c(body));
  return writer.Take();
}

auto DecodeChecksummedFrame(const std::byte *magic, size_t magic_size, const std::vector<std::byte> &frame,
                            size_t maximum_body_size, std::string_view name) -> std::vector<std::byte> {
  ValidateMagic(magic, magic_size, name);
  if (frame.size() < magic_size + sizeof(uint32_t) ||
      frame.size() > magic_size + maximum_body_size + sizeof(uint32_t)) {
    throw FrameError(name, "size is invalid");
  }
  ByteReader reader(frame);
  if (reader.ReadBytes(magic_size) != std::vector<std::byte>(magic, magic + magic_size)) {
    throw FrameError(name, "magic mismatch");
  }
  auto body = reader.ReadBytes(reader.Remaining() - sizeof(uint32_t));
  if (reader.ReadU32() != Crc32c(body)) {
    throw FrameError(name, "checksum mismatch");
  }
  return body;
}

}  // namespace bustub
