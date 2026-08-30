//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// byte_codec.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace bustub {

/** A small, ABI-independent big-endian writer for durable and wire formats. */
class ByteWriter {
 public:
  void PutU8(uint8_t value);
  void PutU32(uint32_t value);
  void PutU64(uint64_t value);
  void PutI64(int64_t value);
  void PutBytes(const void *data, size_t size);
  void PutBytes(const std::vector<std::byte> &bytes);
  void PutString(std::string_view value);

  auto Data() const -> const std::vector<std::byte> & { return data_; }
  auto Take() -> std::vector<std::byte> { return std::move(data_); }

 private:
  std::vector<std::byte> data_;
};

/** Bounds-checked counterpart to ByteWriter. */
class ByteReader {
 public:
  explicit ByteReader(const std::vector<std::byte> &data) : data_(data.data()), size_(data.size()) {}
  ByteReader(const std::byte *data, size_t size) : data_(data), size_(size) {}

  auto ReadU8() -> uint8_t;
  auto ReadU32() -> uint32_t;
  auto ReadU64() -> uint64_t;
  auto ReadI64() -> int64_t;
  auto ReadBytes(size_t size) -> std::vector<std::byte>;
  auto ReadString() -> std::string;
  void Skip(size_t size);

  auto Remaining() const -> size_t { return size_ - offset_; }
  auto Offset() const -> size_t { return offset_; }
  auto Empty() const -> bool { return Remaining() == 0; }

 private:
  void Require(size_t size) const;

  const std::byte *data_;
  size_t size_;
  size_t offset_{0};
};

/** CRC-32C (Castagnoli), used only as an integrity checksum, never as a digest. */
auto Crc32c(const std::byte *data, size_t size) -> uint32_t;
auto Crc32c(const std::vector<std::byte> &data) -> uint32_t;
auto Crc32cExtend(uint32_t previous_crc, const std::byte *data, size_t size) -> uint32_t;

/** Description of a stable magic/version/length/payload/CRC frame. */
struct VersionedFrameSpec {
  const std::byte *magic_;
  size_t magic_size_;
  uint32_t version_;
  size_t maximum_payload_size_;
  std::string_view name_;
};

/** Encodes without changing the protocol-specific payload. CRC covers payload bytes only. */
auto EncodeVersionedFrame(const VersionedFrameSpec &spec, const std::vector<std::byte> &payload)
    -> std::vector<std::byte>;

/** Validates magic/version/length/CRC and returns the protocol-specific payload. */
auto DecodeVersionedFrame(const VersionedFrameSpec &spec, const std::vector<std::byte> &frame)
    -> std::vector<std::byte>;

/** Validates an exact magic/version/length prefix and returns its bounded payload size. */
auto VersionedFramePayloadSize(const VersionedFrameSpec &spec, const std::vector<std::byte> &prefix) -> size_t;

/**
 * Existing formats such as Snapshot bundle use magic/body/CRC without a
 * separate outer version/length header. These helpers share their boundary
 * and checksum validation while leaving the body layout byte-for-byte intact.
 */
auto EncodeChecksummedFrame(const std::byte *magic, size_t magic_size, const std::vector<std::byte> &body,
                            size_t maximum_body_size, std::string_view name) -> std::vector<std::byte>;
auto DecodeChecksummedFrame(const std::byte *magic, size_t magic_size, const std::vector<std::byte> &frame,
                            size_t maximum_body_size, std::string_view name) -> std::vector<std::byte>;

}  // namespace bustub
