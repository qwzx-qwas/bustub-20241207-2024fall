//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// versioned_frame_test.cpp
//
//===----------------------------------------------------------------------===//

#include <array>
#include <cstdint>
#include <initializer_list>
#include <vector>

#include "common/byte_codec.h"
#include "gtest/gtest.h"

namespace bustub {
namespace {

auto Bytes(std::initializer_list<uint8_t> values) -> std::vector<std::byte> {
  std::vector<std::byte> result;
  result.reserve(values.size());
  for (const auto value : values) {
    result.push_back(static_cast<std::byte>(value));
  }
  return result;
}

}  // namespace

TEST(VersionedFrameTest, Crc32cMatchesStandardKnownAnswer) {
  const auto input = Bytes({0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39});
  EXPECT_EQ(Crc32c(input), 0xe3069283U);
}

TEST(VersionedFrameTest, MatchesGoldenAndRejectsEveryOuterBoundary) {
  constexpr std::array<std::byte, 4> magic{std::byte{'T'}, std::byte{'E'}, std::byte{'S'}, std::byte{'T'}};
  const VersionedFrameSpec spec{magic.data(), magic.size(), 7, 16, "test frame"};
  const std::vector<std::byte> payload{std::byte{1}, std::byte{2}, std::byte{3}};
  const auto encoded = EncodeVersionedFrame(spec, payload);

  // Assembled directly from the documented V1 layout. The CRC-32C bytes are fixed literals, not produced by
  // EncodeVersionedFrame or Crc32c while constructing the expected value.
  const auto golden = Bytes({0x54, 0x45, 0x53, 0x54, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03,
                             0xf1, 0x30, 0xf2, 0x1e});
  EXPECT_EQ(encoded, golden);
  EXPECT_EQ(DecodeVersionedFrame(spec, golden), payload);
  EXPECT_EQ(VersionedFramePayloadSize(spec, {golden.begin(), golden.begin() + 12}), 3);

  for (size_t size = 0; size < encoded.size(); size++) {
    EXPECT_THROW(DecodeVersionedFrame(spec, {encoded.begin(), encoded.begin() + static_cast<ptrdiff_t>(size)}),
                 std::runtime_error)
        << "accepted prefix size " << size;
  }
  auto corrupt_magic = encoded;
  corrupt_magic[0] ^= std::byte{1};
  EXPECT_THROW(DecodeVersionedFrame(spec, corrupt_magic), std::runtime_error);
  auto corrupt_version = encoded;
  corrupt_version[7] ^= std::byte{1};
  EXPECT_THROW(DecodeVersionedFrame(spec, corrupt_version), std::runtime_error);
  auto corrupt_length = encoded;
  corrupt_length[11] ^= std::byte{1};
  EXPECT_THROW(DecodeVersionedFrame(spec, corrupt_length), std::runtime_error);
  auto corrupt_payload = encoded;
  corrupt_payload[12] ^= std::byte{1};
  EXPECT_THROW(DecodeVersionedFrame(spec, corrupt_payload), std::runtime_error);
  auto corrupt_crc = encoded;
  corrupt_crc.back() ^= std::byte{1};
  EXPECT_THROW(DecodeVersionedFrame(spec, corrupt_crc), std::runtime_error);
}

TEST(VersionedFrameTest, ChecksummedBodyPreservesLegacyLayout) {
  constexpr std::array<std::byte, 4> magic{std::byte{'B'}, std::byte{'O'}, std::byte{'D'}, std::byte{'Y'}};
  const std::vector<std::byte> body{std::byte{4}, std::byte{5}};
  auto frame = EncodeChecksummedFrame(magic.data(), magic.size(), body, 8, "body frame");
  const auto golden = Bytes({0x42, 0x4f, 0x44, 0x59, 0x04, 0x05, 0x8a, 0x1a, 0x02, 0x12});
  EXPECT_EQ(frame, golden);
  EXPECT_EQ(DecodeChecksummedFrame(magic.data(), magic.size(), golden, 8, "body frame"), body);
  frame.back() ^= std::byte{1};
  EXPECT_THROW(DecodeChecksummedFrame(magic.data(), magic.size(), frame, 8, "body frame"), std::runtime_error);
}

}  // namespace bustub
