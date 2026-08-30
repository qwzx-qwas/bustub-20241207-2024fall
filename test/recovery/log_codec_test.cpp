//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_codec_test.cpp
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <cstdint>
#include <vector>

#include "gtest/gtest.h"
#include "recovery/log_codec.h"

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

// M2-T01: the documented big-endian frame and CRC-32C remain stable independently of the decoder.
TEST(LogCodecTest, PayloadEntryMatchesGoldenFrameAndRejectsCorruption) {
  const auto golden = Bytes({0x42, 0x52, 0x4c, 0x47, 0x00, 0x00, 0x00, 0x23, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
                             0x00, 0x00, 0x00, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00,
                             0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x7f, 0xff, 0xd1, 0xb3, 0x47, 0x3f});
  EXPECT_EQ(LogCodec::Encode({1, 42, 7, EntryType::COMMAND_BATCH, {std::byte{0x00}, std::byte{0x7f}, std::byte{0xff}}}),
            golden);

  const auto decoded = LogCodec::DecodeOne(golden);
  ASSERT_EQ(decoded.status_, LogDecodeStatus::COMPLETE);
  ASSERT_TRUE(decoded.entry_.has_value());
  EXPECT_EQ(decoded.entry_->format_version_, 1);
  EXPECT_EQ(decoded.entry_->index_, 42);
  EXPECT_EQ(decoded.entry_->term_, 7);
  EXPECT_EQ(decoded.entry_->type_, EntryType::COMMAND_BATCH);
  EXPECT_EQ(decoded.entry_->payload_, Bytes({0x00, 0x7f, 0xff}));
  EXPECT_EQ(decoded.bytes_consumed_, 43);

  auto corrupt = golden;
  corrupt[corrupt.size() - 1] ^= std::byte{1};
  EXPECT_EQ(LogCodec::DecodeOne(corrupt).status_, LogDecodeStatus::CORRUPT);
  EXPECT_EQ(LogCodec::DecodeOne(golden.data(), 3).status_, LogDecodeStatus::TRUNCATED);
  EXPECT_EQ(LogCodec::DecodeOne(golden.data(), golden.size() - 1).status_, LogDecodeStatus::TRUNCATED);
}

// M2-T02: index zero remains a logical sentinel and is never physically encoded.
TEST(LogCodecTest, RejectsPhysicalSentinelAndInvalidNoop) {
  EXPECT_THROW(LogCodec::Encode({1, 0, 0, EntryType::NOOP, {}}), std::runtime_error);
  EXPECT_THROW(LogCodec::Encode({1, 1, 1, EntryType::NOOP, {std::byte{1}}}), std::runtime_error);

  const auto golden = Bytes({0x42, 0x52, 0x4c, 0x47, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
                             0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
                             0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x02, 0x05, 0xd9, 0xbb});
  EXPECT_EQ(LogCodec::Encode({1, 1, 1, EntryType::NOOP, {}}), golden);
  const auto noop = LogCodec::DecodeOne(golden);
  ASSERT_EQ(noop.status_, LogDecodeStatus::COMPLETE);
  ASSERT_TRUE(noop.entry_.has_value());
  EXPECT_EQ(noop.entry_->format_version_, 1);
  EXPECT_EQ(noop.entry_->index_, 1);
  EXPECT_EQ(noop.entry_->term_, 1);
  EXPECT_EQ(noop.entry_->type_, EntryType::NOOP);
  EXPECT_TRUE(noop.entry_->payload_.empty());
  EXPECT_EQ(noop.bytes_consumed_, 40);
}

}  // namespace bustub
