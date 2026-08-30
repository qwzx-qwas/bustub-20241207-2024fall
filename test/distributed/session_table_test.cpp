//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// session_table_test.cpp
//
//===----------------------------------------------------------------------===//

#include <array>
#include <cstdint>
#include <map>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "common/byte_codec.h"
#include "distributed/session_table.h"
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

auto Hex(std::string_view text) -> std::vector<std::byte> {
  if (text.size() % 2 != 0) {
    throw std::runtime_error("invalid test hex literal");
  }
  const auto nibble = [](char value) -> uint8_t {
    if (value >= '0' && value <= '9') {
      return static_cast<uint8_t>(value - '0');
    }
    if (value >= 'a' && value <= 'f') {
      return static_cast<uint8_t>(value - 'a' + 10);
    }
    throw std::runtime_error("invalid test hex digit");
  };
  std::vector<std::byte> result;
  result.reserve(text.size() / 2);
  for (size_t offset = 0; offset < text.size(); offset += 2) {
    result.push_back(static_cast<std::byte>((nibble(text[offset]) << 4U) | nibble(text[offset + 1])));
  }
  return result;
}

auto LiteralFingerprint(uint8_t first_byte) -> RequestFingerprintV1 {
  std::array<std::byte, RequestFingerprintV1::DIGEST_SIZE> digest{};
  for (size_t index = 0; index < digest.size(); index++) {
    digest[index] = static_cast<std::byte>(first_byte + static_cast<uint8_t>(index));
  }
  return {RequestFingerprintV1::FORMAT_VERSION, digest};
}

void PutFingerprint(ByteWriter *writer, const RequestFingerprintV1 &fingerprint) {
  writer->PutU32(fingerprint.format_version_);
  writer->PutBytes(fingerprint.digest_.data(), fingerprint.digest_.size());
}

void PutU32At(std::vector<std::byte> *bytes, size_t offset, uint32_t value) {
  ASSERT_NE(bytes, nullptr);
  ASSERT_LE(offset + sizeof(uint32_t), bytes->size());
  (*bytes)[offset] = static_cast<std::byte>((value >> 24U) & 0xffU);
  (*bytes)[offset + 1] = static_cast<std::byte>((value >> 16U) & 0xffU);
  (*bytes)[offset + 2] = static_cast<std::byte>((value >> 8U) & 0xffU);
  (*bytes)[offset + 3] = static_cast<std::byte>(value & 0xffU);
}

auto SessionFrame(const std::vector<std::byte> &payload) -> std::vector<std::byte> {
  constexpr std::array<std::byte, 8> magic{std::byte{'B'}, std::byte{'S'}, std::byte{'T'}, std::byte{'S'},
                                           std::byte{'E'}, std::byte{'S'}, std::byte{'0'}, std::byte{'1'}};
  return EncodeVersionedFrame(
      {magic.data(), magic.size(), SessionSnapshotCodec::FORMAT_VERSION, 64U * 1024U * 1024U, "test session snapshot"},
      payload);
}

}  // namespace

// M0 fixed response framing.
TEST(SessionTableTest, WriteResponseHasStableLiteralLayout) {
  const auto expected =
      Bytes({0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0d});
  EXPECT_EQ(WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 7, 11, 13}), expected);
  EXPECT_EQ(WriteResponseCodec::Decode(expected), (WriteResponseV1{1, WriteStatus::COMMITTED, 7, 11, 13}));
}

// M8 exact-once transition semantics bind the retained request identity to its exact payload fingerprint.
TEST(SessionTableTest, SequenceGapTooOldExactRetryAndPayloadMismatchAreDistinct) {
  SessionTable sessions;
  const auto original_fingerprint = LiteralFingerprint(0x10);
  const auto changed_fingerprint = LiteralFingerprint(0x80);
  EXPECT_EQ(sessions.Classify(41, 0, original_fingerprint), RequestDisposition::TOO_OLD);
  EXPECT_EQ(sessions.Classify(41, 1, original_fingerprint), RequestDisposition::NEW_REQUEST);
  EXPECT_EQ(sessions.Classify(41, 2, changed_fingerprint), RequestDisposition::GAP);

  const auto first = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 1, 7, 10});
  sessions.RecordCommitted(41, 1, original_fingerprint, first);
  EXPECT_EQ(sessions.Classify(41, 1, original_fingerprint), RequestDisposition::RETRY_LAST);
  EXPECT_EQ(sessions.Classify(41, 1, changed_fingerprint), RequestDisposition::PAYLOAD_MISMATCH);
  EXPECT_EQ(sessions.Classify(41, 0, changed_fingerprint), RequestDisposition::TOO_OLD);
  EXPECT_EQ(sessions.Classify(41, 2, changed_fingerprint), RequestDisposition::NEW_REQUEST);
  EXPECT_EQ(sessions.Classify(41, 3, changed_fingerprint), RequestDisposition::GAP);
  EXPECT_EQ(sessions.GetLastResponse(41), first);

  EXPECT_NO_THROW(sessions.RecordCommitted(41, 1, original_fingerprint, first));
  const auto committed_record = sessions.SnapshotRecords();
  EXPECT_THROW(sessions.RecordCommitted(41, 1, changed_fingerprint, first), std::runtime_error);
  EXPECT_EQ(sessions.SnapshotRecords(), committed_record);
  const auto different = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 1, 8, 11});
  EXPECT_THROW(sessions.RecordCommitted(41, 1, original_fingerprint, different), std::runtime_error);
  EXPECT_EQ(sessions.SnapshotRecords(), committed_record);
  EXPECT_THROW(sessions.RecordCommitted(41, 3, changed_fingerprint,
                                        WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 3, 8, 12})),
               std::runtime_error);
  EXPECT_EQ(sessions.SnapshotRecords(), committed_record);
}

TEST(SessionTableTest, RejectedFirstRequestGapHasNoSessionSideEffect) {
  SessionTable sessions;
  const auto fingerprint = LiteralFingerprint(0x20);
  const auto request_two = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 2, 3, 9});
  EXPECT_THROW(sessions.RecordCommitted(77, 2, fingerprint, request_two), std::runtime_error);
  EXPECT_TRUE(sessions.SnapshotRecords().empty());
  EXPECT_FALSE(sessions.GetLastResponse(77).has_value());
  EXPECT_EQ(sessions.Classify(77, 1, fingerprint), RequestDisposition::NEW_REQUEST);

  const auto mismatched = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 8, 3, 10});
  EXPECT_THROW(sessions.RecordCommitted(77, 1, fingerprint, mismatched), std::runtime_error);
  EXPECT_THROW(
      sessions.RecordCommitted(0, 1, fingerprint, WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 1, 3, 10})),
      std::runtime_error);
  EXPECT_TRUE(sessions.SnapshotRecords().empty());
}

TEST(SessionTableTest, SnapshotBoundaryRejectsFutureCommittedResponse) {
  SessionTable sessions;
  const auto fingerprint = LiteralFingerprint(0x30);
  const auto response = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 1, 6, 7});
  sessions.RestoreRecords({{77, SessionRecord{1, fingerprint, response}}});

  EXPECT_NO_THROW(sessions.ValidateSnapshotBoundary(7));
  EXPECT_NO_THROW(sessions.ValidateSnapshotBoundary(9));
  EXPECT_THROW(sessions.ValidateSnapshotBoundary(6), std::runtime_error);
  EXPECT_THROW(sessions.ValidateSnapshotBoundary(0), std::runtime_error);

  const auto record = sessions.GetLastResponse(77);
  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(*record, response);
}

// M8 persistence gate: this non-empty V2 frame is a hand-written oracle. Its two SHA-256 values and CRC32C were
// generated outside the production codec from the literal raw SQL below.
TEST(SessionTableTest, V2SnapshotMatchesLiteralGoldenAndRestoresIndependentClientRecords) {
  constexpr std::string_view sql_ten = "INSERT INTO accounts VALUES (1, 'seed');";
  constexpr std::string_view sql_twenty = "UPDATE accounts SET balance = balance + 7 WHERE id <= 2;";
  const auto response_ten =
      Bytes({0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04});
  const auto response_twenty =
      Bytes({0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b});
  const auto fingerprint_ten = ComputeWriteIntentFingerprintV1(sql_ten);
  const auto fingerprint_twenty = ComputeWriteIntentFingerprintV1(sql_twenty);
  const auto golden =
      Hex("425354534553303100000002000000b400000002000000000000000a00000000000000010000000140aa0f142a34738f"
          "fc7e8e07980f969286e99d9cbdac60463e1324eee20dcea2000000200000000100000001000000000000000100000000"
          "000000020000000000000004000000000000001400000000000000030000000113dced9b6fa59298bb0a338033f3b09e"
          "d42c42e91bb6be2119b3f733932925c30000002000000001000000010000000000000003000000000000000700000000"
          "0000000b41f66aff");

  SessionTable source;
  source.RestoreRecords({{10, SessionRecord{1, fingerprint_ten, response_ten}},
                         {20, SessionRecord{3, fingerprint_twenty, response_twenty}}});
  EXPECT_EQ(SessionSnapshotCodec::Encode(source), golden);

  SessionTable restored;
  SessionSnapshotCodec::DecodeInto(golden, &restored);
  const auto records = restored.SnapshotRecords();
  ASSERT_EQ(records.size(), 2);
  ASSERT_NE(records.find(10), records.end());
  ASSERT_NE(records.find(20), records.end());
  EXPECT_EQ(records.at(10).last_request_id_, 1);
  EXPECT_EQ(records.at(10).request_fingerprint_, fingerprint_ten);
  EXPECT_EQ(records.at(10).encoded_response_, response_ten);
  EXPECT_EQ(records.at(20).last_request_id_, 3);
  EXPECT_EQ(records.at(20).request_fingerprint_, fingerprint_twenty);
  EXPECT_EQ(records.at(20).encoded_response_, response_twenty);

  const auto decoded_ten = WriteResponseCodec::Decode(records.at(10).encoded_response_);
  EXPECT_EQ(decoded_ten.format_version_, 1);
  EXPECT_EQ(decoded_ten.status_, WriteStatus::COMMITTED);
  EXPECT_EQ(decoded_ten.request_id_, 1);
  EXPECT_EQ(decoded_ten.term_, 2);
  EXPECT_EQ(decoded_ten.commit_index_, 4);
  const auto decoded_twenty = WriteResponseCodec::Decode(records.at(20).encoded_response_);
  EXPECT_EQ(decoded_twenty.format_version_, 1);
  EXPECT_EQ(decoded_twenty.status_, WriteStatus::COMMITTED);
  EXPECT_EQ(decoded_twenty.request_id_, 3);
  EXPECT_EQ(decoded_twenty.term_, 7);
  EXPECT_EQ(decoded_twenty.commit_index_, 11);

  EXPECT_EQ(restored.GetLastResponse(10), response_ten);
  EXPECT_EQ(restored.GetLastResponse(20), response_twenty);
  EXPECT_EQ(restored.Classify(10, 1, ComputeWriteIntentFingerprintV1(sql_ten)), RequestDisposition::RETRY_LAST);
  EXPECT_EQ(restored.Classify(10, 1, ComputeWriteIntentFingerprintV1("INSERT INTO accounts VALUES (1, 'changed');")),
            RequestDisposition::PAYLOAD_MISMATCH);
  EXPECT_EQ(restored.Classify(20, 3, ComputeWriteIntentFingerprintV1(sql_twenty)), RequestDisposition::RETRY_LAST);
}

// M2 cumulative gate: persisted M0 records feed the exact-once classifier after recovery.
TEST(SessionTableTest, RestoredRecordsDriveExactOnceClassification) {
  const auto response_ten = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 1, 2, 4});
  const auto response_twenty = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 3, 7, 11});
  const auto fingerprint_ten = LiteralFingerprint(0x00);
  const auto changed_ten = LiteralFingerprint(0x40);
  const auto fingerprint_twenty = LiteralFingerprint(0x80);
  const auto changed_twenty = LiteralFingerprint(0xc0);
  SessionTable restored;
  restored.RestoreRecords({{10, SessionRecord{1, fingerprint_ten, response_ten}},
                           {20, SessionRecord{3, fingerprint_twenty, response_twenty}}});

  EXPECT_EQ(restored.Classify(10, 1, fingerprint_ten), RequestDisposition::RETRY_LAST);
  EXPECT_EQ(restored.Classify(10, 1, changed_ten), RequestDisposition::PAYLOAD_MISMATCH);
  EXPECT_EQ(restored.Classify(10, 2, changed_ten), RequestDisposition::NEW_REQUEST);
  EXPECT_EQ(restored.Classify(10, 3, changed_ten), RequestDisposition::GAP);
  EXPECT_EQ(restored.Classify(20, 2, changed_twenty), RequestDisposition::TOO_OLD);
  EXPECT_EQ(restored.Classify(20, 3, fingerprint_twenty), RequestDisposition::RETRY_LAST);
  EXPECT_EQ(restored.Classify(20, 3, changed_twenty), RequestDisposition::PAYLOAD_MISMATCH);
  EXPECT_EQ(restored.Classify(20, 4, changed_twenty), RequestDisposition::NEW_REQUEST);
  EXPECT_EQ(restored.Classify(20, 5, changed_twenty), RequestDisposition::GAP);
}

TEST(SessionTableTest, V2MalformedAndUnsupportedSnapshotsAreRejectedAtomically) {
  const auto live_fingerprint = LiteralFingerprint(0x20);
  const auto response = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 1, 2, 4});
  SessionTable target;
  target.RestoreRecords({{99, SessionRecord{1, live_fingerprint, response}}});
  const auto live_records = target.SnapshotRecords();
  const auto expect_rejected_without_replacement = [&](const std::vector<std::byte> &frame) {
    EXPECT_THROW(SessionSnapshotCodec::DecodeInto(frame, &target), std::runtime_error);
    EXPECT_EQ(target.SnapshotRecords(), live_records);
  };

  const auto candidate_fingerprint = LiteralFingerprint(0x80);
  ByteWriter valid_payload;
  valid_payload.PutU32(1);
  valid_payload.PutU64(10);
  valid_payload.PutU64(1);
  PutFingerprint(&valid_payload, candidate_fingerprint);
  valid_payload.PutU32(static_cast<uint32_t>(response.size()));
  valid_payload.PutBytes(response);
  const auto valid_v2 = SessionFrame(valid_payload.Data());

  auto old_v1_outer = valid_v2;
  PutU32At(&old_v1_outer, 8, 1);
  expect_rejected_without_replacement(old_v1_outer);

  auto unknown_v3_outer = valid_v2;
  PutU32At(&unknown_v3_outer, 8, 3);
  expect_rejected_without_replacement(unknown_v3_outer);

  auto corrupt_crc = valid_v2;
  corrupt_crc.back() ^= std::byte{1};
  expect_rejected_without_replacement(corrupt_crc);

  auto truncated = valid_v2;
  truncated.pop_back();
  expect_rejected_without_replacement(truncated);

  ByteWriter duplicate;
  duplicate.PutU32(2);
  for (size_t occurrence = 0; occurrence < 2; occurrence++) {
    duplicate.PutU64(10);
    duplicate.PutU64(1);
    PutFingerprint(&duplicate, candidate_fingerprint);
    duplicate.PutU32(static_cast<uint32_t>(response.size()));
    duplicate.PutBytes(response);
  }
  expect_rejected_without_replacement(SessionFrame(duplicate.Data()));

  ByteWriter mismatched;
  mismatched.PutU32(1);
  mismatched.PutU64(10);
  mismatched.PutU64(2);
  PutFingerprint(&mismatched, candidate_fingerprint);
  mismatched.PutU32(static_cast<uint32_t>(response.size()));
  mismatched.PutBytes(response);
  expect_rejected_without_replacement(SessionFrame(mismatched.Data()));

  ByteWriter unknown_fingerprint;
  unknown_fingerprint.PutU32(1);
  unknown_fingerprint.PutU64(10);
  unknown_fingerprint.PutU64(1);
  auto unsupported = candidate_fingerprint;
  unsupported.format_version_ = 2;
  PutFingerprint(&unknown_fingerprint, unsupported);
  unknown_fingerprint.PutU32(static_cast<uint32_t>(response.size()));
  unknown_fingerprint.PutBytes(response);
  expect_rejected_without_replacement(SessionFrame(unknown_fingerprint.Data()));

  EXPECT_THROW(SessionSnapshotCodec::DecodeInto(valid_v2, nullptr), std::runtime_error);
  EXPECT_EQ(target.SnapshotRecords(), live_records);
}

}  // namespace bustub
