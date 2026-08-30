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

auto SessionFrame(const std::vector<std::byte> &payload) -> std::vector<std::byte> {
  constexpr std::array<std::byte, 8> magic{std::byte{'B'}, std::byte{'S'}, std::byte{'T'}, std::byte{'S'},
                                           std::byte{'E'}, std::byte{'S'}, std::byte{'0'}, std::byte{'1'}};
  return EncodeVersionedFrame({magic.data(), magic.size(), 1, 64U * 1024U * 1024U, "test session snapshot"}, payload);
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

// M2 exact-once transition semantics.
TEST(SessionTableTest, SequenceGapTooOldAndExactRetryAreDistinct) {
  SessionTable sessions;
  EXPECT_EQ(sessions.Classify(41, 0), RequestDisposition::TOO_OLD);
  EXPECT_EQ(sessions.Classify(41, 1), RequestDisposition::NEW_REQUEST);
  EXPECT_EQ(sessions.Classify(41, 2), RequestDisposition::GAP);

  const auto first = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 1, 7, 10});
  sessions.RecordCommitted(41, 1, first);
  EXPECT_EQ(sessions.Classify(41, 1), RequestDisposition::RETRY_LAST);
  EXPECT_EQ(sessions.Classify(41, 0), RequestDisposition::TOO_OLD);
  EXPECT_EQ(sessions.Classify(41, 2), RequestDisposition::NEW_REQUEST);
  EXPECT_EQ(sessions.Classify(41, 3), RequestDisposition::GAP);
  EXPECT_EQ(sessions.GetLastResponse(41), first);

  EXPECT_NO_THROW(sessions.RecordCommitted(41, 1, first));
  const auto different = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 1, 8, 11});
  EXPECT_THROW(sessions.RecordCommitted(41, 1, different), std::runtime_error);
  EXPECT_THROW(sessions.RecordCommitted(41, 3, WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 3, 8, 12})),
               std::runtime_error);
}

TEST(SessionTableTest, RejectedFirstRequestGapHasNoSessionSideEffect) {
  SessionTable sessions;
  const auto request_two = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 2, 3, 9});
  EXPECT_THROW(sessions.RecordCommitted(77, 2, request_two), std::runtime_error);
  EXPECT_TRUE(sessions.SnapshotRecords().empty());
  EXPECT_FALSE(sessions.GetLastResponse(77).has_value());
  EXPECT_EQ(sessions.Classify(77, 1), RequestDisposition::NEW_REQUEST);

  const auto mismatched = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 8, 3, 10});
  EXPECT_THROW(sessions.RecordCommitted(77, 1, mismatched), std::runtime_error);
  EXPECT_THROW(sessions.RecordCommitted(0, 1, WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 1, 3, 10})),
               std::runtime_error);
  EXPECT_TRUE(sessions.SnapshotRecords().empty());
}

TEST(SessionTableTest, SnapshotBoundaryRejectsFutureCommittedResponse) {
  SessionTable sessions;
  const auto response = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 1, 6, 7});
  sessions.RestoreRecords({{77, SessionRecord{1, response}}});

  EXPECT_NO_THROW(sessions.ValidateSnapshotBoundary(7));
  EXPECT_NO_THROW(sessions.ValidateSnapshotBoundary(9));
  EXPECT_THROW(sessions.ValidateSnapshotBoundary(6), std::runtime_error);
  EXPECT_THROW(sessions.ValidateSnapshotBoundary(0), std::runtime_error);

  const auto record = sessions.GetLastResponse(77);
  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(*record, response);
}

// M0 persistence gate: the frame is fixed without calling the M2 transition API.
TEST(SessionTableTest, SnapshotMatchesGoldenFrameAndRestoresIndependentClientRecords) {
  const auto response_ten =
      Bytes({0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04});
  const auto response_twenty =
      Bytes({0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b});
  const auto golden = Bytes({
      0x42, 0x53, 0x54, 0x53, 0x45, 0x53, 0x30, 0x31, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x6c, 0x00, 0x00, 0x00,
      0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
      0x00, 0x20, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00,
      0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0x7e, 0xc8, 0xf3, 0x2d,
  });

  SessionTable source;
  source.RestoreRecords({{10, SessionRecord{1, response_ten}}, {20, SessionRecord{3, response_twenty}}});
  EXPECT_EQ(SessionSnapshotCodec::Encode(source), golden);

  SessionTable restored;
  SessionSnapshotCodec::DecodeInto(golden, &restored);
  const auto records = restored.SnapshotRecords();
  ASSERT_EQ(records.size(), 2);
  ASSERT_NE(records.find(10), records.end());
  ASSERT_NE(records.find(20), records.end());
  EXPECT_EQ(records.at(10).last_request_id_, 1);
  EXPECT_EQ(records.at(10).encoded_response_, response_ten);
  EXPECT_EQ(records.at(20).last_request_id_, 3);
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
}

// M2 cumulative gate: persisted M0 records feed the exact-once classifier after recovery.
TEST(SessionTableTest, RestoredRecordsDriveExactOnceClassification) {
  const auto response_ten = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 1, 2, 4});
  const auto response_twenty = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 3, 7, 11});
  SessionTable restored;
  restored.RestoreRecords({{10, SessionRecord{1, response_ten}}, {20, SessionRecord{3, response_twenty}}});

  EXPECT_EQ(restored.Classify(10, 1), RequestDisposition::RETRY_LAST);
  EXPECT_EQ(restored.Classify(10, 2), RequestDisposition::NEW_REQUEST);
  EXPECT_EQ(restored.Classify(10, 3), RequestDisposition::GAP);
  EXPECT_EQ(restored.Classify(20, 2), RequestDisposition::TOO_OLD);
  EXPECT_EQ(restored.Classify(20, 3), RequestDisposition::RETRY_LAST);
  EXPECT_EQ(restored.Classify(20, 4), RequestDisposition::NEW_REQUEST);
  EXPECT_EQ(restored.Classify(20, 5), RequestDisposition::GAP);
}

TEST(SessionTableTest, MalformedSnapshotIsRejectedWithoutReplacingLiveSessions) {
  const auto response = WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, 1, 2, 4});
  SessionTable target;
  target.RestoreRecords({{99, SessionRecord{1, response}}});

  ByteWriter duplicate;
  duplicate.PutU32(2);
  for (size_t occurrence = 0; occurrence < 2; occurrence++) {
    duplicate.PutU64(10);
    duplicate.PutU64(1);
    duplicate.PutU32(static_cast<uint32_t>(response.size()));
    duplicate.PutBytes(response);
  }
  EXPECT_THROW(SessionSnapshotCodec::DecodeInto(SessionFrame(duplicate.Data()), &target), std::runtime_error);

  ByteWriter mismatched;
  mismatched.PutU32(1);
  mismatched.PutU64(10);
  mismatched.PutU64(2);
  mismatched.PutU32(static_cast<uint32_t>(response.size()));
  mismatched.PutBytes(response);
  EXPECT_THROW(SessionSnapshotCodec::DecodeInto(SessionFrame(mismatched.Data()), &target), std::runtime_error);

  auto truncated = SessionFrame(duplicate.Data());
  truncated.pop_back();
  EXPECT_THROW(SessionSnapshotCodec::DecodeInto(truncated, &target), std::runtime_error);
  EXPECT_THROW(SessionSnapshotCodec::DecodeInto(SessionFrame(duplicate.Data()), nullptr), std::runtime_error);

  const auto records = target.SnapshotRecords();
  ASSERT_EQ(records.size(), 1);
  ASSERT_NE(records.find(99), records.end());
  EXPECT_EQ(records.at(99).last_request_id_, 1);
  EXPECT_EQ(records.at(99).encoded_response_, response);
}

}  // namespace bustub
