//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// rpc_codec_test.cpp
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <initializer_list>
#include <stdexcept>
#include <string_view>
#include <variant>
#include <vector>

#include "gtest/gtest.h"
#include "raft/rpc_codec.h"

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

auto Hex(std::string_view value) -> std::vector<std::byte> {
  if (value.size() % 2 != 0) {
    throw std::runtime_error("hex fixture has an odd number of digits");
  }
  const auto digit = [](char character) -> uint8_t {
    if (character >= '0' && character <= '9') {
      return static_cast<uint8_t>(character - '0');
    }
    if (character >= 'a' && character <= 'f') {
      return static_cast<uint8_t>(character - 'a' + 10);
    }
    throw std::runtime_error("hex fixture contains a non-lowercase-hex digit");
  };
  std::vector<std::byte> result;
  result.reserve(value.size() / 2);
  for (size_t offset = 0; offset < value.size(); offset += 2) {
    result.push_back(static_cast<std::byte>((digit(value[offset]) << 4U) | digit(value[offset + 1])));
  }
  return result;
}

void ExpectGoldenEnvelope(const RaftEnvelope &envelope) {
  EXPECT_EQ(envelope.from_, 0x0102030405060708ULL);
  EXPECT_EQ(envelope.to_, 0x1112131415161718ULL);
  EXPECT_EQ(envelope.group_id_, "g");
}

}  // namespace

TEST(RaftRpcCodecTest, RemainingV1MessageKindsMatchFixedGoldenFramesAndLiteralFields) {
  // These fixtures were assembled from the V1 field table. Expected bytes do not call the production encoder,
  // nested LogCodec, or checksum implementation at test runtime.
  {
    const auto golden =
        Hex("425241465430303100000001000000260102030405060708111213141516171800000001670000"
            "000200000009212223242526272801c6d692ef");
    const RaftEnvelope expected{0x0102030405060708ULL, 0x1112131415161718ULL,
                                RequestVoteResponse{0x2122232425262728ULL, true}, "g"};
    EXPECT_EQ(RaftRpcCodec::Encode(expected), golden);
    const auto decoded = RaftRpcCodec::Decode(golden);
    ExpectGoldenEnvelope(decoded);
    ASSERT_TRUE(std::holds_alternative<RequestVoteResponse>(decoded.message_));
    const auto &vote = std::get<RequestVoteResponse>(decoded.message_);
    EXPECT_EQ(vote.term_, 0x2122232425262728ULL);
    EXPECT_TRUE(vote.vote_granted_);
  }
  {
    const auto golden =
        Hex("425241465430303100000001000000860102030405060708111213141516171800000001670000000300000069"
            "21222324252627283132333435363738414243444546474851525354555657586162636465666768717273747576"
            "7778018182838485868788000000010000002842524c4700000020000000019192939495969798a1a2a3a4a5a6"
            "a7a800000002000000008d40560d653739ae");
    const RaftEnvelope expected{
        0x0102030405060708ULL, 0x1112131415161718ULL,
        AppendEntriesRequest{0x2122232425262728ULL,
                             0x3132333435363738ULL,
                             0x4142434445464748ULL,
                             0x5152535455565758ULL,
                             0x6162636465666768ULL,
                             {{1, 0x9192939495969798ULL, 0xa1a2a3a4a5a6a7a8ULL, EntryType::NOOP, {}}},
                             0x7172737475767778ULL,
                             0x8182838485868788ULL},
        "g"};
    EXPECT_EQ(RaftRpcCodec::Encode(expected), golden);
    const auto decoded = RaftRpcCodec::Decode(golden);
    ExpectGoldenEnvelope(decoded);
    ASSERT_TRUE(std::holds_alternative<AppendEntriesRequest>(decoded.message_));
    const auto &append = std::get<AppendEntriesRequest>(decoded.message_);
    EXPECT_EQ(append.term_, 0x2122232425262728ULL);
    EXPECT_EQ(append.leader_id_, 0x3132333435363738ULL);
    EXPECT_EQ(append.request_id_, 0x4142434445464748ULL);
    EXPECT_EQ(append.prev_log_index_, 0x5152535455565758ULL);
    EXPECT_EQ(append.prev_log_term_, 0x6162636465666768ULL);
    ASSERT_EQ(append.entries_.size(), 1);
    EXPECT_EQ(append.entries_[0],
              (ReplicatedLogEntry{1, 0x9192939495969798ULL, 0xa1a2a3a4a5a6a7a8ULL, EntryType::NOOP, {}}));
    EXPECT_EQ(append.leader_commit_, 0x7172737475767778ULL);
    EXPECT_EQ(append.read_context_, 0x8182838485868788ULL);
  }
  {
    const auto golden =
        Hex("425241465430303100000001000000500102030405060708111213141516171800000001670000"
            "000400000033212223242526272831323334353637380041424344454647480151525354555657"
            "58616263646566676801717273747576777801f88792");
    const RaftEnvelope expected{
        0x0102030405060708ULL, 0x1112131415161718ULL,
        AppendEntriesResponse{0x2122232425262728ULL, 0x3132333435363738ULL, false, 0x4142434445464748ULL,
                              0x5152535455565758ULL, 0x6162636465666768ULL, 0x7172737475767778ULL},
        "g"};
    EXPECT_EQ(RaftRpcCodec::Encode(expected), golden);
    const auto decoded = RaftRpcCodec::Decode(golden);
    ExpectGoldenEnvelope(decoded);
    ASSERT_TRUE(std::holds_alternative<AppendEntriesResponse>(decoded.message_));
    const auto &append = std::get<AppendEntriesResponse>(decoded.message_);
    EXPECT_EQ(append.term_, 0x2122232425262728ULL);
    EXPECT_EQ(append.request_id_, 0x3132333435363738ULL);
    EXPECT_FALSE(append.success_);
    EXPECT_EQ(append.match_index_, 0x4142434445464748ULL);
    EXPECT_EQ(append.conflict_term_, 0x5152535455565758ULL);
    EXPECT_EQ(append.conflict_index_, 0x6162636465666768ULL);
    EXPECT_EQ(append.read_context_, 0x7172737475767778ULL);
  }
  {
    const auto golden =
        Hex("42524146543030310000000100000069010203040506070811121314151617180000000167000000050000004c"
            "21222324252627283132333435363738414243444546474800000004736e61705152535455565758616263646566"
            "676800000000000000020000000000000005a1b2c3d4010000000378797a7bee2013");
    const RaftEnvelope expected{0x0102030405060708ULL, 0x1112131415161718ULL,
                                InstallSnapshotRequest{0x2122232425262728ULL,
                                                       0x3132333435363738ULL,
                                                       0x4142434445464748ULL,
                                                       "snap",
                                                       0x5152535455565758ULL,
                                                       0x6162636465666768ULL,
                                                       2,
                                                       5,
                                                       0xa1b2c3d4U,
                                                       true,
                                                       {std::byte{'x'}, std::byte{'y'}, std::byte{'z'}}},
                                "g"};
    EXPECT_EQ(RaftRpcCodec::Encode(expected), golden);
    const auto decoded = RaftRpcCodec::Decode(golden);
    ExpectGoldenEnvelope(decoded);
    ASSERT_TRUE(std::holds_alternative<InstallSnapshotRequest>(decoded.message_));
    const auto &snapshot = std::get<InstallSnapshotRequest>(decoded.message_);
    EXPECT_EQ(snapshot.term_, 0x2122232425262728ULL);
    EXPECT_EQ(snapshot.leader_id_, 0x3132333435363738ULL);
    EXPECT_EQ(snapshot.request_id_, 0x4142434445464748ULL);
    EXPECT_EQ(snapshot.snapshot_id_, "snap");
    EXPECT_EQ(snapshot.last_included_index_, 0x5152535455565758ULL);
    EXPECT_EQ(snapshot.last_included_term_, 0x6162636465666768ULL);
    EXPECT_EQ(snapshot.offset_, 2);
    EXPECT_EQ(snapshot.total_size_, 5);
    EXPECT_EQ(snapshot.payload_checksum_, 0xa1b2c3d4U);
    EXPECT_TRUE(snapshot.done_);
    EXPECT_EQ(snapshot.data_, Bytes({'x', 'y', 'z'}));
  }
  {
    const auto golden =
        Hex("425241465430303100000001000000400102030405060708111213141516171800000001670000"
            "000600000023212223242526272831323334353637380101004142434445464748515253545556"
            "5758bfee51dc");
    const RaftEnvelope expected{0x0102030405060708ULL, 0x1112131415161718ULL,
                                InstallSnapshotResponse{0x2122232425262728ULL, 0x3132333435363738ULL, true, true, false,
                                                        0x4142434445464748ULL, 0x5152535455565758ULL},
                                "g"};
    EXPECT_EQ(RaftRpcCodec::Encode(expected), golden);
    const auto decoded = RaftRpcCodec::Decode(golden);
    ExpectGoldenEnvelope(decoded);
    ASSERT_TRUE(std::holds_alternative<InstallSnapshotResponse>(decoded.message_));
    const auto &snapshot = std::get<InstallSnapshotResponse>(decoded.message_);
    EXPECT_EQ(snapshot.term_, 0x2122232425262728ULL);
    EXPECT_EQ(snapshot.request_id_, 0x3132333435363738ULL);
    EXPECT_TRUE(snapshot.success_);
    EXPECT_TRUE(snapshot.stale_);
    EXPECT_FALSE(snapshot.complete_);
    EXPECT_EQ(snapshot.match_index_, 0x4142434445464748ULL);
    EXPECT_EQ(snapshot.next_offset_, 0x5152535455565758ULL);
  }
}

TEST(RaftRpcCodecTest, RequestVoteV1MatchesDocumentedGoldenBytes) {
  // Hand-assembled from the V1 protocol: envelope endpoints/group, type=1, a 32-byte RequestVote body, then CRC-32C.
  const auto golden = Bytes({
      0x42, 0x52, 0x41, 0x46, 0x54, 0x30, 0x30, 0x31, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x3d, 0x01,
      0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x00, 0x00,
      0x00, 0x01, 0x67, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26,
      0x27, 0x28, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47,
      0x48, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x80, 0x20, 0xff, 0x7d,
  });
  const RaftEnvelope expected{
      0x0102030405060708ULL, 0x1112131415161718ULL,
      RequestVoteRequest{0x2122232425262728ULL, 0x3132333435363738ULL, 0x4142434445464748ULL, 0x5152535455565758ULL},
      "g"};
  EXPECT_EQ(RaftRpcCodec::Encode(expected), golden);

  const auto decoded = RaftRpcCodec::Decode(golden);
  EXPECT_EQ(decoded.from_, 0x0102030405060708ULL);
  EXPECT_EQ(decoded.to_, 0x1112131415161718ULL);
  EXPECT_EQ(decoded.group_id_, "g");
  ASSERT_TRUE(std::holds_alternative<RequestVoteRequest>(decoded.message_));
  const auto &vote = std::get<RequestVoteRequest>(decoded.message_);
  EXPECT_EQ(vote.term_, 0x2122232425262728ULL);
  EXPECT_EQ(vote.candidate_id_, 0x3132333435363738ULL);
  EXPECT_EQ(vote.last_log_index_, 0x4142434445464748ULL);
  EXPECT_EQ(vote.last_log_term_, 0x5152535455565758ULL);
}

TEST(RaftRpcCodecTest, RejectsCorruptionTruncationAndInvalidSnapshotChunk) {
  auto frame = RaftRpcCodec::Encode({1, 2, RequestVoteResponse{3, false}, "demo"});
  auto corrupt = frame;
  corrupt.back() ^= std::byte{1};
  EXPECT_THROW(RaftRpcCodec::Decode(corrupt), std::runtime_error);
  frame.pop_back();
  EXPECT_THROW(RaftRpcCodec::Decode(frame), std::runtime_error);
  EXPECT_THROW(
      RaftRpcCodec::Encode({1, 2, InstallSnapshotRequest{3, 1, 1, "s", 8, 3, 0, 4, 0, true, {std::byte{1}}}, "demo"}),
      std::runtime_error);
  EXPECT_THROW(RaftRpcCodec::Encode({1, 2, RequestVoteResponse{3, false}, ""}), std::runtime_error);
}

}  // namespace bustub
