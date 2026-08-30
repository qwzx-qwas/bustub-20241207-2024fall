//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// command_codec_test.cpp
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "common/byte_codec.h"
#include "distributed/command.h"
#include "gtest/gtest.h"
#include "recovery/log_codec.h"
#include "type/value_factory.h"

namespace bustub {
namespace {

auto Bytes(std::initializer_list<uint8_t> values) -> std::vector<std::byte> {
  std::vector<std::byte> result;
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

auto LiteralInsert(table_oid_t table_oid, std::string_view key_hex, std::string_view tuple_hex) -> ReplicatedCommand {
  return InsertRowCommand{table_oid, EncodedPrimaryKeyV1{1, TypeId::INTEGER, Hex(key_hex)}, Hex(tuple_hex)};
}

auto Insert(table_oid_t table_oid, int32_t key, std::string value) -> ReplicatedCommand {
  Schema schema({Column("id", TypeId::INTEGER), Column("value", TypeId::VARCHAR, 64)});
  Tuple tuple({ValueFactory::GetIntegerValue(key), ValueFactory::GetVarcharValue(value)}, &schema);
  return InsertRowCommand{table_oid, PrimaryKeyCodecV1::Encode(ValueFactory::GetIntegerValue(key)),
                          TupleCodecV1::Encode(tuple, schema)};
}

auto LiteralFingerprint(uint8_t first_byte = 0) -> RequestFingerprintV1 {
  std::array<std::byte, RequestFingerprintV1::DIGEST_SIZE> digest{};
  for (size_t index = 0; index < digest.size(); index++) {
    digest[index] = static_cast<std::byte>(first_byte + static_cast<uint8_t>(index));
  }
  return {RequestFingerprintV1::FORMAT_VERSION, digest};
}

void PutU32At(std::vector<std::byte> *bytes, size_t offset, uint32_t value) {
  if (bytes == nullptr || offset + sizeof(uint32_t) > bytes->size()) {
    throw std::runtime_error("invalid test frame offset");
  }
  (*bytes)[offset] = static_cast<std::byte>((value >> 24U) & 0xffU);
  (*bytes)[offset + 1] = static_cast<std::byte>((value >> 16U) & 0xffU);
  (*bytes)[offset + 2] = static_cast<std::byte>((value >> 8U) & 0xffU);
  (*bytes)[offset + 3] = static_cast<std::byte>(value & 0xffU);
}

auto CanonicalDmlGolden() -> std::vector<std::byte> {
  return Hex(
      "42434d444241543100000002000000f10000000000000009000000000000000100000001000102030405060708090a0b"
      "0c0d0e0f101112131415161718191a1b1c1d1e1f00000000000000040000000300000003000000330000000100000001"
      "0000000400000004ffffffff0000001b00000001000000020000000400ffffffff00000007000000000161000000030000"
      "0033000000010000000100000004000000040000002a0000001b000000010000000200000004000000002a0000000700"
      "00000001620000000300000033000000020000000100000004000000040000002a0000001b000000010000000200000004"
      "000000002a00000007000000000163b336507c");
}

}  // namespace

TEST(PrimaryKeyCodecV1Test, IntegerGoldenBytesAndSignedOrder) {
  const auto minus_one = PrimaryKeyCodecV1::Encode(ValueFactory::GetIntegerValue(-1));
  const auto forty_two = PrimaryKeyCodecV1::Encode(ValueFactory::GetIntegerValue(42));
  const auto minimum = PrimaryKeyCodecV1::EncodeInteger(std::numeric_limits<int32_t>::min());
  const auto maximum = PrimaryKeyCodecV1::Encode(ValueFactory::GetIntegerValue(std::numeric_limits<int32_t>::max()));
  EXPECT_EQ(minus_one.bytes_, Bytes({0xff, 0xff, 0xff, 0xff}));
  EXPECT_EQ(forty_two.bytes_, Bytes({0x00, 0x00, 0x00, 0x2a}));
  EXPECT_EQ(minimum.bytes_, Bytes({0x80, 0x00, 0x00, 0x00}));
  EXPECT_EQ(maximum.bytes_, Bytes({0x7f, 0xff, 0xff, 0xff}));
  EXPECT_LT(PrimaryKeyCodecV1::CanonicalCompare(minimum, minus_one), 0);
  EXPECT_LT(PrimaryKeyCodecV1::CanonicalCompare(minus_one, forty_two), 0);
  EXPECT_LT(PrimaryKeyCodecV1::CanonicalCompare(forty_two, maximum), 0);
  EXPECT_EQ(PrimaryKeyCodecV1::Decode(minus_one).GetAs<int32_t>(), -1);

  const auto big_min = PrimaryKeyCodecV1::EncodeBigInt(std::numeric_limits<int64_t>::min());
  const auto big_max = PrimaryKeyCodecV1::Encode(ValueFactory::GetBigIntValue(std::numeric_limits<int64_t>::max()));
  EXPECT_EQ(big_min.bytes_, Bytes({0x80, 0, 0, 0, 0, 0, 0, 0}));
  EXPECT_EQ(big_max.bytes_, Bytes({0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}));
  EXPECT_LT(PrimaryKeyCodecV1::CanonicalCompare(big_min, big_max), 0);
}

TEST(PrimaryKeyCodecV1Test, VarcharIsRawBinaryIdentity) {
  const auto upper = PrimaryKeyCodecV1::Encode(ValueFactory::GetVarcharValue("ABC"));
  const auto lower = PrimaryKeyCodecV1::Encode(ValueFactory::GetVarcharValue("abc"));
  const auto trailing = PrimaryKeyCodecV1::Encode(ValueFactory::GetVarcharValue("ABC "));
  const auto empty = PrimaryKeyCodecV1::Encode(ValueFactory::GetVarcharValue(""));
  EXPECT_EQ(upper.bytes_, Bytes({0, 0, 0, 3, 'A', 'B', 'C'}));
  EXPECT_EQ(empty.bytes_, Bytes({0, 0, 0, 0}));
  EXPECT_LT(PrimaryKeyCodecV1::CanonicalCompare(upper, trailing), 0);
  EXPECT_LT(PrimaryKeyCodecV1::CanonicalCompare(upper, lower), 0);
  EXPECT_EQ(PrimaryKeyCodecV1::Decode(trailing).ToString(), "ABC ");

  const std::string utf8{"\xE4\xB8\xAD", 3};
  const auto multibyte = PrimaryKeyCodecV1::Encode(ValueFactory::GetVarcharValue(utf8));
  EXPECT_EQ(multibyte.bytes_, Bytes({0, 0, 0, 3, 0xe4, 0xb8, 0xad}));
}

// M8 consumer gate: the non-empty V2 frame and fingerprint bytes are independent of the production encoder/hash.
TEST(CommandBatchCodecTest, V2LiteralConsumerFrameAndMalformedInput) {
  const auto minus_one = LiteralInsert(1, "ffffffff", "00000001000000020000000400ffffffff00000007000000000161");
  const auto table_one_forty_two =
      LiteralInsert(1, "0000002a", "000000010000000200000004000000002a00000007000000000162");
  const auto table_two_forty_two =
      LiteralInsert(2, "0000002a", "000000010000000200000004000000002a00000007000000000163");
  const auto fingerprint = LiteralFingerprint();
  const TransactionCommandBatch literal_consumer_batch{
      2, 9, 1, fingerprint, 4, {minus_one, table_one_forty_two, table_two_forty_two}};
  // Hand-assembled from the V2 field order, literal digest 00..1f, and the three tuple/key encodings below.
  const auto golden = CanonicalDmlGolden();
  EXPECT_EQ(CommandBatchCodec::Encode(literal_consumer_batch), golden);
  const auto decoded = CommandBatchCodec::Decode(golden);
  EXPECT_EQ(decoded.format_version_, 2);
  EXPECT_EQ(decoded.client_id_, 9);
  EXPECT_EQ(decoded.request_id_, 1);
  EXPECT_EQ(decoded.request_fingerprint_, fingerprint);
  EXPECT_EQ(decoded.expected_start_schema_epoch_, 4);
  ASSERT_EQ(decoded.commands_.size(), 3);
  const std::array<table_oid_t, 3> expected_tables{1, 1, 2};
  const std::array<std::vector<std::byte>, 3> expected_keys{Hex("ffffffff"), Hex("0000002a"), Hex("0000002a")};
  const std::array<std::vector<std::byte>, 3> expected_tuples{
      Hex("00000001000000020000000400ffffffff00000007000000000161"),
      Hex("000000010000000200000004000000002a00000007000000000162"),
      Hex("000000010000000200000004000000002a00000007000000000163")};
  for (size_t index = 0; index < decoded.commands_.size(); index++) {
    ASSERT_TRUE(std::holds_alternative<InsertRowCommand>(decoded.commands_[index]));
    const auto &insert = std::get<InsertRowCommand>(decoded.commands_[index]);
    EXPECT_EQ(insert.table_oid_, expected_tables[index]);
    EXPECT_EQ(insert.primary_key_.codec_version_, 1);
    EXPECT_EQ(insert.primary_key_.type_, TypeId::INTEGER);
    EXPECT_EQ(insert.primary_key_.bytes_, expected_keys[index]);
    EXPECT_EQ(insert.complete_tuple_, expected_tuples[index]);
  }

  auto corrupt = golden;
  corrupt.back() ^= std::byte{1};
  EXPECT_THROW(CommandBatchCodec::Decode(corrupt), std::runtime_error);

  // This is the complete, checksum-valid nonempty CommandBatchV1 frame from the pre-M8 contract, not a V2 body with
  // its outer version byte changed. Fresh-directory M8 must reject the actual legacy layout.
  const auto legacy_v1 =
      Hex("42434d444241543100000001000000cd0000000000000009000000000000000100000000000000040000000300000003"
          "0000003300000001000000010000000400000004ffffffff0000001b00000001000000020000000400ffffffff00000007"
          "0000000001610000000300000033000000010000000100000004000000040000002a0000001b0000000100000002000000"
          "04000000002a000000070000000001620000000300000033000000020000000100000004000000040000002a0000001b00"
          "0000010000000200000004000000002a00000007000000000163bd7d94c6");
  EXPECT_THROW(CommandBatchCodec::Decode(legacy_v1), std::runtime_error);

  auto unknown_v3_outer = golden;
  PutU32At(&unknown_v3_outer, 8, 3);
  EXPECT_THROW(CommandBatchCodec::Decode(unknown_v3_outer), std::runtime_error);

  auto truncated = golden;
  truncated.pop_back();
  EXPECT_THROW(CommandBatchCodec::Decode(truncated), std::runtime_error);

  auto unknown_command = golden;
  constexpr size_t frame_header_size = 16;
  constexpr size_t batch_prefix_size =
      sizeof(uint64_t) * 2 + RequestFingerprintCodec::ENCODED_BYTES + sizeof(uint64_t) + sizeof(uint32_t);
  PutU32At(&unknown_command, frame_header_size + batch_prefix_size, 0x7fffffffU);
  PutU32At(&unknown_command, unknown_command.size() - sizeof(uint32_t),
           Crc32c(unknown_command.data() + frame_header_size,
                  unknown_command.size() - frame_header_size - sizeof(uint32_t)));
  EXPECT_THROW(CommandBatchCodec::Decode(unknown_command), std::runtime_error);

  auto unknown_fingerprint = golden;
  constexpr size_t fingerprint_version_offset = frame_header_size + sizeof(uint64_t) * 2;
  PutU32At(&unknown_fingerprint, fingerprint_version_offset, 2);
  PutU32At(&unknown_fingerprint, unknown_fingerprint.size() - sizeof(uint32_t),
           Crc32c(unknown_fingerprint.data() + frame_header_size,
                  unknown_fingerprint.size() - frame_header_size - sizeof(uint32_t)));
  EXPECT_THROW(CommandBatchCodec::Decode(unknown_fingerprint), std::runtime_error);

  TransactionCommandBatch unsorted{2, 9, 2, fingerprint, 4, {table_two_forty_two, minus_one}};
  EXPECT_THROW(CommandBatchCodec::Encode(unsorted), std::runtime_error);
}

TEST(CommandBatchCodecTest, CompleteFrameFitsOneReplicatedLogPayload) {
  static_assert(CommandBatchCodec::MAX_ENCODED_BATCH_BYTES == LogCodec::MAX_PAYLOAD_BYTES);
  EXPECT_EQ(CommandBatchCodec::MAX_BATCH_BYTES + CommandBatchCodec::FRAME_OVERHEAD_BYTES, LogCodec::MAX_PAYLOAD_BYTES);

  const std::string sql = "CREATE TABLE boundary_probe(id int PRIMARY KEY, note varchar(32));";
  const auto fingerprint = ComputeWriteIntentFingerprintV1(sql);
  const auto batch = CommandBuilder::Build(
      55, 1, fingerprint, 0,
      {CreateTableCommand{0,
                          0,
                          "boundary_probe",
                          {{"id", TypeId::INTEGER, 4, false}, {"note", TypeId::VARCHAR, 32, true}},
                          {0, TypeId::INTEGER, 1}}});
  const auto encoded_batch = CommandBatchCodec::Encode(batch);
  ASSERT_LE(encoded_batch.size(), LogCodec::MAX_PAYLOAD_BYTES);
  EXPECT_NO_THROW(LogCodec::Encode({1, 1, 1, EntryType::COMMAND_BATCH, encoded_batch}));
}

// M5 producer gate: permutation/collision behavior converges on the independent M2 frame above.
TEST(CommandBuilderTest, CanonicalizesPermutationsAndRejectsDuplicateKeys) {
  const std::vector<ReplicatedCommand> commands{Insert(2, 42, "c"), Insert(1, 42, "b"), Insert(1, -1, "a")};
  const auto fingerprint = LiteralFingerprint();
  const auto golden = CanonicalDmlGolden();
  EXPECT_EQ(CommandBatchCodec::Encode(CommandBuilder::Build(9, 1, fingerprint, 4, commands)), golden);
  EXPECT_EQ(CommandBatchCodec::Encode(CommandBuilder::Build(
                9, 1, fingerprint, 4, {Insert(1, -1, "a"), Insert(2, 42, "c"), Insert(1, 42, "b")})),
            golden);
  std::mt19937 generator(0x5eed);
  for (size_t iteration = 0; iteration < 64; iteration++) {
    auto permutation = commands;
    std::shuffle(permutation.begin(), permutation.end(), generator);
    EXPECT_EQ(CommandBatchCodec::Encode(CommandBuilder::Build(9, 1, fingerprint, 4, std::move(permutation))), golden)
        << "permutation iteration " << iteration;
  }
  EXPECT_THROW(CommandBuilder::Build(9, 2, fingerprint, 4, {Insert(1, 42, "x"), Insert(1, 42, "y")}),
               std::runtime_error);
}

// M5 full command-set and stable-value producer extension.
TEST(CommandBatchCodecTest, ExplicitOidDdlAndTupleCodec) {
  CreateTableCommand create{7,
                            11,
                            "accounts",
                            {{"id", TypeId::INTEGER, 4, false}, {"name", TypeId::VARCHAR, 64, true}},
                            {0, TypeId::INTEGER, 1}};
  const auto fingerprint = LiteralFingerprint();
  const auto batch = CommandBuilder::Build(3, 1, fingerprint, 10, {create});
  const TransactionCommandBatch literal_consumer_batch{2, 3, 1, fingerprint, 10, {create}};
  const auto create_golden =
      Hex("42434d4442415431000000020000008c0000000000000003000000000000000100000001000102030405060708090a0b"
          "0c0d0e0f101112131415161718191a1b1c1d1e1f000000000000000a000000010000000100000044000000070000000b"
          "000000086163636f756e747300000002000000026964000000040000000400000000046e616d65000000070000004001"
          "0000000000000004000000010bdbbe06");
  EXPECT_EQ(CommandBatchCodec::Encode(literal_consumer_batch), create_golden);
  EXPECT_EQ(CommandBatchCodec::Encode(batch), create_golden);
  const auto decoded = CommandBatchCodec::Decode(create_golden);
  EXPECT_EQ(decoded.format_version_, 2);
  EXPECT_EQ(decoded.client_id_, 3);
  EXPECT_EQ(decoded.request_id_, 1);
  EXPECT_EQ(decoded.request_fingerprint_, fingerprint);
  EXPECT_EQ(decoded.expected_start_schema_epoch_, 10);
  ASSERT_EQ(decoded.commands_.size(), 1);
  ASSERT_TRUE(std::holds_alternative<CreateTableCommand>(decoded.commands_[0]));
  const auto &decoded_create = std::get<CreateTableCommand>(decoded.commands_[0]);
  EXPECT_EQ(decoded_create.table_oid_, 7);
  EXPECT_EQ(decoded_create.primary_index_oid_, 11);
  EXPECT_EQ(decoded_create.table_name_, "accounts");
  ASSERT_EQ(decoded_create.columns_.size(), 2);
  EXPECT_EQ(decoded_create.columns_[0], (ReplicatedColumnDefinition{"id", TypeId::INTEGER, 4, false}));
  EXPECT_EQ(decoded_create.columns_[1], (ReplicatedColumnDefinition{"name", TypeId::VARCHAR, 64, true}));
  EXPECT_EQ(decoded_create.primary_key_, (ReplicatedPrimaryKeyDefinition{0, TypeId::INTEGER, 1}));

  Schema schema({Column("id", TypeId::INTEGER), Column("name", TypeId::VARCHAR, 64), Column("score", TypeId::BIGINT)});
  Tuple tuple({ValueFactory::GetIntegerValue(-7), ValueFactory::GetVarcharValue(std::string("A\0B", 3)),
               ValueFactory::GetBigIntValue(99)},
              &schema);
  const auto tuple_bytes = TupleCodecV1::Encode(tuple, schema);
  EXPECT_EQ(tuple_bytes, Hex("00000001000000030000000400fffffff900000007000000000341004200000005000000000000000063"));
  const auto restored = TupleCodecV1::Decode(tuple_bytes, schema);
  EXPECT_EQ(restored.GetValue(&schema, 0).GetAs<int32_t>(), -7);
  const auto restored_name = restored.GetValue(&schema, 1);
  ASSERT_FALSE(restored_name.IsNull());
  ASSERT_EQ(restored_name.GetStorageSize(), 4);
  EXPECT_EQ(std::string(restored_name.GetData(), restored_name.GetStorageSize() - 1), std::string("A\0B", 3));
  EXPECT_EQ(restored_name.GetData()[3], '\0');
  EXPECT_EQ(restored.GetValue(&schema, 2).GetAs<int64_t>(), 99);

  CreateIndexCommand unique{12, 7, "bad_unique", {1}, IndexType::BPlusTreeIndex, IndexConstraintKind::SECONDARY_UNIQUE};
  EXPECT_THROW(CommandBuilder::Build(3, 2, fingerprint, 11, {unique}), std::runtime_error);

  // A zero-row DML statement is a real committed request: its empty command
  // list must retain client/session identity in a stable frame.
  const auto empty_golden =
      Hex("42434d444241543100000002000000400000000000000003000000000000000200000001000102030405060708090a0b"
          "0c0d0e0f101112131415161718191a1b1c1d1e1f000000000000000b00000000c7b424c9");
  const TransactionCommandBatch literal_empty_dml{2, 3, 2, fingerprint, 11, {}};
  const auto empty_dml = CommandBuilder::Build(3, 2, fingerprint, 11, {});
  EXPECT_EQ(CommandBatchCodec::Encode(literal_empty_dml), empty_golden);
  EXPECT_EQ(CommandBatchCodec::Encode(empty_dml), empty_golden);
  const auto decoded_empty = CommandBatchCodec::Decode(empty_golden);
  EXPECT_EQ(decoded_empty.format_version_, 2);
  EXPECT_EQ(decoded_empty.client_id_, 3);
  EXPECT_EQ(decoded_empty.request_id_, 2);
  EXPECT_EQ(decoded_empty.request_fingerprint_, fingerprint);
  EXPECT_EQ(decoded_empty.expected_start_schema_epoch_, 11);
  EXPECT_TRUE(decoded_empty.commands_.empty());

  auto nullable_primary = create;
  nullable_primary.columns_[0].nullable_ = true;
  EXPECT_THROW(CommandBuilder::Build(3, 3, fingerprint, 11, {nullable_primary}), std::runtime_error);
  auto unsupported_primary = create;
  unsupported_primary.columns_[0].type_ = TypeId::DECIMAL;
  unsupported_primary.primary_key_.type_ = TypeId::DECIMAL;
  EXPECT_THROW(CommandBuilder::Build(3, 3, fingerprint, 11, {unsupported_primary}), std::runtime_error);
}

}  // namespace bustub
