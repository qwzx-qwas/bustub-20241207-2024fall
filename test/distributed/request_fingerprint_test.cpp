//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// request_fingerprint_test.cpp
//
//===----------------------------------------------------------------------===//

#include "distributed/request_fingerprint.h"
#include "common/sha256.h"

#include <array>        // NOLINT(build/include_order)
#include <cctype>       // NOLINT(build/include_order)
#include <cstddef>      // NOLINT(build/include_order)
#include <cstdint>      // NOLINT(build/include_order)
#include <stdexcept>    // NOLINT(build/include_order)
#include <string>       // NOLINT(build/include_order)
#include <string_view>  // NOLINT(build/include_order)
#include <vector>       // NOLINT(build/include_order)

#include "gtest/gtest.h"

namespace bustub {
namespace {

auto Bytes(std::string_view value) -> std::vector<std::byte> {
  const auto *first = reinterpret_cast<const std::byte *>(value.data());
  return {first, first + value.size()};
}

auto HexNibble(char value) -> uint8_t {
  if (value >= '0' && value <= '9') {
    return static_cast<uint8_t>(value - '0');
  }
  value = static_cast<char>(std::tolower(static_cast<unsigned char>(value)));
  if (value >= 'a' && value <= 'f') {
    return static_cast<uint8_t>(value - 'a' + 10);
  }
  throw std::runtime_error("invalid test hex literal");
}

auto BytesFromHex(std::string_view hex) -> std::vector<std::byte> {
  if (hex.size() % 2 != 0) {
    throw std::runtime_error("odd test hex literal");
  }
  std::vector<std::byte> bytes;
  bytes.reserve(hex.size() / 2);
  for (size_t index = 0; index < hex.size(); index += 2) {
    bytes.push_back(static_cast<std::byte>((HexNibble(hex[index]) << 4U) | HexNibble(hex[index + 1])));
  }
  return bytes;
}

auto Hex(const Sha256Digest &digest) -> std::string {
  constexpr std::string_view digits = "0123456789abcdef";
  std::string result;
  result.reserve(digest.size() * 2);
  for (const auto byte : digest) {
    const auto value = static_cast<uint8_t>(byte);
    result.push_back(digits[value >> 4U]);
    result.push_back(digits[value & 0x0fU]);
  }
  return result;
}

TEST(Sha256Test, MatchesNistKnownAnswerVectors) {
  EXPECT_EQ(Hex(Sha256(nullptr, 0)), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  EXPECT_EQ(Hex(Sha256(Bytes("abc"))), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  EXPECT_EQ(Hex(Sha256(Bytes("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"))),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
  const std::vector<std::byte> million_as(1000000, std::byte{'a'});
  EXPECT_EQ(Hex(Sha256(million_as)), "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
  EXPECT_THROW(Sha256(nullptr, 1), std::invalid_argument);
}

TEST(RequestFingerprintTest, MatchesLiteralWriteIntentOracle) {
  constexpr std::string_view sql = "INSERT INTO t VALUES (1)";
  const auto literal_preimage = BytesFromHex(
      "4255535455425f524146545f57524954455f494e54454e54"
      "00000001"
      "00000001"
      "00000018"
      "494e5345525420494e544f20742056414c55455320283129");
  ASSERT_EQ(literal_preimage.size(), 60U);
  EXPECT_EQ(Hex(Sha256(literal_preimage)), "1a868f938e9bd8560fff5229bc54a24c72f1ef29c4f1535f64a6f937c3e375c2");

  const auto fingerprint = ComputeWriteIntentFingerprintV1(sql);
  EXPECT_EQ(fingerprint.format_version_, 1U);
  EXPECT_EQ(Hex(fingerprint.digest_), "1a868f938e9bd8560fff5229bc54a24c72f1ef29c4f1535f64a6f937c3e375c2");

  const auto literal_record = BytesFromHex(
      "00000001"
      "1a868f938e9bd8560fff5229bc54a24c72f1ef29c4f1535f64a6f937c3e375c2");
  EXPECT_EQ(RequestFingerprintCodec::Encode(fingerprint), literal_record);
  const auto decoded_literal = RequestFingerprintCodec::Decode(literal_record);
  EXPECT_EQ(decoded_literal.format_version_, 1U);
  EXPECT_EQ(Hex(decoded_literal.digest_), "1a868f938e9bd8560fff5229bc54a24c72f1ef29c4f1535f64a6f937c3e375c2");
}

TEST(RequestFingerprintTest, BindsExactRawPayloadBytes) {
  const auto original = ComputeWriteIntentFingerprintV1("INSERT INTO t VALUES (1)");
  EXPECT_FALSE(ComputeWriteIntentFingerprintV1("INSERT  INTO t VALUES (1)") == original);
  EXPECT_FALSE(ComputeWriteIntentFingerprintV1("insert into t values (1)") == original);
  EXPECT_FALSE(ComputeWriteIntentFingerprintV1("INSERT INTO t VALUES (1);") == original);
  const std::string embedded_nul{"INSERT INTO t VALUES (1)\0--suffix", 33};
  EXPECT_FALSE(ComputeWriteIntentFingerprintV1(embedded_nul) == original);
}

TEST(RequestFingerprintTest, RejectsInvalidVersionLengthAndPayloadSize) {
  const RequestFingerprintV1 default_fingerprint;
  EXPECT_THROW(default_fingerprint.Validate(), std::runtime_error);
  EXPECT_THROW(RequestFingerprintCodec::Validate(default_fingerprint), std::runtime_error);
  EXPECT_THROW(RequestFingerprintCodec::Encode(default_fingerprint), std::runtime_error);

  auto wrong_version = BytesFromHex(
      "00000002"
      "1a868f938e9bd8560fff5229bc54a24c72f1ef29c4f1535f64a6f937c3e375c2");
  EXPECT_THROW(RequestFingerprintCodec::Decode(wrong_version), std::runtime_error);
  wrong_version.pop_back();
  EXPECT_THROW(RequestFingerprintCodec::Decode(wrong_version), std::runtime_error);

  EXPECT_THROW(ComputeWriteIntentFingerprintV1(""), std::runtime_error);
  const std::string oversized(RequestFingerprintCodec::MAX_WRITE_PAYLOAD_BYTES + 1, 'x');
  EXPECT_THROW(ComputeWriteIntentFingerprintV1(oversized), std::length_error);
}

}  // namespace
}  // namespace bustub
