//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// request_fingerprint.cpp
//
//===----------------------------------------------------------------------===//

#include "distributed/request_fingerprint.h"

#include <algorithm>
#include <array>
#include <limits>
#include <stdexcept>

#include "common/byte_codec.h"

namespace bustub {
namespace {

constexpr std::array<std::byte, 24> WRITE_INTENT_DOMAIN{
    std::byte{'B'}, std::byte{'U'}, std::byte{'S'}, std::byte{'T'}, std::byte{'U'}, std::byte{'B'},
    std::byte{'_'}, std::byte{'R'}, std::byte{'A'}, std::byte{'F'}, std::byte{'T'}, std::byte{'_'},
    std::byte{'W'}, std::byte{'R'}, std::byte{'I'}, std::byte{'T'}, std::byte{'E'}, std::byte{'_'},
    std::byte{'I'}, std::byte{'N'}, std::byte{'T'}, std::byte{'E'}, std::byte{'N'}, std::byte{'T'}};
constexpr uint32_t WRITE_SQL_OPERATION = 1;

static_assert(RequestFingerprintCodec::DIGEST_BYTES == Sha256Digest{}.size());

}  // namespace

void RequestFingerprintV1::Validate() const {
  if (format_version_ != FORMAT_VERSION) {
    throw std::runtime_error("unsupported request fingerprint version");
  }
}

void RequestFingerprintCodec::Validate(const RequestFingerprintV1 &fingerprint) { fingerprint.Validate(); }

auto RequestFingerprintCodec::Encode(const RequestFingerprintV1 &fingerprint) -> std::vector<std::byte> {
  Validate(fingerprint);
  ByteWriter writer;
  writer.PutU32(fingerprint.format_version_);
  writer.PutBytes(fingerprint.digest_.data(), fingerprint.digest_.size());
  return writer.Take();
}

auto RequestFingerprintCodec::Decode(const std::vector<std::byte> &bytes) -> RequestFingerprintV1 {
  if (bytes.size() != ENCODED_BYTES) {
    throw std::runtime_error("invalid request fingerprint length");
  }
  ByteReader reader(bytes);
  RequestFingerprintV1 fingerprint;
  fingerprint.format_version_ = reader.ReadU32();
  const auto digest = reader.ReadBytes(DIGEST_BYTES);
  std::copy(digest.begin(), digest.end(), fingerprint.digest_.begin());
  Validate(fingerprint);
  return fingerprint;
}

auto ComputeWriteIntentFingerprintV1(std::string_view sql) -> RequestFingerprintV1 {
  if (sql.empty()) {
    throw std::runtime_error("write intent SQL is empty");
  }
  if (sql.size() > RequestFingerprintCodec::MAX_WRITE_PAYLOAD_BYTES ||
      sql.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::length_error("write intent SQL exceeds the fingerprint boundary");
  }

  ByteWriter preimage;
  preimage.PutBytes(WRITE_INTENT_DOMAIN.data(), WRITE_INTENT_DOMAIN.size());
  preimage.PutU32(RequestFingerprintCodec::FORMAT_VERSION);
  preimage.PutU32(WRITE_SQL_OPERATION);
  preimage.PutU32(static_cast<uint32_t>(sql.size()));
  preimage.PutBytes(sql.data(), sql.size());
  return {RequestFingerprintV1::FORMAT_VERSION, Sha256(preimage.Data())};
}

}  // namespace bustub
