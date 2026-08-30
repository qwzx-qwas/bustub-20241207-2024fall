//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// request_fingerprint.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "common/sha256.h"

namespace bustub {

/** Versioned binding for one exact write payload. Client/request identity is deliberately stored separately. */
struct RequestFingerprintV1 {
  static constexpr uint32_t FORMAT_VERSION = 1;
  static constexpr size_t DIGEST_SIZE = 32;

  uint32_t format_version_{0};
  std::array<std::byte, DIGEST_SIZE> digest_{};

  void Validate() const;

  friend auto operator==(const RequestFingerprintV1 &lhs, const RequestFingerprintV1 &rhs) -> bool {
    return lhs.format_version_ == rhs.format_version_ && lhs.digest_ == rhs.digest_;
  }
};

class RequestFingerprintCodec {
 public:
  static constexpr uint32_t FORMAT_VERSION = RequestFingerprintV1::FORMAT_VERSION;
  static constexpr size_t DIGEST_BYTES = RequestFingerprintV1::DIGEST_SIZE;
  static constexpr size_t ENCODED_BYTES = sizeof(uint32_t) + DIGEST_BYTES;
  static constexpr size_t MAX_WRITE_PAYLOAD_BYTES = 8U * 1024U * 1024U;

  static void Validate(const RequestFingerprintV1 &fingerprint);
  static auto Encode(const RequestFingerprintV1 &fingerprint) -> std::vector<std::byte>;
  static auto Decode(const std::vector<std::byte> &bytes) -> RequestFingerprintV1;
};

/**
 * Bind exact SQL bytes before any state-dependent parse or prepare. The preimage excludes client/request identity,
 * routing metadata, framing checksums, and prepared command bytes.
 */
auto ComputeWriteIntentFingerprintV1(std::string_view sql) -> RequestFingerprintV1;

}  // namespace bustub
