//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sha256.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <array>
#include <cstddef>
#include <vector>

namespace bustub {

using Sha256Digest = std::array<std::byte, 32>;

/** Compute the SHA-256 digest defined by FIPS 180-4. */
auto Sha256(const std::byte *data, size_t size) -> Sha256Digest;

/** Convenience overload for an owned byte sequence. */
auto Sha256(const std::vector<std::byte> &data) -> Sha256Digest;

}  // namespace bustub
