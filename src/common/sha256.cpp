//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sha256.cpp
//
//===----------------------------------------------------------------------===//

#include "common/sha256.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace bustub {
namespace {

constexpr size_t SHA256_BLOCK_BYTES = 64;

constexpr std::array<uint32_t, 64> ROUND_CONSTANTS{
    0x428a2f98U, 0x71374491U, 0xb5c0fbcfU, 0xe9b5dba5U, 0x3956c25bU, 0x59f111f1U, 0x923f82a4U, 0xab1c5ed5U,
    0xd807aa98U, 0x12835b01U, 0x243185beU, 0x550c7dc3U, 0x72be5d74U, 0x80deb1feU, 0x9bdc06a7U, 0xc19bf174U,
    0xe49b69c1U, 0xefbe4786U, 0x0fc19dc6U, 0x240ca1ccU, 0x2de92c6fU, 0x4a7484aaU, 0x5cb0a9dcU, 0x76f988daU,
    0x983e5152U, 0xa831c66dU, 0xb00327c8U, 0xbf597fc7U, 0xc6e00bf3U, 0xd5a79147U, 0x06ca6351U, 0x14292967U,
    0x27b70a85U, 0x2e1b2138U, 0x4d2c6dfcU, 0x53380d13U, 0x650a7354U, 0x766a0abbU, 0x81c2c92eU, 0x92722c85U,
    0xa2bfe8a1U, 0xa81a664bU, 0xc24b8b70U, 0xc76c51a3U, 0xd192e819U, 0xd6990624U, 0xf40e3585U, 0x106aa070U,
    0x19a4c116U, 0x1e376c08U, 0x2748774cU, 0x34b0bcb5U, 0x391c0cb3U, 0x4ed8aa4aU, 0x5b9cca4fU, 0x682e6ff3U,
    0x748f82eeU, 0x78a5636fU, 0x84c87814U, 0x8cc70208U, 0x90befffaU, 0xa4506cebU, 0xbef9a3f7U, 0xc67178f2U};

constexpr auto RotateRight(uint32_t value, uint32_t amount) -> uint32_t {
  return (value >> amount) | (value << (32U - amount));
}

auto ReadBigEndianU32(const std::byte *bytes) -> uint32_t {
  uint32_t value = 0;
  for (size_t index = 0; index < sizeof(uint32_t); index++) {
    value = (value << 8U) | static_cast<uint8_t>(bytes[index]);
  }
  return value;
}

void Transform(const std::byte *block, std::array<uint32_t, 8> *state) {
  std::array<uint32_t, 64> schedule{};
  for (size_t index = 0; index < 16; index++) {
    schedule[index] = ReadBigEndianU32(block + index * sizeof(uint32_t));
  }
  for (size_t index = 16; index < schedule.size(); index++) {
    const auto previous = schedule[index - 15];
    const auto sigma0 = RotateRight(previous, 7) ^ RotateRight(previous, 18) ^ (previous >> 3U);
    const auto recent = schedule[index - 2];
    const auto sigma1 = RotateRight(recent, 17) ^ RotateRight(recent, 19) ^ (recent >> 10U);
    schedule[index] = schedule[index - 16] + sigma0 + schedule[index - 7] + sigma1;
  }

  auto a = (*state)[0];
  auto b = (*state)[1];
  auto c = (*state)[2];
  auto d = (*state)[3];
  auto e = (*state)[4];
  auto f = (*state)[5];
  auto g = (*state)[6];
  auto h = (*state)[7];
  for (size_t index = 0; index < schedule.size(); index++) {
    const auto sum1 = RotateRight(e, 6) ^ RotateRight(e, 11) ^ RotateRight(e, 25);
    const auto choose = (e & f) ^ (~e & g);
    const auto temporary1 = h + sum1 + choose + ROUND_CONSTANTS[index] + schedule[index];
    const auto sum0 = RotateRight(a, 2) ^ RotateRight(a, 13) ^ RotateRight(a, 22);
    const auto majority = (a & b) ^ (a & c) ^ (b & c);
    const auto temporary2 = sum0 + majority;
    h = g;
    g = f;
    f = e;
    e = d + temporary1;
    d = c;
    c = b;
    b = a;
    a = temporary1 + temporary2;
  }

  (*state)[0] += a;
  (*state)[1] += b;
  (*state)[2] += c;
  (*state)[3] += d;
  (*state)[4] += e;
  (*state)[5] += f;
  (*state)[6] += g;
  (*state)[7] += h;
}

}  // namespace

auto Sha256(const std::byte *data, size_t size) -> Sha256Digest {
  if (data == nullptr && size != 0) {
    throw std::invalid_argument("SHA-256 input pointer is null");
  }
  if (size > std::numeric_limits<uint64_t>::max() / 8U) {
    throw std::length_error("SHA-256 input exceeds the FIPS 180-4 length field");
  }

  std::array<uint32_t, 8> state{0x6a09e667U, 0xbb67ae85U, 0x3c6ef372U, 0xa54ff53aU,
                                0x510e527fU, 0x9b05688cU, 0x1f83d9abU, 0x5be0cd19U};
  size_t offset = 0;
  while (size - offset >= SHA256_BLOCK_BYTES) {
    Transform(data + offset, &state);
    offset += SHA256_BLOCK_BYTES;
  }

  std::array<std::byte, SHA256_BLOCK_BYTES * 2> tail{};
  const auto remaining = size - offset;
  if (remaining != 0) {
    std::memcpy(tail.data(), data + offset, remaining);
  }
  tail[remaining] = std::byte{0x80};
  const size_t padded_size = remaining < 56 ? SHA256_BLOCK_BYTES : SHA256_BLOCK_BYTES * 2;
  const auto bit_length = static_cast<uint64_t>(size) * 8U;
  for (size_t index = 0; index < sizeof(uint64_t); index++) {
    tail[padded_size - 1 - index] = static_cast<std::byte>((bit_length >> (index * 8U)) & 0xffU);
  }
  Transform(tail.data(), &state);
  if (padded_size == SHA256_BLOCK_BYTES * 2) {
    Transform(tail.data() + SHA256_BLOCK_BYTES, &state);
  }

  Sha256Digest digest{};
  for (size_t word = 0; word < state.size(); word++) {
    for (size_t byte = 0; byte < sizeof(uint32_t); byte++) {
      digest[word * sizeof(uint32_t) + byte] =
          static_cast<std::byte>((state[word] >> ((sizeof(uint32_t) - 1 - byte) * 8U)) & 0xffU);
    }
  }
  return digest;
}

auto Sha256(const std::vector<std::byte> &data) -> Sha256Digest { return Sha256(data.data(), data.size()); }

}  // namespace bustub
