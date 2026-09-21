//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// storage_range_contract_test.cpp
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string>
#include <system_error>  // NOLINT(build/c++11)
#include <utility>
#include <vector>

#include "common/byte_codec.h"
#include "distributed/raft_state_machine.h"
#include "gtest/gtest.h"
#include "recovery/durable_storage.h"
#include "storage/byte_range.h"

namespace bustub {
namespace {

// Observe the granted slice at the existing storage boundary, then use real file IO.
// A rejected OS read must not hide that the decoder already requested another range.
class SliceCheckingStorage : public PosixDurableStorage {
 public:
  explicit SliceCheckingStorage(DurableFileSlice allowed) : allowed_(std::move(allowed)) {}

  auto ReadFile(const std::filesystem::path &path, size_t maximum_size) -> std::vector<std::byte> override {
    return ReadFileRange(path, 0, maximum_size);
  }

  auto ReadFileRange(const std::filesystem::path &path, uint64_t offset, size_t maximum_size)
      -> std::vector<std::byte> override {
    if (path != allowed_.path_ || allowed_.size_ > std::numeric_limits<uint64_t>::max() - allowed_.offset_ ||
        offset < allowed_.offset_ || offset - allowed_.offset_ > allowed_.size_ ||
        maximum_size > allowed_.size_ - (offset - allowed_.offset_)) {
      read_outside_slice_ = true;
      throw std::runtime_error("decoder requested bytes outside its granted slice");
    }
    return PosixDurableStorage::ReadFileRange(path, offset, maximum_size);
  }

  bool read_outside_slice_{false};

 private:
  DurableFileSlice allowed_;
};

TEST(StorageRangeContractTest, AddressRangesCannotWrap) {
  constexpr auto limit = std::numeric_limits<uint64_t>::max();
  EXPECT_FALSE(StorageByteRange::Create(limit - 3, 4).has_value());
  EXPECT_FALSE(StorageByteRange::Create(1, limit).has_value());

  const auto range = StorageByteRange::Create(limit - 3, 3);
  ASSERT_TRUE(range.has_value());
  const auto last_byte = range->Subrange(2, 1);
  ASSERT_TRUE(last_byte.has_value());
  EXPECT_EQ(last_byte->Offset(), limit - 1);
  EXPECT_EQ(last_byte->Size(), 1);
}

TEST(StorageRangeContractTest, SubrangesCannotEscapeOrWrap) {
  const auto parent = StorageByteRange::Create(4096, 8192);
  ASSERT_TRUE(parent.has_value());
  const auto tail = parent->Subrange(8190, 2);
  ASSERT_TRUE(tail.has_value());
  EXPECT_EQ(tail->Offset(), 12286);
  EXPECT_EQ(tail->Size(), 2);
  EXPECT_FALSE(parent->Subrange(8190, 3).has_value());
  EXPECT_FALSE(parent->Subrange(std::numeric_limits<uint64_t>::max(), 1).has_value());
  EXPECT_FALSE(parent->Subrange(1, std::numeric_limits<uint64_t>::max()).has_value());

  // Empty ranges express an EOF position; they do not authorize reading its next byte.
  const auto eof = parent->Subrange(8192, 0);
  ASSERT_TRUE(eof.has_value());
  EXPECT_EQ(eof->Offset(), 12288);
  EXPECT_EQ(eof->Size(), 0);
  EXPECT_FALSE(eof->Subrange(0, 1).has_value());
  EXPECT_FALSE(parent->Subrange(8193, 0).has_value());
}

class SnapshotRangeContractTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto pattern = (std::filesystem::temp_directory_path() / "bustub-s0-range-XXXXXX").string();
    const auto *directory = mkdtemp(pattern.data());
    ASSERT_NE(directory, nullptr);
    root_ = directory;
    path_ = root_ / "container.bundle";

    // The format oracle is the existing independent V1 golden test. Here a valid, nonempty
    // bundle is surrounded by unrelated bytes to exercise the public file-slice boundary.
    const BusTubSnapshotBundleV1 bundle{
        7, {std::byte{0x10}, std::byte{0x20}, std::byte{0x30}}, {std::byte{0x40}}, {std::byte{0x50}, std::byte{0x60}}};
    const auto encoded = BusTubSnapshotBundleCodec::Encode(bundle);
    bytes_ = {std::byte{0xa1}, std::byte{0xb2}, std::byte{0xc3}};
    payload_ = {path_, bytes_.size(), encoded.size()};
    bytes_.insert(bytes_.end(), encoded.begin(), encoded.end());
    bytes_.push_back(std::byte{0xd4});
    bytes_.push_back(std::byte{0xe5});
    storage_.WriteFile(path_, bytes_);

    // Qualify both the fixture and the observer before introducing malformed bounds.
    SliceCheckingStorage observed(payload_);
    ASSERT_NO_THROW(static_cast<void>(BusTubSnapshotBundleCodec::DecodeFile(payload_, &observed)));
    ASSERT_FALSE(observed.read_outside_slice_);
  }

  void TearDown() override {
    if (!root_.empty()) {
      std::error_code error;
      std::filesystem::remove_all(root_, error);
      EXPECT_FALSE(error) << error.message();
    }
  }

  void ExpectRejectedWithin(const DurableFileSlice &slice) {
    SliceCheckingStorage observed(slice);
    EXPECT_THROW(BusTubSnapshotBundleCodec::DecodeFile(slice, &observed), std::runtime_error);
    EXPECT_FALSE(observed.read_outside_slice_);
  }

  PosixDurableStorage storage_;
  std::filesystem::path root_;
  std::filesystem::path path_;
  DurableFileSlice payload_;
  std::vector<std::byte> bytes_;
};

TEST_F(SnapshotRangeContractTest, RejectsContentOutsideDeclaredSlice) {
  auto shortened = payload_;
  shortened.size_--;
  ExpectRejectedWithin(shortened);

  auto extended = payload_;
  extended.size_++;
  ExpectRejectedWithin(extended);
}

TEST_F(SnapshotRangeContractTest, RejectsWrappingSlice) {
  auto wrapped = payload_;
  wrapped.offset_ = std::numeric_limits<uint64_t>::max() - 8;
  ExpectRejectedWithin(wrapped);
}

TEST_F(SnapshotRangeContractTest, RejectsOversizedNestedContent) {
  // V1's database length follows magic(8), version(4) and applied index(8).
  // A UINT64_MAX length must not wrap a cursor or reach beyond the granted slice.
  for (size_t i = 0; i < sizeof(uint64_t); i++) {
    bytes_[payload_.offset_ + 20 + i] = std::byte{0xff};
  }
  // Keep the checksum valid so checksum rejection cannot substitute for length validation.
  // This helper builds the fixture only; the independent expectation is rejection without
  // requesting bytes beyond the caller's slice, not agreement between two CRC computations.
  const auto checksum_offset = payload_.offset_ + payload_.size_ - sizeof(uint32_t);
  const auto checksum = Crc32c(bytes_.data() + payload_.offset_ + 8, payload_.size_ - 8 - sizeof(uint32_t));
  for (size_t i = 0; i < sizeof(uint32_t); i++) {
    bytes_[checksum_offset + i] = static_cast<std::byte>((checksum >> (24 - 8 * i)) & 0xffU);
  }
  storage_.WriteFile(path_, bytes_);
  ExpectRejectedWithin(payload_);
}

}  // namespace
}  // namespace bustub
