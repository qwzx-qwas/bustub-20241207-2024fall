#include "recovery/page_snapshot.h"
#include "catalog/catalog_snapshot.h"
#include "common/byte_codec.h"
namespace bustub {
auto PageSnapshotHeaderSize(uint64_t count, uint64_t catalog, uint64_t sessions) -> uint64_t {
  return (PAGE_SNAPSHOT_FIXED + count * 4 + catalog + sessions + 4 + BUSTUB_PAGE_SIZE - 1) / BUSTUB_PAGE_SIZE *
         BUSTUB_PAGE_SIZE;
}
auto ReadPageSnapshotDirectory(const SnapshotInput &input, uint64_t index) -> std::optional<PageSnapshotDirectory> {
  if (input.size_ < PAGE_SNAPSHOT_MAGIC.size() ||
      input.Read(0, PAGE_SNAPSHOT_MAGIC.size()) !=
          std::vector<std::byte>(PAGE_SNAPSHOT_MAGIC.begin(), PAGE_SNAPSHOT_MAGIC.end()))
    return std::nullopt;
  if (input.size_ < PAGE_SNAPSHOT_FIXED + 4 || input.size_ > (1ULL << 30U))
    throw std::runtime_error("invalid page snapshot size");
  const auto prefix = input.Read(8, PAGE_SNAPSHOT_FIXED - 8);
  ByteReader r(prefix);
  if (r.ReadU32() != 1 || r.ReadU32() != BUSTUB_PAGE_SIZE || r.ReadU64() != index || index >= TXN_START_ID)
    throw std::runtime_error("unsupported page snapshot format or boundary");
  const auto highwater = r.ReadU64(), count = r.ReadU64(), catalog = r.ReadU64(), sessions = r.ReadU64();
  if (highwater > INT32_MAX || count > highwater || count > input.size_ / BUSTUB_PAGE_SIZE ||
      catalog > CatalogSnapshotCodec::MAX_CATALOG_BYTES || sessions > 64U * 1024U * 1024U)
    throw std::runtime_error("page snapshot directory exceeds its limits");
  const auto header = PageSnapshotHeaderSize(count, catalog, sessions);
  if (header + count * BUSTUB_PAGE_SIZE != input.size_)
    throw std::runtime_error("page snapshot directory and body length differ");
  PageSnapshotDirectory result{highwater, header, catalog, sessions, {}};
  const auto bytes = input.Read(PAGE_SNAPSHOT_FIXED, count * 4);
  ByteReader entries(bytes);
  result.pages_.reserve(count);
  for (uint64_t i = 0; i < count; ++i) {
    const auto page = entries.ReadU32();
    if (page >= highwater || (!result.pages_.empty() && page <= static_cast<uint32_t>(result.pages_.back())))
      throw std::runtime_error("page snapshot has an invalid or duplicate page identity");
    result.pages_.push_back(static_cast<page_id_t>(page));
  }
  return result;
}
}  // namespace bustub
