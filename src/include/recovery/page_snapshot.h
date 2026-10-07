// Native immutable page-stream directory, shared by install and incremental planning.
#pragma once
#include <array>
#include <optional>
#include "common/config.h"
#include "recovery/snapshot_stream.h"
namespace bustub {
inline constexpr std::array<std::byte, 8> PAGE_SNAPSHOT_MAGIC{std::byte{'B'}, std::byte{'S'}, std::byte{'P'},
                                                              std::byte{'A'}, std::byte{'G'}, std::byte{'E'},
                                                              std::byte{'0'}, std::byte{'1'}};
inline constexpr uint64_t PAGE_SNAPSHOT_FIXED = 56;
auto PageSnapshotHeaderSize(uint64_t count, uint64_t catalog, uint64_t sessions) -> uint64_t;
struct PageSnapshotDirectory {
  uint64_t highwater_, header_size_, catalog_size_, session_size_;
  std::vector<page_id_t> pages_;
};
// nullopt means another format, not corrupt native metadata. Does not read page bodies.
auto ReadPageSnapshotDirectory(const SnapshotInput &input, uint64_t index) -> std::optional<PageSnapshotDirectory>;
}  // namespace bustub
