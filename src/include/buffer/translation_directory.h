//===----------------------------------------------------------------------===//
// BusTub — F26 transient 8/7/16 page-to-frame directory (Linux).
//===----------------------------------------------------------------------===//
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex>

#include "common/config.h"

namespace bustub {
class ResourceBudget;

struct TranslationDirectoryState;
struct TranslationGroup;
struct TranslationEntry;

/** Short, thread-affine access to ONE translation. Retains its directory owner
 * and group gate. Never hold across content waits or IO. No raw entry escapes.
 * Frame() is only a lookup: F26/c must validate/pin the FrameHeader before this
 * access ends. Version() is a wrapping translation tag, not an IO identity. */
class TranslationAccess {
 public:
  ~TranslationAccess();
  TranslationAccess(TranslationAccess &&other) noexcept;
  TranslationAccess(const TranslationAccess &) = delete;
  auto operator=(const TranslationAccess &) -> TranslationAccess & = delete;
  auto operator=(TranslationAccess &&) -> TranslationAccess & = delete;

  auto Frame() const -> std::optional<frame_id_t>;
  auto Version() const -> uint32_t;
  void SetFrame(std::optional<frame_id_t> frame);

 private:
  friend class TranslationDirectory;
  TranslationAccess(std::shared_ptr<TranslationDirectoryState> owner, uint32_t page, TranslationGroup *group,
                    TranslationEntry *entry, std::shared_lock<std::shared_mutex> gate);
  std::shared_ptr<TranslationDirectoryState> owner_;
  uint32_t page_;
  TranslationGroup *group_;
  TranslationEntry *entry_;
  std::shared_lock<std::shared_mutex> gate_;
};

struct TranslationDirectoryOptions {
  // Owned directory/control/path-cache allocations + materialized OS pages. Leaf virtual
  // reservations and kernel page tables are not falsely counted as resident RAM.
  size_t max_bytes_;
  std::shared_ptr<ResourceBudget> memory_budget_{};
};

/** Each constructed directory is a distinct opened-instance context. No copy,
 * shared global page table, persistent mapping or WAL. Internal access guards
 * retain old state but can never publish into a later directory. Destruction
 * requires external calls to have stopped; existing guards can finish safely.
 * Bad page/frame inputs throw; exhausted memory budget throws std::bad_alloc.
 */
class TranslationDirectory {
 public:
  explicit TranslationDirectory(const TranslationDirectoryOptions &options);
  ~TranslationDirectory();
  TranslationDirectory(const TranslationDirectory &) = delete;
  auto operator=(const TranslationDirectory &) -> TranslationDirectory & = delete;

  auto Access(page_id_t page) -> TranslationAccess;
  /** Bounded maintenance at a BufferPool call boundary, before taking entry or
   * frame rights. Busy groups are skipped; actual OS reclaim errors propagate. */
  void Maintain();
  void Prefetch(const page_id_t *pages, size_t count) const;

 private:
  std::shared_ptr<TranslationDirectoryState> state_;
};

}  // namespace bustub
