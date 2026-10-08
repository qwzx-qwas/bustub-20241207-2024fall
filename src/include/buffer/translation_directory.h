//===----------------------------------------------------------------------===//
// BusTub — F26 transient 8/7/16 page-to-frame directory (Linux).
//===----------------------------------------------------------------------===//
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

#include "common/config.h"

namespace bustub {
class ResourceBudget;

struct TranslationDirectoryState;
struct TranslationGroup;
struct TranslationReader;
class TranslationDirectory;

/** Thread-affine call scope: reserve one reusable access record before side
 * effects. It protects no group while idle, and must outlive borrowed accesses.
 * Nested accesses still use separate records. */
class TranslationContext {
 public:
  explicit TranslationContext(TranslationDirectory &directory);
  ~TranslationContext();
  TranslationContext(const TranslationContext &) = delete;
  auto operator=(const TranslationContext &) -> TranslationContext & = delete;

 private:
  friend class TranslationDirectory;
  std::shared_ptr<TranslationDirectoryState> owner_;
  TranslationReader *reader_;
  TranslationContext *previous_;
};

/** Short, thread-affine access to ONE translation. Retains its directory owner
 * and a private access registration. Never hold across content waits or IO. No raw entry escapes.
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
                    uint64_t *entry, TranslationReader *reader, bool write);
  std::shared_ptr<TranslationDirectoryState> owner_;
  uint32_t page_;
  TranslationGroup *group_;
  uint64_t *entry_;
  TranslationReader *reader_;
  bool write_;
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

  /** Read-only atomic lookup; does not materialize an empty translation page.
   * Before using a candidate, lock the frame and recheck Frame() and page identity. */
  auto Lookup(page_id_t page) -> TranslationAccess;
  /** Exclusive mapping access, reserving backing memory before the first CAS. */
  auto Access(page_id_t page) -> TranslationAccess;
  /** Bounded maintenance at a BufferPool call boundary, before taking entry or
   * frame rights. Busy groups are skipped; actual OS reclaim errors propagate. */
  void Maintain();
  void Prefetch(const page_id_t *pages, size_t count) const;

 private:
  friend class TranslationContext;
  auto Open(page_id_t page, bool write) -> TranslationAccess;
  std::shared_ptr<TranslationDirectoryState> state_;
};

}  // namespace bustub
