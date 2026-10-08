#include "buffer/translation_directory.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <mutex>
#include <new>
#include <thread>
#include <utility>

#include "buffer_memory.h"
#include "storage/disk/resource_budget.h"

namespace bustub {

namespace {
constexpr size_t kLeafEntries = 1U << 16;
constexpr uint64_t kFrameMask = 0xffffffffULL;
constexpr uint64_t kVersionMask = 0xffffffULL;
constexpr uint64_t kLatchMask = 0xff00000000000000ULL;

class Budget {
 public:
  explicit Budget(const TranslationDirectoryOptions &options)
      : limit_(options.max_bytes_), memory_(ResourceAccount::Create(options.memory_budget_)) {}
  auto TryTake(size_t bytes) -> bool {
    auto used = used_.load(std::memory_order_relaxed);
    do {
      if (bytes > limit_ - used) {
        return false;
      }
    } while (!used_.compare_exchange_weak(used, used + bytes, std::memory_order_relaxed));
    try {
      if (memory_->Reserve(bytes, false)) return true;
    } catch (...) {
      used_.fetch_sub(bytes, std::memory_order_relaxed);
      throw;
    }
    used_.fetch_sub(bytes, std::memory_order_relaxed);
    return false;
  }
  void Take(size_t bytes) {
    if (!TryTake(bytes)) {
      throw std::bad_alloc();
    }
  }
  void Return(size_t bytes) {
    memory_->Release(bytes, false);
    used_.fetch_sub(bytes, std::memory_order_relaxed);
  }
  auto Account() const -> std::shared_ptr<ResourceAccount> { return memory_; }

 private:
  size_t limit_;
  std::atomic<size_t> used_{0};
  std::shared_ptr<ResourceAccount> memory_;
};

// Producers set hints; the one maintenance visitor consumes them. Pop rotates
// so a busy group cannot permanently hide later candidates. Parent hints may be
// conservative; they never authorize reclaim without the group's exclusive gate.
class Candidates {
 public:
  static auto Bytes(size_t count) -> size_t { return ((count + 63) / 64) * sizeof(std::atomic<uint64_t>); }
  explicit Candidates(size_t count) : count_(count), words_(new std::atomic<uint64_t>[(count + 63) / 64]) {
    for (size_t w = 0; w < (count + 63) / 64; ++w) {
      words_[w].store(0);
    }
  }
  void Mark(size_t bit) { words_[bit / 64].fetch_or(uint64_t{1} << (bit % 64), std::memory_order_release); }
  auto Any() const -> bool {
    for (size_t w = 0; w < (count_ + 63) / 64; ++w) {
      if (words_[w].load(std::memory_order_acquire) != 0) {
        return true;
      }
    }
    return false;
  }
  auto Pop() -> std::optional<size_t> {
    // Two half-open intervals implement wraparound without repeatedly choosing
    // the lowest set bit. Only the serialized visitor changes next_.
    for (auto range : {std::pair<size_t, size_t>{next_, count_}, {0, next_}}) {
      auto begin = range.first;
      while (begin < range.second) {
        const auto end = std::min(range.second, (begin / 64 + 1) * 64);
        auto bits = words_[begin / 64].load(std::memory_order_acquire);
        bits &= ~uint64_t{0} << (begin % 64);
        if (end % 64 != 0) {
          bits &= (uint64_t{1} << (end % 64)) - 1;
        }
        if (bits != 0) {
          const auto offset = static_cast<size_t>(__builtin_ctzll(bits));
          const auto result = (begin / 64) * 64 + offset;
          words_[begin / 64].fetch_and(~(uint64_t{1} << offset), std::memory_order_acq_rel);
          next_ = (result + 1) % count_;
          return result;
        }
        begin = end;
      }
    }
    return std::nullopt;
  }

 private:
  size_t count_;
  std::unique_ptr<std::atomic<uint64_t>[]> words_;
  size_t next_{0};
};

// Member-before-resource ordering returns budget even if construction throws.
class Charge {
 public:
  Charge(Budget *budget, size_t bytes) : budget_(budget), bytes_(bytes) { budget_->Take(bytes_); }
  ~Charge() {
    if (budget_ != nullptr) {
      budget_->Return(bytes_);
    }
  }
  Charge(Charge &&other) noexcept : budget_(std::exchange(other.budget_, nullptr)), bytes_(other.bytes_) {}
  Charge(const Charge &) = delete;
  auto operator=(const Charge &) -> Charge & = delete;

 private:
  Budget *budget_;
  size_t bytes_;
};
}  // namespace

struct TranslationEntry {
  std::atomic<uint64_t> word_{0};

  void Lock() {
    auto value = word_.load(std::memory_order_relaxed);
    for (;;) {
      if ((value & kLatchMask) == 0 && word_.compare_exchange_weak(value, value | kLatchMask, std::memory_order_acquire,
                                                                   std::memory_order_relaxed)) {
        return;
      }
      std::this_thread::yield();
      value = word_.load(std::memory_order_relaxed);
    }
  }
  void Unlock() { word_.fetch_and(~kLatchMask, std::memory_order_release); }
};
static_assert(sizeof(TranslationEntry) == 8, "F26 translation must occupy eight bytes");
static_assert(std::atomic<uint64_t>::is_always_lock_free, "F26 requires lock-free 64-bit atomic entries");

struct TranslationGroup {
  std::shared_mutex gate_;
  bool constructed_{false};  // Under gate_, never stored in reclaimable RAM.
  std::atomic<size_t> live_entries_{0};
};

namespace {
struct Leaf {
  static auto Create(Budget *budget, size_t os_page) -> std::unique_ptr<Leaf> {
    Charge charge(budget, buffer_memory::Add(
                              sizeof(Leaf),
                              buffer_memory::Add(buffer_memory::Multiply(GroupCount(os_page), sizeof(TranslationGroup)),
                                                 Candidates::Bytes(GroupCount(os_page)))));
    return std::make_unique<Leaf>(std::move(charge), budget, os_page);
  }
  Leaf(Charge charge, Budget *budget, size_t os_page)
      : control_charge_(std::move(charge)),
        budget_(budget),
        page_bytes_(os_page),
        entries_per_group_(os_page / sizeof(TranslationEntry)),
        memory_(kLeafEntries * sizeof(TranslationEntry), os_page, os_page),
        groups_(std::make_unique<TranslationGroup[]>(GroupCount(os_page))),
        candidates_(GroupCount(os_page)) {
    memory_.UseBasePages();
  }
  ~Leaf() {
    for (size_t group = 0; group < GroupCount(page_bytes_); ++group) {
      if (groups_[group].constructed_) {
        const auto end = std::min((group + 1) * entries_per_group_, kLeafEntries);
        for (size_t i = group * entries_per_group_; i < end; ++i) {
          At(i)->~TranslationEntry();
        }
        budget_->Return(page_bytes_);
      }
    }
  }
  static auto GroupCount(size_t os_page) -> size_t {
    return buffer_memory::RoundUp(kLeafEntries * sizeof(TranslationEntry), os_page) / os_page;
  }
  auto At(size_t index) -> TranslationEntry * {
    return std::launder(reinterpret_cast<TranslationEntry *>(memory_.Data() + index * sizeof(TranslationEntry)));
  }
  void Construct(size_t index) {
    const auto end = std::min((index + 1) * entries_per_group_, kLeafEntries);
    for (size_t i = index * entries_per_group_; i < end; ++i) {
      new (memory_.Data() + i * sizeof(TranslationEntry)) TranslationEntry();
    }
    groups_[index].constructed_ = true;
  }
  auto OpenGroup(size_t index) -> std::shared_lock<std::shared_mutex> {
    auto &group = groups_[index];
    for (;;) {
      std::shared_lock read(group.gate_);
      if (group.constructed_) {
        return read;
      }
      read.unlock();
      {
        std::unique_lock write(group.gate_);
        if (!group.constructed_) {
          budget_->Take(page_bytes_);
          Construct(index);  // Construction cannot throw.
        }
      }
      // Recheck after reacquiring the shared gate: maintenance may have reclaimed
      // this empty group between the exclusive unlock and this new access.
    }
  }
  auto Reclaim(size_t index) -> bool {
    auto &group = groups_[index];
    std::unique_lock gate(group.gate_, std::try_to_lock);
    if (!gate.owns_lock()) {
      candidates_.Mark(index);
      return false;
    }
    if (!group.constructed_ || group.live_entries_.load(std::memory_order_relaxed) != 0) {
      return false;
    }
    const auto end = std::min((index + 1) * entries_per_group_, kLeafEntries);
    for (size_t i = index * entries_per_group_; i < end; ++i) {
      At(i)->~TranslationEntry();
    }
    group.constructed_ = false;
    if (madvise(memory_.Data() + index * page_bytes_, page_bytes_, MADV_DONTNEED) != 0) {
      const auto error = errno;
      Construct(index);  // Empty group remains charged and has valid C++ objects.
      candidates_.Mark(index);
      throw std::system_error(error, std::generic_category(), "reclaim translation memory");
    }
    budget_->Return(page_bytes_);
    return true;
  }

  Charge control_charge_;
  Budget *budget_;
  size_t page_bytes_;
  size_t entries_per_group_;
  buffer_memory::Region memory_;
  std::unique_ptr<TranslationGroup[]> groups_;
  Candidates candidates_;
};

template <typename T>
struct Slot {
  std::mutex mutex_;
  std::unique_ptr<T> child_;
  std::atomic<T *> published_{nullptr};

  template <typename... Args>
  auto Get(Args... args) -> T * {
    if (auto *ready = published_.load(std::memory_order_acquire); ready != nullptr) {
      return ready;
    }
    auto candidate = T::Create(args...);
    T *result;
    {
      std::lock_guard lock(mutex_);
      if (child_ == nullptr) {
        child_ = std::move(candidate);
        published_.store(child_.get(), std::memory_order_release);
      }
      result = child_.get();
    }
    // The losing candidate releases its mapping and charges outside the lock.
    return result;
  }
};

struct Middle {
  static auto Create(Budget *budget) -> std::unique_ptr<Middle> {
    Charge charge(budget, sizeof(Middle) + Candidates::Bytes(128));
    return std::make_unique<Middle>(std::move(charge));
  }
  explicit Middle(Charge charge) : charge_(std::move(charge)), candidates_(128) {}
  Charge charge_;
  std::array<Slot<Leaf>, 128> leaves_;
  Candidates candidates_;
};
}  // namespace

struct TranslationDirectoryState {
  explicit TranslationDirectoryState(const TranslationDirectoryOptions &options)
      : budget_(options),
        charge_(&budget_, sizeof(TranslationDirectoryState) + Candidates::Bytes(256)),
        os_page_(buffer_memory::PageBytes()),
        candidates_(256) {
    if (os_page_ % sizeof(TranslationEntry) != 0) {
      throw std::runtime_error("OS page cannot hold aligned translation groups");
    }
  }
  auto Resolve(uint32_t prefix) -> Leaf * {
    auto *middle = roots_[prefix >> 7].Get(&budget_);
    return middle->leaves_[prefix & 0x7fU].Get(&budget_, os_page_);
  }
  void Mark(uint32_t page) {
    const auto prefix = page >> 16;
    auto *middle = roots_[prefix >> 7].published_.load(std::memory_order_acquire);
    auto *leaf = middle->leaves_[prefix & 0x7fU].published_.load(std::memory_order_acquire);
    leaf->candidates_.Mark((page & 0xffffU) / leaf->entries_per_group_);
    middle->candidates_.Mark(prefix & 0x7fU);
    candidates_.Mark(prefix >> 7);
  }
  auto Reclaim(size_t attempts) -> size_t {
    std::unique_lock visitor(maintenance_, std::try_to_lock);
    if (!visitor.owns_lock()) {
      return 0;
    }
    size_t reclaimed = 0;
    for (size_t step = 0; step < attempts; ++step) {
      const auto root = candidates_.Pop();
      if (!root) {
        break;
      }
      auto *middle = roots_[*root].published_.load(std::memory_order_acquire);
      const auto child = middle->candidates_.Pop();
      if (child) {
        auto *leaf = middle->leaves_[*child].published_.load(std::memory_order_acquire);
        const auto group = leaf->candidates_.Pop();
        try {
          if (group && leaf->Reclaim(*group)) {
            reclaimed += os_page_;
          }
        } catch (...) {
          middle->candidates_.Mark(*child);
          candidates_.Mark(*root);
          throw;
        }
        if (leaf->candidates_.Any()) {
          middle->candidates_.Mark(*child);
        }
      }
      if (middle->candidates_.Any()) {
        candidates_.Mark(*root);
      }
    }
    return reclaimed;
  }
  Budget budget_;
  Charge charge_;
  size_t os_page_;
  std::array<Slot<Middle>, 256> roots_;
  Candidates candidates_;
  std::mutex maintenance_;
};

namespace {
// One bounded cache per thread, for its most recently used directory. Weak
// identity is checked before touching leaf pointers; it cannot keep a closed
// directory alive. The TLS shell is one pointer/counter, not an owner registry.
struct PathCache {
  struct Path {
    uint32_t prefix_;
    Leaf *leaf_;
  };
  explicit PathCache(const std::shared_ptr<TranslationDirectoryState> &owner)
      : owner_(owner), account_(owner->budget_.Account()) {}
  ~PathCache() {
    if (auto owner = owner_.lock()) {
      owner->budget_.Return(sizeof(PathCache));
    } else {
      account_->Release(sizeof(PathCache), false);
    }
  }
  auto Find(uint32_t prefix) -> Leaf * {
    for (size_t i = 0; i < size_; ++i) {
      if (paths_[i].prefix_ == prefix) {
        auto hit = paths_[i];
        for (size_t j = i; j != 0; --j) {
          paths_[j] = paths_[j - 1];
        }
        paths_[0] = hit;
        return hit.leaf_;
      }
    }
    return nullptr;
  }
  void Remember(uint32_t prefix, Leaf *leaf) {
    size_ = std::min(size_ + 1, paths_.size());
    for (size_t i = size_ - 1; i != 0; --i) {
      paths_[i] = paths_[i - 1];
    }
    paths_[0] = {prefix, leaf};
  }
  std::weak_ptr<TranslationDirectoryState> owner_;
  std::shared_ptr<ResourceAccount> account_;
  std::array<Path, 8> paths_{};
  size_t size_{0};
};
struct ThreadPaths {
  std::unique_ptr<PathCache> cache_;
  size_t calls_{0};
  auto For(const std::shared_ptr<TranslationDirectoryState> &owner) -> PathCache * {
    if (cache_ && cache_->owner_.lock() != owner) {
      cache_.reset();
    }
    if (!cache_ && owner->budget_.TryTake(sizeof(PathCache))) {
      try {
        cache_ = std::make_unique<PathCache>(owner);
      } catch (const std::bad_alloc &) {
        owner->budget_.Return(sizeof(PathCache));
        return nullptr;  // Optional cache; the bounded array lookup still works.
      }
    }
    return cache_.get();
  }
};
thread_local ThreadPaths thread_paths;
}  // namespace

TranslationDirectory::TranslationDirectory(const TranslationDirectoryOptions &options) : state_(nullptr) {
  // Check the fixed root allocation before allocating it; descendants reserve
  // their dynamic charges before allocation through Create().
  if (options.max_bytes_ < sizeof(TranslationDirectoryState)) {
    throw std::bad_alloc();
  }
  // Separate weak control block: an idle thread cache must not retain the large root allocation.
  state_ = std::shared_ptr<TranslationDirectoryState>(new TranslationDirectoryState(options));
}
TranslationDirectory::~TranslationDirectory() = default;

void TranslationDirectory::Maintain() {
  // The production caller holds no translation/frame rights here. Reclaim
  // failure is reported before starting page work, not halfway through eviction.
  auto owner = state_;
  thread_paths.For(owner);
  if (++thread_paths.calls_ % 64 == 0) {
    owner->Reclaim(16);
  }
}

void TranslationDirectory::Prefetch(const page_id_t *pages, size_t count) const {
  auto owner = state_;
  for (size_t i = 0; i < count; ++i) {
    if (pages[i] < 0) {
      throw std::invalid_argument("negative translation page id");
    }
    const auto prefix = static_cast<uint32_t>(pages[i]) >> 16;
    auto *middle = owner->roots_[prefix >> 7].published_.load(std::memory_order_acquire);
    if (!middle) {
      continue;
    }
    __builtin_prefetch(&middle->leaves_[prefix & 0x7fU], 0, 1);
    auto *leaf = middle->leaves_[prefix & 0x7fU].published_.load(std::memory_order_acquire);
    if (leaf) {
      __builtin_prefetch(leaf->memory_.Data() + (pages[i] & 0xffffU) * sizeof(TranslationEntry), 0, 1);
    }
  }
}

auto TranslationDirectory::Access(page_id_t page) -> TranslationAccess {
  if (page < 0) {
    throw std::invalid_argument("negative translation page id");
  }
  const auto id = static_cast<uint32_t>(page);
  auto owner = state_;
  auto *cache = thread_paths.For(owner);
  // One bounded pressure retry. Periodic work runs at the outer call boundary.
  for (bool retried = false;; retried = true) {
    try {
      const auto prefix = id >> 16;
      auto *leaf = cache ? cache->Find(prefix) : nullptr;
      if (!leaf) {
        leaf = owner->Resolve(prefix);
        if (cache) {
          cache->Remember(prefix, leaf);
        }
      }
      const auto suffix = id & 0xffffU;
      const auto group = suffix / leaf->entries_per_group_;
      auto gate = leaf->OpenGroup(group);
      return TranslationAccess(std::move(owner), id, &leaf->groups_[group], leaf->At(suffix), std::move(gate));
    } catch (const std::bad_alloc &) {
      if (retried) {
        throw;
      }
      const bool cached = cache != nullptr;
      thread_paths.cache_.reset();
      cache = nullptr;  // An optimization must not take the last bytes needed by a group.
      if (owner->Reclaim(64) == 0 && !cached) {
        throw;
      }
    }
  }
}

TranslationAccess::TranslationAccess(std::shared_ptr<TranslationDirectoryState> owner, uint32_t page,
                                     TranslationGroup *group, TranslationEntry *entry,
                                     std::shared_lock<std::shared_mutex> gate)
    : owner_(std::move(owner)), page_(page), group_(group), entry_(entry), gate_(std::move(gate)) {
  entry_->Lock();
}
TranslationAccess::~TranslationAccess() {
  if (entry_ != nullptr) {
    entry_->Unlock();
    if (group_->live_entries_.load(std::memory_order_relaxed) == 0) {
      owner_->Mark(page_);
    }
  }
  // Member order releases gate before the retained owner can be destroyed.
}
TranslationAccess::TranslationAccess(TranslationAccess &&other) noexcept
    : owner_(std::move(other.owner_)),
      page_(other.page_),
      group_(other.group_),
      entry_(std::exchange(other.entry_, nullptr)),
      gate_(std::move(other.gate_)) {}

auto TranslationAccess::Frame() const -> std::optional<frame_id_t> {
  const auto code = entry_->word_.load(std::memory_order_relaxed) & kFrameMask;
  if (code == 0) {
    return std::nullopt;
  }
  return static_cast<frame_id_t>(code - 1);
}
auto TranslationAccess::Version() const -> uint32_t {
  return (entry_->word_.load(std::memory_order_relaxed) >> 32) & kVersionMask;
}
void TranslationAccess::SetFrame(std::optional<frame_id_t> frame) {
  if (frame.has_value() && *frame < 0) {
    throw std::invalid_argument("negative translation frame id");
  }
  const auto code = frame.has_value() ? static_cast<uint64_t>(*frame) + 1 : 0;
  const auto old = entry_->word_.load(std::memory_order_relaxed);
  if ((old & kFrameMask) == code) {
    return;
  }
  const auto version = (((old >> 32) & kVersionMask) + 1) & kVersionMask;
  if ((old & kFrameMask) == 0) {
    group_->live_entries_.fetch_add(1, std::memory_order_relaxed);
  } else if (code == 0) {
    group_->live_entries_.fetch_sub(1, std::memory_order_relaxed);
  }
  entry_->word_.store(kLatchMask | (version << 32) | code, std::memory_order_relaxed);
}

}  // namespace bustub
