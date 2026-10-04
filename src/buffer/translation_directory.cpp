#include "buffer/translation_directory.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <mutex>
#include <new>
#include <thread>
#include <utility>

#include "buffer_memory.h"

namespace bustub {

namespace {
constexpr size_t kLeafEntries = 1U << 16;
constexpr uint64_t kFrameMask = 0xffffffffULL;
constexpr uint64_t kVersionMask = 0xffffffULL;
constexpr uint64_t kLatchMask = 0xff00000000000000ULL;

class Budget {
 public:
  explicit Budget(size_t limit) : limit_(limit) {}
  void Take(size_t bytes) {
    auto used = used_.load(std::memory_order_relaxed);
    do {
      if (bytes > limit_ - used) {
        throw std::bad_alloc();
      }
    } while (!used_.compare_exchange_weak(used, used + bytes, std::memory_order_relaxed));
  }
  void Return(size_t bytes) { used_.fetch_sub(bytes, std::memory_order_relaxed); }

 private:
  size_t limit_;
  std::atomic<size_t> used_{0};
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
    Charge charge(budget, buffer_memory::Add(sizeof(Leaf),
                                             buffer_memory::Multiply(GroupCount(os_page), sizeof(TranslationGroup))));
    return std::make_unique<Leaf>(std::move(charge), budget, os_page);
  }
  Leaf(Charge charge, Budget *budget, size_t os_page)
      : control_charge_(std::move(charge)),
        budget_(budget),
        page_bytes_(os_page),
        entries_per_group_(os_page / sizeof(TranslationEntry)),
        memory_(kLeafEntries * sizeof(TranslationEntry), os_page, os_page),
        groups_(std::make_unique<TranslationGroup[]>(GroupCount(os_page))) {
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
  auto OpenGroup(size_t index) -> std::shared_lock<std::shared_mutex> {
    auto &group = groups_[index];
    std::shared_lock read(group.gate_);
    if (group.constructed_) {
      return read;
    }
    read.unlock();
    {
      std::unique_lock write(group.gate_);
      if (!group.constructed_) {
        budget_->Take(page_bytes_);
        // Atomics have explicit lifetime, and construction here cannot throw.
        const auto end = std::min((index + 1) * entries_per_group_, kLeafEntries);
        for (size_t i = index * entries_per_group_; i < end; ++i) {
          new (memory_.Data() + i * sizeof(TranslationEntry)) TranslationEntry();
        }
        group.constructed_ = true;
      }
    }
    read.lock();
    return read;
  }

  Charge control_charge_;
  Budget *budget_;
  size_t page_bytes_;
  size_t entries_per_group_;
  buffer_memory::Region memory_;
  std::unique_ptr<TranslationGroup[]> groups_;
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
    Charge charge(budget, sizeof(Middle));
    return std::make_unique<Middle>(std::move(charge));
  }
  explicit Middle(Charge charge) : charge_(std::move(charge)) {}
  Charge charge_;
  std::array<Slot<Leaf>, 128> leaves_;
};
}  // namespace

struct TranslationDirectoryState {
  explicit TranslationDirectoryState(size_t limit)
      : budget_(limit), charge_(&budget_, sizeof(TranslationDirectoryState)), os_page_(buffer_memory::PageBytes()) {
    if (os_page_ % sizeof(TranslationEntry) != 0) {
      throw std::runtime_error("OS page cannot hold aligned translation groups");
    }
  }
  Budget budget_;
  Charge charge_;
  size_t os_page_;
  std::array<Slot<Middle>, 256> roots_;
};

TranslationDirectory::TranslationDirectory(const TranslationDirectoryOptions &options) : state_(nullptr) {
  // Check the fixed root allocation before allocating it; descendants reserve
  // their dynamic charges before allocation through Create().
  if (options.max_bytes_ < sizeof(TranslationDirectoryState)) {
    throw std::bad_alloc();
  }
  state_ = std::make_shared<TranslationDirectoryState>(options.max_bytes_);
}
TranslationDirectory::~TranslationDirectory() = default;

auto TranslationDirectory::Access(page_id_t page) -> TranslationAccess {
  if (page < 0) {
    throw std::invalid_argument("negative translation page id");
  }
  const auto id = static_cast<uint32_t>(page);
  auto owner = state_;
  auto *middle = owner->roots_[id >> 23].Get(&owner->budget_);
  auto *leaf = middle->leaves_[(id >> 16) & 0x7fU].Get(&owner->budget_, owner->os_page_);
  const auto suffix = id & 0xffffU;
  const auto group = suffix / leaf->entries_per_group_;
  auto gate = leaf->OpenGroup(group);
  return TranslationAccess(std::move(owner), &leaf->groups_[group], leaf->At(suffix), std::move(gate));
}

TranslationAccess::TranslationAccess(std::shared_ptr<TranslationDirectoryState> owner, TranslationGroup *group,
                                     TranslationEntry *entry, std::shared_lock<std::shared_mutex> gate)
    : owner_(std::move(owner)), group_(group), entry_(entry), gate_(std::move(gate)) {
  entry_->Lock();
}
TranslationAccess::~TranslationAccess() {
  if (entry_ != nullptr) {
    entry_->Unlock();
  }
  // Member order releases gate before the retained owner can be destroyed.
}
TranslationAccess::TranslationAccess(TranslationAccess &&other) noexcept
    : owner_(std::move(other.owner_)),
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
