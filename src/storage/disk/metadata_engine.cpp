#include "storage/disk/metadata_engine.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>
#include <limits>
#include <mutex>  // NOLINT(build/c++11)
#include <set>
#include <utility>

#include "common/byte_codec.h"
#include "common/rid.h"
#include "storage/disk/metadata_backend.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {
namespace {
constexpr size_t TRAILER = 64;
constexpr size_t BODY = BUSTUB_PAGE_SIZE - TRAILER;
constexpr size_t SLOT_BYTES = 16;
constexpr size_t RECORD_HEADER = 8;
constexpr size_t OVERFLOW_HEADER = 8;
constexpr page_id_t HEADER_PAGE = 0;
constexpr uint32_t FORMAT_VERSION = 1;
constexpr char FORMAT_MAGIC[] = "BUSTMETA";
constexpr uint32_t FULL = 1;
constexpr uint32_t PATCH = 2;
constexpr uint32_t CHECKPOINT = 2;

enum class PageKind : uint32_t { Root = 1, Tree = 2, Records = 3, Overflow = 4, Free = 5 };
using TreeKey = GenericKey<32>;
struct KeyCompare {
  auto operator()(const TreeKey &a, const TreeKey &b) const -> int {
    return std::memcmp(a.data_, b.data_, sizeof(a.data_));
  }
};
using Leaf = BPlusTreeLeafPage<TreeKey, RID, KeyCompare>;
using Internal = BPlusTreeInternalPage<TreeKey, page_id_t, KeyCompare>;
static_assert(sizeof(TreeKey) == 32 && sizeof(RID) == 8 && sizeof(page_id_t) == 4);
static_assert(sizeof(BPlusTreePage) == 12 && sizeof(Leaf) <= BUSTUB_PAGE_SIZE && sizeof(Internal) <= BUSTUB_PAGE_SIZE);
// Arrays retain their upstream layout; reserve trailer bytes in the value array.
constexpr int LEAF_LIMIT = (BUSTUB_PAGE_SIZE - 16) / (sizeof(TreeKey) + sizeof(RID)) - TRAILER / sizeof(RID);
constexpr int INTERNAL_LIMIT =
    (BUSTUB_PAGE_SIZE - 12) / (sizeof(TreeKey) + sizeof(page_id_t)) - TRAILER / sizeof(page_id_t);

void Require(bool condition, const char *message) {
  if (!condition) {
    throw MetadataError(MetadataErrorCode::Corrupt, message);
  }
}

// Runtime page fields are native; EncodeBody/DecodeBody are the sole disk codec.
template <class T>
auto Load(const char *data, size_t offset) -> T {
  T value;
  std::memcpy(&value, data + offset, sizeof(T));
  return value;
}
template <class T>
void Store(char *data, size_t offset, T value) {
  std::memcpy(data + offset, &value, sizeof(T));
}
auto EncodeKey(const MetadataKey &key) -> TreeKey {
  ByteWriter w;
  w.PutU64(key.category_);
  w.PutU64(key.owner_);
  w.PutU64(key.item_);
  TreeKey result{};
  std::memcpy(result.data_, w.Data().data(), w.Data().size());
  return result;
}
auto DecodeKey(const TreeKey &key) -> MetadataKey {
  ByteReader r(reinterpret_cast<const std::byte *>(key.data_), 24);
  return {r.ReadU64(), r.ReadU64(), r.ReadU64()};
}

struct PageBudget {
  explicit PageBudget(uint32_t limit) : limit_(limit) {}
  std::atomic<uint32_t> live_{0};
  uint32_t limit_;
};
struct PageImage {
  alignas(8) std::array<char, BUSTUB_PAGE_SIZE> data_{};
  PageKind kind_{PageKind::Tree};
  uint64_t generation_{1};
  uint64_t lsn_{0};
};
auto NewImage(const std::shared_ptr<PageBudget> &budget) -> std::shared_ptr<PageImage> {
  auto live = budget->live_.load();
  do {
    if (live >= budget->limit_) {
      throw MetadataError(MetadataErrorCode::ResourceUnavailable, "metadata live-page budget exhausted");
    }
  } while (!budget->live_.compare_exchange_weak(live, live + 1));
  PageImage *image;
  try {
    image = new PageImage();
  } catch (...) {
    budget->live_.fetch_sub(1);
    throw;
  }
  return {image, [budget](PageImage *p) {
            delete p;
            budget->live_.fetch_sub(1);
          }};
}
}  // namespace

struct MetadataVersion {
  std::vector<std::shared_ptr<const PageImage>> pages_;
};

namespace {
class MetadataPager {
 public:
  class ReadGuard {
   public:
    ReadGuard(page_id_t id, std::shared_ptr<const PageImage> page) : id_(id), page_(std::move(page)) {}
    template <class T>
    auto As() const -> const T * {
      return reinterpret_cast<const T *>(page_->data_.data());
    }
    auto GetPageId() const -> page_id_t { return id_; }

   private:
    page_id_t id_{INVALID_PAGE_ID};
    std::shared_ptr<const PageImage> page_;
  };
  class WriteGuard {
   public:
    WriteGuard() = default;
    WriteGuard(page_id_t id, std::shared_ptr<PageImage> page) : id_(id), page_(std::move(page)) {}
    template <class T>
    auto As() const -> const T * {
      return reinterpret_cast<const T *>(page_->data_.data());
    }
    template <class T>
    auto AsMut() -> T * {
      return reinterpret_cast<T *>(page_->data_.data());
    }
    auto GetPageId() const -> page_id_t { return id_; }
    void Drop() { page_.reset(); }

   private:
    page_id_t id_{INVALID_PAGE_ID};
    std::shared_ptr<PageImage> page_;
  };

  explicit MetadataPager(const MetadataVersion &view) : view_(view) {}
  MetadataPager(MetadataVersion &working, std::shared_ptr<PageBudget> budget, uint32_t limit)
      : view_(working), working_(&working), budget_(std::move(budget)), limit_(limit), dirty_(working.pages_.size()) {}

  auto Image(page_id_t id) const -> const PageImage & {
    Require(id >= 0 && static_cast<size_t>(id) < view_.pages_.size() && view_.pages_[id] != nullptr,
            "metadata page reference is invalid");
    return *view_.pages_[id];
  }
  auto ReadPage(page_id_t id) const -> ReadGuard {
    Require(Image(id).kind_ != PageKind::Free, "metadata read references a free page");
    return {id, view_.pages_[id]};
  }
  auto Mutable(page_id_t id) -> std::shared_ptr<PageImage> {
    if (working_ == nullptr) {
      throw std::logic_error("cannot modify a metadata snapshot");
    }
    const auto &old = Image(id);
    if (!dirty_[id]) {
      auto copy = NewImage(budget_);
      *copy = old;
      dirty_[id] = copy;
      working_->pages_[id] = copy;
    }
    return dirty_[id];
  }
  auto WritePage(page_id_t id) -> WriteGuard { return {id, Mutable(id)}; }
  auto NewPage() -> page_id_t { return Allocate(PageKind::Tree); }
  auto Allocate(PageKind kind) -> page_id_t {
    auto header = Mutable(HEADER_PAGE);
    auto free = Load<page_id_t>(header->data_.data(), 8);
    auto image = NewImage(budget_);
    image->kind_ = kind;
    page_id_t id;
    if (free != INVALID_PAGE_ID) {
      const auto &previous = Image(free);
      Require(previous.kind_ == PageKind::Free, "invalid metadata free list");
      Require(previous.generation_ != std::numeric_limits<uint64_t>::max(), "metadata generation exhausted");
      image->generation_ = previous.generation_ + 1;
      Store(header->data_.data(), 8, Load<page_id_t>(previous.data_.data(), 0));
      id = free;
      working_->pages_[id] = image;
      dirty_[id] = image;
    } else {
      if (working_->pages_.size() >= limit_) {
        throw MetadataError(MetadataErrorCode::ResourceUnavailable, "metadata page region is full");
      }
      id = static_cast<page_id_t>(working_->pages_.size());
      working_->pages_.push_back(image);
      dirty_.push_back(image);
      Store(header->data_.data(), 4, static_cast<uint32_t>(working_->pages_.size()));
    }
    return id;
  }
  auto DeletePage(page_id_t id) -> bool {
    Require(id != HEADER_PAGE && Image(id).kind_ != PageKind::Free, "invalid metadata page release");
    auto header = Mutable(HEADER_PAGE);
    auto image = Mutable(id);
    image->data_.fill(0);
    image->kind_ = PageKind::Free;
    Store(image->data_.data(), 0, Load<page_id_t>(header->data_.data(), 8));
    Store(header->data_.data(), 8, id);
    return true;
  }
  auto Dirty() const -> const std::vector<std::shared_ptr<PageImage>> & { return dirty_; }

 private:
  const MetadataVersion &view_;
  MetadataVersion *working_{nullptr};
  std::shared_ptr<PageBudget> budget_;
  uint32_t limit_{0};
  std::vector<std::shared_ptr<PageImage>> dirty_;
};
using Tree = BPlusTree<TreeKey, RID, KeyCompare, MetadataPager>;

auto TreeFor(MetadataPager *pager) -> Tree {
  return Tree("metadata", HEADER_PAGE, pager, KeyCompare{}, LEAF_LIMIT, INTERNAL_LIMIT, BPlusTreeOpenMode::Open);
}

// Slotted record pages store small values together; larger values use a page
// chain. Slots are stable within one published version, and tombstones reusable.
struct Slot {
  uint32_t offset_;
  uint32_t size_;
  page_id_t overflow_;
  uint32_t total_;
};
auto ReadSlot(const char *data, uint32_t slot) -> Slot {
  const size_t p = RECORD_HEADER + SLOT_BYTES * slot;
  return {Load<uint32_t>(data, p), Load<uint32_t>(data, p + 4), Load<page_id_t>(data, p + 8),
          Load<uint32_t>(data, p + 12)};
}
void WriteSlot(char *data, uint32_t slot, const Slot &value) {
  const size_t p = RECORD_HEADER + SLOT_BYTES * slot;
  Store(data, p, value.offset_);
  Store(data, p + 4, value.size_);
  Store(data, p + 8, value.overflow_);
  Store(data, p + 12, value.total_);
}
constexpr uint32_t DELETED = std::numeric_limits<uint32_t>::max();

auto ReadValue(const MetadataPager &pager, RID rid) -> std::vector<std::byte> {
  const auto &image = pager.Image(rid.GetPageId());
  Require(image.kind_ == PageKind::Records, "metadata value references a non-record page");
  const auto *data = image.data_.data();
  Require(rid.GetSlotNum() < Load<uint32_t>(data, 4), "metadata value slot is invalid");
  auto slot = ReadSlot(data, rid.GetSlotNum());
  Require(slot.total_ != DELETED, "metadata index references a deleted value");
  if (slot.overflow_ == INVALID_PAGE_ID) {
    Require(slot.offset_ <= BODY && slot.size_ <= BODY - slot.offset_ && slot.size_ == slot.total_,
            "invalid inline metadata value");
    auto begin = reinterpret_cast<const std::byte *>(data + slot.offset_);
    return {begin, begin + slot.size_};
  }
  std::vector<std::byte> result;
  result.reserve(slot.total_);
  auto next = slot.overflow_;
  while (next != INVALID_PAGE_ID) {
    const auto &page = pager.Image(next);
    Require(page.kind_ == PageKind::Overflow, "metadata value references a non-overflow page");
    auto length = Load<uint32_t>(page.data_.data(), 4);
    Require(length > 0 && length <= BODY - OVERFLOW_HEADER && length <= slot.total_ - result.size(),
            "invalid metadata overflow length or cycle");
    auto begin = reinterpret_cast<const std::byte *>(page.data_.data() + OVERFLOW_HEADER);
    result.insert(result.end(), begin, begin + length);
    next = Load<page_id_t>(page.data_.data(), 0);
  }
  Require(result.size() == slot.total_, "incomplete metadata overflow value");
  return result;
}

void RemoveValue(MetadataPager *pager, RID rid) {
  auto page = pager->Mutable(rid.GetPageId());
  auto slot = ReadSlot(page->data_.data(), rid.GetSlotNum());
  auto next = slot.overflow_;
  while (next != INVALID_PAGE_ID) {
    auto after = Load<page_id_t>(pager->Image(next).data_.data(), 0);
    pager->DeletePage(next);
    next = after;
  }
  WriteSlot(page->data_.data(), rid.GetSlotNum(), {0, 0, INVALID_PAGE_ID, DELETED});
  // Reclaim the directory tail without renumbering any live RID. Otherwise an
  // empty page formerly holding many small values cannot hold one large value.
  auto count = Load<uint32_t>(page->data_.data(), 4);
  while (count > 0 && ReadSlot(page->data_.data(), count - 1).total_ == DELETED) {
    --count;
  }
  Store(page->data_.data(), 4, count);
}

auto InsertValue(MetadataPager *pager, const std::vector<std::byte> &value) -> RID {
  // A value that cannot fit in an otherwise empty record page gets overflow.
  const size_t inline_bytes = value.size() <= BODY - RECORD_HEADER - SLOT_BYTES ? value.size() : 0;
  auto record = Load<page_id_t>(pager->Image(HEADER_PAGE).data_.data(), 12);
  uint32_t chosen = 0;
  while (record != INVALID_PAGE_ID) {
    const auto *data = pager->Image(record).data_.data();
    auto count = Load<uint32_t>(data, 4);
    size_t used = RECORD_HEADER + count * SLOT_BYTES;
    chosen = count;
    for (uint32_t i = 0; i < count; i++) {
      auto slot = ReadSlot(data, i);
      if (slot.total_ == DELETED) {
        chosen = i;
      } else {
        used += slot.size_;
      }
    }
    if (used + inline_bytes + (chosen == count ? SLOT_BYTES : 0) <= BODY) {
      break;
    }
    record = Load<page_id_t>(data, 0);
  }
  if (record == INVALID_PAGE_ID) {
    record = pager->Allocate(PageKind::Records);
    auto header = pager->Mutable(HEADER_PAGE);
    auto page = pager->Mutable(record);
    Store(page->data_.data(), 0, Load<page_id_t>(header->data_.data(), 12));
    Store(header->data_.data(), 12, record);
    chosen = 0;
  }
  auto page = pager->Mutable(record);
  auto previous = page->data_;
  auto count = Load<uint32_t>(previous.data(), 4);
  page->data_.fill(0);
  Store(page->data_.data(), 0, Load<page_id_t>(previous.data(), 0));
  Store(page->data_.data(), 4, count + (chosen == count ? 1U : 0U));
  size_t end = BODY;
  for (uint32_t i = 0; i < count; i++) {
    auto slot = ReadSlot(previous.data(), i);
    if (slot.total_ != DELETED) {
      end -= slot.size_;
      std::memcpy(page->data_.data() + end, previous.data() + slot.offset_, slot.size_);
      slot.offset_ = static_cast<uint32_t>(end);
    }
    WriteSlot(page->data_.data(), i, slot);
  }
  page_id_t overflow = INVALID_PAGE_ID;
  if (inline_bytes == value.size()) {
    end -= inline_bytes;
    if (!value.empty()) {
      std::memcpy(page->data_.data() + end, value.data(), value.size());
    }
  } else {
    size_t remaining = value.size();
    while (remaining > 0) {
      auto length = std::min(remaining, BODY - OVERFLOW_HEADER);
      auto id = pager->Allocate(PageKind::Overflow);
      auto part = pager->Mutable(id);
      Store(part->data_.data(), 0, overflow);
      Store(part->data_.data(), 4, static_cast<uint32_t>(length));
      std::memcpy(part->data_.data() + OVERFLOW_HEADER, value.data() + remaining - length, length);
      remaining -= length;
      overflow = id;
    }
  }
  WriteSlot(
      page->data_.data(), chosen,
      {static_cast<uint32_t>(end), static_cast<uint32_t>(inline_bytes), overflow, static_cast<uint32_t>(value.size())});
  return {record, chosen};
}

using PageBody = std::array<std::byte, BODY>;
void Put32(PageBody *body, size_t offset, uint32_t value) {
  for (size_t i = 0; i < 4; i++) {
    (*body)[offset + i] = static_cast<std::byte>((value >> ((3 - i) * 8)) & 0xffU);
  }
}
auto Get32(const PageBody &body, size_t offset) -> uint32_t {
  ByteReader r(body.data() + offset, 4);
  return r.ReadU32();
}

auto EncodeBody(const PageImage &image) -> PageBody {
  PageBody body{};
  const auto *data = image.data_.data();
  auto field = [&](size_t offset) { Put32(&body, offset, Load<uint32_t>(data, offset)); };
  switch (image.kind_) {
    case PageKind::Root:
      for (size_t p = 0; p < 16; p += 4) {
        field(p);
      }
      break;
    case PageKind::Tree: {
      const auto *base = reinterpret_cast<const BPlusTreePage *>(data);
      field(0);
      field(4);
      field(8);
      if (base->IsLeafPage()) {
        const auto *leaf = reinterpret_cast<const Leaf *>(data);
        field(12);
        constexpr size_t values = 16 + ((BUSTUB_PAGE_SIZE - 16) / 40) * 32;
        for (int i = 0; i < leaf->GetSize(); i++) {
          auto key = leaf->KeyAt(i);
          auto value = leaf->ValueAt(i);
          std::memcpy(body.data() + 16 + i * 32, key.data_, 32);
          Put32(&body, values + i * 8, static_cast<uint32_t>(value.GetPageId()));
          Put32(&body, values + i * 8 + 4, value.GetSlotNum());
        }
      } else {
        const auto *node = reinterpret_cast<const Internal *>(data);
        constexpr size_t values = 12 + ((BUSTUB_PAGE_SIZE - 12) / 36) * 32;
        for (int i = 0; i < node->GetSize(); i++) {
          if (i != 0) {
            auto key = node->KeyAt(i);
            std::memcpy(body.data() + 12 + i * 32, key.data_, 32);
          }
          Put32(&body, values + i * 4, static_cast<uint32_t>(node->ValueAt(i)));
        }
      }
      break;
    }
    case PageKind::Records: {
      field(0);
      field(4);
      auto count = Load<uint32_t>(data, 4);
      for (uint32_t i = 0; i < count; i++) {
        for (size_t j = 0; j < SLOT_BYTES; j += 4) {
          field(RECORD_HEADER + i * SLOT_BYTES + j);
        }
        auto slot = ReadSlot(data, i);
        if (slot.total_ != DELETED && slot.size_ != 0) {
          std::memcpy(body.data() + slot.offset_, data + slot.offset_, slot.size_);
        }
      }
      break;
    }
    case PageKind::Overflow: {
      field(0);
      field(4);
      auto length = Load<uint32_t>(data, 4);
      std::memcpy(body.data() + OVERFLOW_HEADER, data + OVERFLOW_HEADER, length);
      break;
    }
    case PageKind::Free:
      field(0);
      break;
  }
  return body;
}

void DecodeBody(const PageBody &body, uint32_t max_value, PageImage *image) {
  KeyCompare compare;
  auto *data = image->data_.data();
  image->data_.fill(0);
  auto field = [&](size_t p) { Store(data, p, Get32(body, p)); };
  switch (image->kind_) {
    case PageKind::Root:
      for (size_t p = 0; p < 16; p += 4) {
        field(p);
      }
      break;
    case PageKind::Tree: {
      auto type = Get32(body, 0);
      auto count = Get32(body, 4);
      Require(type == 1 || type == 2, "invalid metadata tree page type");
      auto capacity = type == 1 ? LEAF_LIMIT : INTERNAL_LIMIT;
      Require(
          count > 0 && count <= static_cast<uint32_t>(capacity) && Get32(body, 8) == static_cast<uint32_t>(capacity),
          "invalid metadata tree size");
      field(0);
      field(4);
      field(8);
      if (type == 1) {
        field(12);
        auto *leaf = reinterpret_cast<Leaf *>(data);
        constexpr size_t values = 16 + ((BUSTUB_PAGE_SIZE - 16) / 40) * 32;
        for (uint32_t i = 0; i < count; i++) {
          TreeKey key{};
          std::memcpy(key.data_, body.data() + 16 + i * 32, 32);
          Require(std::all_of(key.data_ + 24, key.data_ + 32, [](char c) { return c == 0; }),
                  "metadata key reserved bytes are nonzero");
          leaf->SetKeyAt(i, key);
          leaf->SetValueAt(i,
                           RID(static_cast<page_id_t>(Get32(body, values + i * 8)), Get32(body, values + i * 8 + 4)));
          Require(i == 0 || compare(leaf->KeyAt(i - 1), key) < 0, "unsorted metadata leaf");
        }
      } else {
        Require(count >= 2, "metadata internal page has no branching");
        auto *node = reinterpret_cast<Internal *>(data);
        constexpr size_t values = 12 + ((BUSTUB_PAGE_SIZE - 12) / 36) * 32;
        for (uint32_t i = 0; i < count; i++) {
          if (i != 0) {
            TreeKey key{};
            std::memcpy(key.data_, body.data() + 12 + i * 32, 32);
            Require(std::all_of(key.data_ + 24, key.data_ + 32, [](char c) { return c == 0; }),
                    "metadata key reserved bytes are nonzero");
            node->SetKeyAt(i, key);
            Require(i == 1 || compare(node->KeyAt(i - 1), key) < 0, "unsorted metadata separators");
          }
          node->SetValueAt(i, static_cast<page_id_t>(Get32(body, values + i * 4)));
        }
      }
      break;
    }
    case PageKind::Records: {
      auto count = Get32(body, 4);
      Require(count <= (BODY - RECORD_HEADER) / SLOT_BYTES, "invalid metadata slot count");
      field(0);
      field(4);
      for (uint32_t i = 0; i < count; i++) {
        for (size_t j = 0; j < SLOT_BYTES; j += 4) {
          field(RECORD_HEADER + i * SLOT_BYTES + j);
        }
        auto slot = ReadSlot(data, i);
        if (slot.total_ == DELETED) {
          Require(slot.size_ == 0 && slot.overflow_ == INVALID_PAGE_ID, "invalid deleted metadata slot");
          continue;
        }
        Require(slot.total_ <= max_value && slot.offset_ >= RECORD_HEADER + count * SLOT_BYTES &&
                    slot.offset_ <= BODY && slot.size_ <= BODY - slot.offset_,
                "invalid metadata slot range");
        Require(slot.overflow_ == INVALID_PAGE_ID ? slot.size_ == slot.total_ : slot.size_ == 0,
                "invalid metadata value location");
        std::memcpy(data + slot.offset_, body.data() + slot.offset_, slot.size_);
      }
      break;
    }
    case PageKind::Overflow: {
      auto length = Get32(body, 4);
      Require(length > 0 && length <= BODY - OVERFLOW_HEADER, "invalid metadata overflow size");
      field(0);
      field(4);
      std::memcpy(data + OVERFLOW_HEADER, body.data() + OVERFLOW_HEADER, length);
      break;
    }
    case PageKind::Free:
      field(0);
      break;
    default:
      throw MetadataError(MetadataErrorCode::Corrupt, "unknown metadata page kind");
  }
}

void Seal(PageImage *page, page_id_t id, uint64_t lsn, const PageBody &body) {
  page->lsn_ = lsn;
  std::array<std::byte, TRAILER> trailer{};
  std::memcpy(trailer.data(), FORMAT_MAGIC, 8);
  auto put = [&](size_t offset, uint64_t value, size_t bytes) {
    for (size_t i = 0; i < bytes; i++) {
      trailer[offset + i] = static_cast<std::byte>((value >> ((bytes - i - 1) * 8)) & 0xffU);
    }
  };
  put(8, FORMAT_VERSION, 4);
  put(12, static_cast<uint32_t>(page->kind_), 4);
  put(16, static_cast<uint32_t>(id), 4);
  put(20, BUSTUB_PAGE_SIZE, 4);
  put(24, page->generation_, 8);
  put(32, lsn, 8);
  auto crc = Crc32c(body.data(), body.size());
  put(40, Crc32cExtend(crc, trailer.data(), 40), 4);
  std::memcpy(page->data_.data() + BODY, trailer.data(), trailer.size());
}

void CheckMetadataAdmission(IOAdmission admission) {
  if (admission != IOAdmission::Accepted) {
    throw MetadataError(
        admission == IOAdmission::Full ? MetadataErrorCode::ResourceUnavailable : MetadataErrorCode::NotReady,
        "metadata page IO was not admitted");
  }
}

// F09 keeps page persistence separate from transaction publication. Access is
// serialized by Impl::writeback_mutex_; F02 parallelizes pages within a batch.
class MetadataPageWriter {
 public:
  MetadataPageWriter(MetadataBackend &backend, IOExecutor &executor) : backend_(backend), executor_(executor) {}

  auto Write(std::shared_ptr<const MetadataVersion> view, size_t max_pages) -> MetadataWritebackResult {
    durable_.resize(view->pages_.size());
    std::vector<MetadataPageRequest> requests;
    std::vector<Stamp> versions;
    auto limit = std::min(max_pages, view->pages_.size());
    requests.reserve(limit);
    versions.reserve(limit);
    for (size_t visited = 0; visited < view->pages_.size() && requests.size() < limit; visited++) {
      auto id = cursor_;
      cursor_ = (cursor_ + 1) % view->pages_.size();
      const auto &page = *view->pages_[id];
      if (durable_[id].generation_ != page.generation_ || durable_[id].lsn_ != page.lsn_) {
        requests.push_back({static_cast<page_id_t>(id), IOOperation::Write});
        versions.push_back({page.generation_, page.lsn_});
      }
    }
    if (requests.empty()) {
      return {MetadataWritebackOutcome::Clean, 0, nullptr};
    }
    auto preparation = backend_.TryPrepare(requests, true);
    CheckMetadataAdmission(preparation.admission_);
    auto &batch = *preparation.batch_;
    for (size_t i = 0; i < requests.size(); i++) {
      const auto &page = *view->pages_[requests[i].page_id_];
      auto body = EncodeBody(page);
      auto *buffer = batch.Buffer(i);
      std::memcpy(buffer, body.data(), BODY);
      std::memcpy(buffer + BODY, page.data_.data() + BODY, TRAILER);
    }
    // Canonical bytes now belong to F02. No need to retain unrelated old pages
    // while the device runs; their version stamps were captured before Submit.
    view.reset();
    CheckMetadataAdmission(executor_.TrySubmit(batch));
    batch.Wait();
    const auto &result = batch.Result();
    if (!result.writes_durable_) {
      auto error = result.flush_error_;
      for (const auto &operation : result.operations_) {
        if (operation.error_) {
          error = operation.error_;
          break;
        }
      }
      return {MetadataWritebackOutcome::Failed, requests.size(), error};
    }
    for (size_t i = 0; i < requests.size(); i++) {
      durable_[requests[i].page_id_] = versions[i];
    }
    return {MetadataWritebackOutcome::Durable, requests.size(), nullptr};
  }

 private:
  struct Stamp {
    uint64_t generation_{0};
    uint64_t lsn_{0};
  };
  MetadataBackend &backend_;
  IOExecutor &executor_;
  std::vector<Stamp> durable_;
  size_t cursor_{0};
};

struct MetadataBatchHeader {
  uint32_t kind_;
  uint64_t previous_;
  uint32_t pages_;
  uint32_t count_;
};

auto EncodeHeader(const MetadataBatchHeader &h, const MetadataOptions &options) -> std::vector<std::byte> {
  ByteWriter header;
  header.PutBytes(FORMAT_MAGIC, 8);
  header.PutU32(FORMAT_VERSION);
  header.PutU32(h.kind_);
  header.PutU32(BUSTUB_PAGE_SIZE);
  header.PutU32(24);
  header.PutU32(options.page_limit_);
  header.PutU32(options.max_value_bytes_);
  header.PutU64(h.previous_);
  header.PutU32(h.pages_);
  header.PutU32(h.count_);
  return header.Take();
}

auto ReadHeader(const JournalRecords &records, const MetadataOptions &options) -> MetadataBatchHeader {
  Require(!records.empty(), "missing metadata batch header");
  ByteReader header(records[0]);
  auto magic = header.ReadBytes(8);
  Require(std::memcmp(magic.data(), FORMAT_MAGIC, 8) == 0, "unknown metadata batch");
  const auto format = header.ReadU32();
  const auto kind = header.ReadU32();
  if (format != FORMAT_VERSION || header.ReadU32() != BUSTUB_PAGE_SIZE || header.ReadU32() != 24 ||
      header.ReadU32() != options.page_limit_ || header.ReadU32() != options.max_value_bytes_) {
    throw MetadataError(MetadataErrorCode::InvalidFormat, "metadata format/geometry does not match");
  }
  MetadataBatchHeader result{kind, header.ReadU64(), header.ReadU32(), header.ReadU32()};
  Require(header.Empty() && kind <= CHECKPOINT && result.pages_ > 0 && result.pages_ <= options.page_limit_ &&
              records.size() == result.count_ + 1ULL && (kind != CHECKPOINT || result.count_ == 0),
          "invalid metadata batch header");
  return result;
}

struct CheckpointRecoveryPlan {
  uint64_t covered_{0};
  uint32_t pages_{0};
  uint64_t last_data_{0};
  uint32_t high_water_{0};
  // These slots are rebuilt from the suffix, never trusted as checkpoint pages.
  std::vector<bool> modified_;
};

void InspectCheckpointBatch(uint64_t lsn, const JournalRecords &records, const MetadataOptions &options,
                            CheckpointRecoveryPlan *plan) {
  const auto h = ReadHeader(records, options);
  if (plan->covered_ == 0) {
    Require(h.kind_ == CHECKPOINT && h.previous_ > 0 && h.previous_ < lsn, "missing referenced metadata checkpoint");
    plan->covered_ = plan->last_data_ = h.previous_;
    plan->pages_ = plan->high_water_ = h.pages_;
    plan->modified_.resize(h.pages_);
    return;
  }
  Require(h.previous_ == plan->last_data_ && lsn > h.previous_ && h.pages_ >= plan->high_water_,
          "checkpoint suffix history mismatch");
  if (h.kind_ == CHECKPOINT) {
    Require(h.pages_ == plan->high_water_, "checkpoint changed page high-water");
    return;
  }
  Require(h.kind_ == 1, "metadata creation appears after checkpoint");
  plan->modified_.resize(h.pages_);
  for (size_t n = 1; n < records.size(); ++n) {
    ByteReader record(records[n]);
    record.ReadU32();
    const auto id = record.ReadU32();
    Require(id < h.pages_, "checkpoint suffix page is out of range");
    plan->modified_[id] = true;
  }
  plan->last_data_ = lsn;
  plan->high_water_ = h.pages_;
}

struct PreparedPages {
  JournalRecords records_;
  std::vector<std::pair<page_id_t, PageBody>> bodies_;
};

auto Prepare(const MetadataVersion *base, const MetadataVersion &working, const MetadataPager &pager,
             const MetadataOptions &options, uint64_t previous_lsn, uint64_t checkpoint_lsn) -> PreparedPages {
  PreparedPages prepared;
  prepared.records_.emplace_back();
  const auto &dirty = pager.Dirty();
  uint64_t bytes = 48;
  for (size_t id = 0; id < dirty.size(); id++) {
    if (!dirty[id]) {
      continue;
    }
    const auto &page = *dirty[id];
    auto body = EncodeBody(page);
    const PageImage *before = base != nullptr && id < base->pages_.size() ? base->pages_[id].get() : nullptr;
    PageBody old{};
    if (before != nullptr) {
      old = EncodeBody(*before);
      if (before->kind_ == page.kind_ && before->generation_ == page.generation_ && old == body) {
        continue;
      }
    }
    ByteWriter patch;
    uint32_t spans = 0;
    for (size_t start = 0; start < BODY;) {
      if (body[start] == old[start]) {
        start++;
        continue;
      }
      auto end = start + 1;
      while (end < BODY && body[end] != old[end]) {
        end++;
      }
      patch.PutU32(static_cast<uint32_t>(start));
      patch.PutU32(static_cast<uint32_t>(end - start));
      patch.PutBytes(body.data() + start, end - start);
      spans++;
      start = end;
    }
    bool full = before == nullptr || before->lsn_ <= checkpoint_lsn || before->kind_ != page.kind_ ||
                before->generation_ != page.generation_ || patch.Data().size() >= BODY;
    ByteWriter record;
    record.PutU32(full ? FULL : PATCH);
    record.PutU32(static_cast<uint32_t>(id));
    record.PutU32(static_cast<uint32_t>(page.kind_));
    record.PutU64(page.generation_);
    record.PutU64(before == nullptr ? 0 : before->generation_);
    record.PutU64(before == nullptr ? 0 : before->lsn_);
    record.PutU32(Crc32c(body.data(), BODY));
    record.PutU32(full ? 0 : spans);
    if (full) {
      record.PutBytes(body.data(), BODY);
    } else {
      record.PutBytes(patch.Data());
    }
    bytes += record.Data().size();
    if (bytes > options.max_batch_bytes_) {
      throw MetadataError(MetadataErrorCode::ResourceUnavailable, "metadata WAL batch budget exhausted");
    }
    prepared.records_.push_back(record.Take());
    prepared.bodies_.emplace_back(static_cast<page_id_t>(id), body);
  }
  prepared.records_[0] =
      EncodeHeader({base == nullptr ? 0U : 1U, previous_lsn, static_cast<uint32_t>(working.pages_.size()),
                    static_cast<uint32_t>(prepared.bodies_.size())},
                   options);
  return prepared;
}

// Validate references before exposing recovered state, so a damaged graph cannot
// turn a production lookup into an unbounded traversal.
void Validate(const MetadataVersion &version, const MetadataOptions &options) {
  KeyCompare compare;
  MetadataPager pager(version);
  Require(!version.pages_.empty() && pager.Image(0).kind_ == PageKind::Root, "missing metadata root");
  auto *header = pager.Image(0).data_.data();
  Require(Load<uint32_t>(header, 4) == version.pages_.size(), "metadata allocator high-water mismatch");
  std::set<page_id_t> visited{0};
  auto visit = [&](page_id_t id, PageKind kind) {
    Require(pager.Image(id).kind_ == kind && visited.insert(id).second, "metadata page kind/ownership/cycle mismatch");
  };
  auto free = Load<page_id_t>(header, 8);
  while (free != INVALID_PAGE_ID) {
    visit(free, PageKind::Free);
    free = Load<page_id_t>(pager.Image(free).data_.data(), 0);
  }
  std::set<uint64_t> live_values;
  auto records = Load<page_id_t>(header, 12);
  while (records != INVALID_PAGE_ID) {
    visit(records, PageKind::Records);
    const auto *data = pager.Image(records).data_.data();
    auto count = Load<uint32_t>(data, 4);
    for (uint32_t i = 0; i < count; i++) {
      auto slot = ReadSlot(data, i);
      if (slot.total_ == DELETED) {
        continue;
      }
      Require(slot.total_ <= options.max_value_bytes_, "metadata value exceeds format limit");
      live_values.insert(RID(records, i).Get());
      auto overflow = slot.overflow_;
      uint64_t total = slot.size_;
      while (overflow != INVALID_PAGE_ID) {
        visit(overflow, PageKind::Overflow);
        const auto *part = pager.Image(overflow).data_.data();
        total += Load<uint32_t>(part, 4);
        overflow = Load<page_id_t>(part, 0);
      }
      Require(total == slot.total_, "metadata value chain length mismatch");
    }
    records = Load<page_id_t>(data, 0);
  }
  std::vector<page_id_t> pending;
  auto root = Load<page_id_t>(header, 0);
  if (root != INVALID_PAGE_ID) {
    pending.push_back(root);
  }
  std::set<page_id_t> leaves;
  while (!pending.empty()) {
    auto id = pending.back();
    pending.pop_back();
    visit(id, PageKind::Tree);
    auto guard = pager.ReadPage(id);
    if (guard.As<BPlusTreePage>()->IsLeafPage()) {
      leaves.insert(id);
      auto *leaf = guard.As<Leaf>();
      for (int i = 0; i < leaf->GetSize(); i++) {
        Require(live_values.erase(leaf->ValueAt(i).Get()) == 1, "metadata index/value ownership mismatch");
      }
    } else {
      auto *node = guard.As<Internal>();
      for (int i = 0; i < node->GetSize(); i++) {
        pending.push_back(node->ValueAt(i));
      }
    }
  }
  Require(live_values.empty() && visited.size() == version.pages_.size(), "unreachable metadata pages/values");
  if (!leaves.empty()) {
    // Each leaf must occur exactly once in the next-leaf chain.
    std::set<page_id_t> linked;
    auto first = root;
    while (!reinterpret_cast<const BPlusTreePage *>(pager.Image(first).data_.data())->IsLeafPage()) {
      first = reinterpret_cast<const Internal *>(pager.Image(first).data_.data())->ValueAt(0);
    }
    for (auto id = first; id != INVALID_PAGE_ID;) {
      Require(leaves.count(id) != 0 && linked.insert(id).second, "metadata leaf link cycle or wrong target");
      id = reinterpret_cast<const Leaf *>(pager.Image(id).data_.data())->GetNextPageId();
    }
    Require(linked.size() == leaves.size(), "metadata leaf chain omits a subtree");
    auto tree = TreeFor(&pager);
    std::optional<TreeKey> previous;
    for (auto it = tree.Begin(); !it.IsEnd(); ++it) {
      auto entry = *it;
      Require(!previous || compare(*previous, entry.first) < 0, "metadata leaf chain order mismatch");
      previous = entry.first;
    }
  }
}
}  // namespace

struct MetadataEngine::Impl {
  Impl(BootstrapStore &bootstrap, const JournalIdentity &identity, const JournalOptions &journal_options,
       const MetadataOptions &options)
      : bootstrap_(bootstrap),
        journal_identity_(identity),
        executor_(*bootstrap.BindRegions().executor_),
        regions_(bootstrap),
        backend_(regions_),
        page_writer_(backend_, executor_),
        journal_(bootstrap, identity, journal_options),
        options_(options),
        budget_(std::make_shared<PageBudget>(options.max_live_pages_)) {
    if (options.page_limit_ < 4 || options.page_limit_ > backend_.PageCapacity() ||
        options.page_limit_ > static_cast<uint32_t>(std::numeric_limits<page_id_t>::max()) ||
        options.max_live_pages_ < 2 || options.max_value_bytes_ >= DELETED || options.max_value_bytes_ == 0 ||
        options.max_batch_bytes_ < BODY + 128 || journal_options.max_record_bytes_ < BODY + 44 ||
        journal_options.max_records_per_batch_ < 2 || journal_options.max_batch_bytes_ < BODY + 92) {
      throw std::invalid_argument("invalid metadata capacity or admission limits");
    }
  }

  void Replay(uint64_t lsn, const JournalRecords &records) {
    const auto h = ReadHeader(records, options_);
    const auto pages = h.pages_;
    Require(h.previous_ == lsn_ && lsn > lsn_, "metadata batch history mismatch");
    if (h.kind_ == CHECKPOINT) {
      Require(published_ && h.pages_ == published_->pages_.size(), "checkpoint has no matching metadata state");
      return;
    }
    Require(h.kind_ == (published_ ? 1U : 0U), "metadata creation/transaction order mismatch");
    auto next = published_ ? std::make_shared<MetadataVersion>(*published_) : std::make_shared<MetadataVersion>();
    Require(pages >= next->pages_.size(), "metadata page high-water went backwards");
    next->pages_.resize(pages);
    std::set<uint32_t> changed;
    for (size_t i = 1; i < records.size(); i++) {
      ByteReader record(records[i]);
      auto type = record.ReadU32();
      auto id = record.ReadU32();
      auto image = NewImage(budget_);
      image->kind_ = static_cast<PageKind>(record.ReadU32());
      image->generation_ = record.ReadU64();
      auto base_generation = record.ReadU64();
      auto base_lsn = record.ReadU64();
      auto crc = record.ReadU32();
      auto spans = record.ReadU32();
      Require(id < pages && changed.insert(id).second && image->generation_ != 0, "invalid metadata page record");
      auto before = next->pages_[id];
      const bool from_checkpoint = restoring_checkpoint_ && id < checkpoint_pages_ && !before;
      Require(from_checkpoint ? type == FULL && base_generation > 0 && base_lsn > 0 && base_lsn <= checkpoint_lsn_
                              : (before ? before->generation_ == base_generation && before->lsn_ == base_lsn
                                        : base_generation == 0 && base_lsn == 0),
              "metadata redo base is missing");
      PageBody body{};
      if (type == FULL) {
        Require(spans == 0 && record.Remaining() == BODY, "invalid metadata full image");
        auto bytes = record.ReadBytes(BODY);
        std::copy(bytes.begin(), bytes.end(), body.begin());
      } else {
        Require(type == PATCH && before && before->generation_ == image->generation_ && before->kind_ == image->kind_ &&
                    spans > 0,
                "invalid metadata patch basis");
        body = EncodeBody(*before);
        size_t end = 0;
        for (uint32_t n = 0; n < spans; n++) {
          auto offset = record.ReadU32();
          auto size = record.ReadU32();
          Require(offset >= end && offset <= BODY && size > 0 && size <= BODY - offset, "invalid metadata patch range");
          auto bytes = record.ReadBytes(size);
          std::copy(bytes.begin(), bytes.end(), body.begin() + offset);
          end = offset + size;
        }
      }
      Require(record.Empty() && crc == Crc32c(body.data(), BODY), "metadata reconstructed page checksum mismatch");
      DecodeBody(body, options_.max_value_bytes_, image.get());
      Seal(image.get(), static_cast<page_id_t>(id), lsn, body);
      next->pages_[id] = std::move(image);
    }
    if (!restoring_checkpoint_) {
      for (const auto &page : next->pages_) {
        Require(page != nullptr, "metadata page allocation lacks a full image");
      }
    }
    published_ = std::move(next);
    lsn_ = lsn;
  }

  void LoadCheckpoint(const CheckpointRecoveryPlan &plan) {
    Require(plan.covered_ != 0, "checkpoint reference points to an empty Journal tail");
    auto next = std::make_shared<MetadataVersion>();
    next->pages_.resize(plan.pages_);
    for (uint32_t id = 0; id < plan.pages_; ++id) {
      if (plan.modified_[id]) {
        continue;
      }
      auto preparation = backend_.TryPrepare({{static_cast<page_id_t>(id), IOOperation::Read}}, false);
      CheckMetadataAdmission(preparation.admission_);
      auto &batch = *preparation.batch_;
      CheckMetadataAdmission(executor_.TrySubmit(batch));
      batch.Wait();
      const auto &result = batch.Result().operations_.front();
      if (result.error_) {
        std::rethrow_exception(result.error_);
      }
      Require(result.outcome_ == IOOutcome::Succeeded, "checkpoint page read did not complete");
      const auto *bytes = reinterpret_cast<const std::byte *>(batch.Buffer(0));
      PageBody body{};
      std::copy(bytes, bytes + BODY, body.begin());
      ByteReader trailer(bytes + BODY, TRAILER);
      const auto magic = trailer.ReadBytes(8);
      Require(std::memcmp(magic.data(), FORMAT_MAGIC, 8) == 0 && trailer.ReadU32() == FORMAT_VERSION,
              "checkpoint page format mismatch");
      auto page = NewImage(budget_);
      page->kind_ = static_cast<PageKind>(trailer.ReadU32());
      Require(trailer.ReadU32() == id && trailer.ReadU32() == BUSTUB_PAGE_SIZE, "checkpoint page identity mismatch");
      page->generation_ = trailer.ReadU64();
      const auto page_lsn = trailer.ReadU64();
      Require(page->generation_ > 0 && page_lsn > 0 && page_lsn <= plan.covered_ &&
                  trailer.ReadU32() == Crc32cExtend(Crc32c(body.data(), BODY), bytes + BODY, 40),
              "checkpoint page checksum or version mismatch");
      const auto reserved = trailer.ReadBytes(trailer.Remaining());
      Require(std::all_of(reserved.begin(), reserved.end(), [](std::byte b) { return b == std::byte{0}; }),
              "unsupported checkpoint page trailer");
      DecodeBody(body, options_.max_value_bytes_, page.get());
      Seal(page.get(), static_cast<page_id_t>(id), page_lsn, body);
      next->pages_[id] = std::move(page);
    }
    checkpoint_lsn_ = lsn_ = plan.covered_;
    checkpoint_pages_ = plan.pages_;
    restoring_checkpoint_ = true;
    published_ = std::move(next);
  }

  auto Publish(const MetadataVersion *base, std::shared_ptr<MetadataVersion> next, MetadataPager *pager)
      -> JournalResult {
    auto prepared = Prepare(base, *next, *pager, options_, lsn_, checkpoint_lsn_);
    if (base != nullptr) {
      std::set<page_id_t> changed;
      for (const auto &entry : prepared.bodies_) {
        changed.insert(entry.first);
      }
      for (size_t id = 0; id < base->pages_.size(); id++) {
        if (changed.count(static_cast<page_id_t>(id)) == 0) {
          next->pages_[id] = base->pages_[id];
        }
      }
    }
    auto submission = journal_.TryAppend(prepared.records_);
    if (submission.admission_ != JournalAdmission::Accepted) {
      throw MetadataError(MetadataErrorCode::ResourceUnavailable, "metadata Journal did not admit this batch");
    }
    submission.ticket_->Wait();
    auto result = submission.ticket_->Result();
    if (result.outcome_ != JournalOutcome::Durable) {
      std::lock_guard<std::mutex> lock(view_mutex_);
      ready_ = false;
      return result;
    }
    // No allocation after durability: all images, directory and body buffers
    // already exist. Fill the actual F07 LSN; it was not guessed before append.
    for (const auto &entry : prepared.bodies_) {
      auto *page = pager->Dirty()[entry.first].get();
      Seal(page, entry.first, result.begin_, entry.second);
    }
    {
      std::lock_guard<std::mutex> lock(view_mutex_);
      published_ = std::move(next);
      lsn_ = result.begin_;
      ready_ = true;
    }
    return result;
  }

  BootstrapStore &bootstrap_;
  JournalIdentity journal_identity_;
  IOExecutor &executor_;
  RegionManager regions_;
  MetadataBackend backend_;
  MetadataPageWriter page_writer_;
  JournalService journal_;
  MetadataOptions options_;
  std::shared_ptr<PageBudget> budget_;
  std::mutex writer_mutex_;
  std::mutex writeback_mutex_;
  mutable std::mutex view_mutex_;
  std::shared_ptr<const MetadataVersion> published_;
  uint64_t lsn_{0};
  uint64_t checkpoint_lsn_{0};
  uint32_t checkpoint_pages_{0};
  bool restoring_checkpoint_{false};
  bool checkpointing_{false};
  bool ready_{false};
  bool started_{false};
};

MetadataSnapshot::MetadataSnapshot(std::shared_ptr<const MetadataVersion> version) : version_(std::move(version)) {}
auto MetadataSnapshot::Get(const MetadataKey &key) const -> std::optional<std::vector<std::byte>> {
  MetadataPager pager(*version_);
  auto tree = TreeFor(&pager);
  std::vector<RID> result;
  if (!tree.GetValue(EncodeKey(key), &result)) {
    return std::nullopt;
  }
  return ReadValue(pager, result.front());
}
auto MetadataSnapshot::Scan(const MetadataKey &lower, size_t limit) const -> std::vector<MetadataEntry> {
  if (limit == 0) {
    throw std::invalid_argument("metadata scan limit must be positive");
  }
  MetadataPager pager(*version_);
  auto tree = TreeFor(&pager);
  std::vector<MetadataEntry> result;
  for (auto it = tree.Begin(EncodeKey(lower)); !it.IsEnd() && result.size() < limit; ++it) {
    auto entry = *it;
    result.push_back({DecodeKey(entry.first), ReadValue(pager, entry.second)});
  }
  return result;
}
MetadataEngine::MetadataEngine(BootstrapStore &bootstrap, const JournalIdentity &identity,
                               const JournalOptions &journal_options, const MetadataOptions &options)
    : impl_(std::make_unique<Impl>(bootstrap, identity, journal_options, options)) {}
MetadataEngine::~MetadataEngine() { Close(); }
void MetadataEngine::Create() {
  auto &s = *impl_;
  std::lock_guard<std::mutex> writer(s.writer_mutex_);
  if (s.started_) {
    throw MetadataError(MetadataErrorCode::NotReady, "metadata engine already started");
  }
  s.started_ = true;
  auto next = std::make_shared<MetadataVersion>();
  auto header = NewImage(s.budget_);
  header->kind_ = PageKind::Root;
  Store(header->data_.data(), 0, INVALID_PAGE_ID);
  Store(header->data_.data(), 4, uint32_t{1});
  Store(header->data_.data(), 8, INVALID_PAGE_ID);
  Store(header->data_.data(), 12, INVALID_PAGE_ID);
  next->pages_.push_back(header);
  MetadataPager pager(*next, s.budget_, s.options_.page_limit_);
  pager.Mutable(0);
  s.journal_.Create();
  auto result = s.Publish(nullptr, next, &pager);
  if (result.outcome_ != JournalOutcome::Durable) {
    if (result.error_) {
      std::rethrow_exception(result.error_);
    }
    throw MetadataError(MetadataErrorCode::NotReady, "metadata creation was not confirmed durable");
  }
}
void MetadataEngine::Open() {
  auto &s = *impl_;
  std::lock_guard<std::mutex> writer(s.writer_mutex_);
  if (s.started_) {
    throw MetadataError(MetadataErrorCode::NotReady, "metadata engine already started");
  }
  s.started_ = true;
  try {
    const auto checkpoint = s.bootstrap_.MetadataCheckpoint();
    if (checkpoint) {
      Require(checkpoint->journal_ == s.journal_identity_, "checkpoint belongs to a different Journal");
      CheckpointRecoveryPlan plan;
      s.journal_.OpenFrom(
          checkpoint->position_,
          [&](uint64_t lsn, const JournalRecords &records) { InspectCheckpointBatch(lsn, records, s.options_, &plan); },
          [&](uint64_t lsn, const JournalRecords &records) {
            if (lsn == checkpoint->position_) {
              s.LoadCheckpoint(plan);
            } else {
              s.Replay(lsn, records);
            }
          });
      Require(plan.covered_ != 0, "checkpoint reference has no complete batch");
    } else {
      s.journal_.Open([&](uint64_t lsn, const JournalRecords &records) { s.Replay(lsn, records); });
    }
    Require(s.published_ != nullptr, "Journal has no committed metadata format");
    Validate(*s.published_, s.options_);
    s.restoring_checkpoint_ = false;
    std::lock_guard<std::mutex> view(s.view_mutex_);
    s.ready_ = true;
  } catch (...) {
    s.journal_.Close();
    s.published_.reset();
    throw;
  }
}
auto MetadataEngine::Read() const -> MetadataSnapshot {
  std::lock_guard<std::mutex> view(impl_->view_mutex_);
  if (!impl_->ready_) {
    throw MetadataError(MetadataErrorCode::NotReady, "metadata engine is not ready");
  }
  return MetadataSnapshot(impl_->published_);
}
auto MetadataEngine::Commit(const MetadataSnapshot &base, const std::vector<MetadataMutation> &mutations)
    -> JournalResult {
  auto &s = *impl_;
  std::lock_guard<std::mutex> writer(s.writer_mutex_);
  {
    std::lock_guard<std::mutex> view(s.view_mutex_);
    if (!s.ready_) {
      throw MetadataError(MetadataErrorCode::NotReady, "metadata engine is not ready");
    }
    if (s.checkpointing_) {
      throw MetadataError(MetadataErrorCode::ResourceUnavailable, "metadata checkpoint is excluding modifications");
    }
    if (base.version_ != s.published_) {
      throw MetadataError(MetadataErrorCode::Conflict, "stale or foreign metadata view");
    }
  }
  if (mutations.empty()) {
    throw std::invalid_argument("metadata transaction must be nonempty");
  }
  uint64_t bytes = 0;
  for (const auto &mutation : mutations) {
    auto size = mutation.value_ ? mutation.value_->size() : 0;
    if (size > s.options_.max_value_bytes_ || size > s.options_.max_batch_bytes_ - bytes) {
      throw MetadataError(MetadataErrorCode::ResourceUnavailable, "metadata input budget exceeded");
    }
    bytes += size;
    if (s.options_.max_batch_bytes_ - bytes < 32) {
      throw MetadataError(MetadataErrorCode::ResourceUnavailable, "metadata operation budget exceeded");
    }
    bytes += 32;
  }
  auto next = std::make_shared<MetadataVersion>(*base.version_);
  MetadataPager pager(*next, s.budget_, s.options_.page_limit_);
  auto tree = TreeFor(&pager);
  for (const auto &mutation : mutations) {
    auto key = EncodeKey(mutation.key_);
    std::vector<RID> old;
    if (tree.GetValue(key, &old)) {
      tree.Remove(key);
      RemoveValue(&pager, old.front());
    }
    if (mutation.value_) {
      auto rid = InsertValue(&pager, *mutation.value_);
      Require(tree.Insert(key, rid), "metadata key unexpectedly remained after replacement");
    }
  }
  return s.Publish(base.version_.get(), next, &pager);
}
auto MetadataEngine::Writeback(size_t max_pages) -> MetadataWritebackResult {
  if (max_pages == 0) {
    throw std::invalid_argument("metadata writeback limit must be positive");
  }
  auto &s = *impl_;
  std::unique_lock<std::mutex> writeback(s.writeback_mutex_, std::try_to_lock);
  if (!writeback.owns_lock()) {
    throw MetadataError(MetadataErrorCode::ResourceUnavailable, "metadata writeback is already active");
  }
  std::shared_ptr<const MetadataVersion> view;
  {
    std::lock_guard<std::mutex> lock(s.view_mutex_);
    if (!s.ready_) {
      throw MetadataError(MetadataErrorCode::NotReady, "metadata engine is not ready");
    }
    view = s.published_;
  }
  return s.page_writer_.Write(std::move(view), max_pages);
}
auto MetadataEngine::Checkpoint(size_t max_pages) -> MetadataCheckpointResult {
  if (max_pages == 0) {
    throw std::invalid_argument("metadata checkpoint page limit must be positive");
  }
  auto &s = *impl_;
  std::unique_lock<std::mutex> writer(s.writer_mutex_);
  {
    std::lock_guard<std::mutex> view(s.view_mutex_);
    if (!s.ready_) {
      throw MetadataError(MetadataErrorCode::NotReady, "metadata engine is not ready");
    }
    if (s.checkpointing_) {
      throw MetadataError(MetadataErrorCode::ResourceUnavailable, "metadata checkpoint is already active");
    }
  }
  // Drain an admitted F09 batch before fixing the checkpoint. It never needs
  // writer_mutex_. Close uses this same lock order.
  std::unique_lock<std::mutex> writeback(s.writeback_mutex_);
  std::shared_ptr<const MetadataVersion> version;
  {
    std::lock_guard<std::mutex> view(s.view_mutex_);
    version = s.published_;
    s.checkpointing_ = true;
  }
  struct ReleaseGate {
    Impl &s_;
    ~ReleaseGate() {
      std::lock_guard<std::mutex> view(s_.view_mutex_);
      s_.checkpointing_ = false;
    }
  } release{s};
  const auto covered = s.lsn_;
  writer.unlock();
  JournalRecords records{
      EncodeHeader({CHECKPOINT, covered, static_cast<uint32_t>(version->pages_.size()), 0}, s.options_)};
  for (;;) {
    const auto written = s.page_writer_.Write(version, max_pages);
    if (written.outcome_ == MetadataWritebackOutcome::Failed) {
      return {MetadataCheckpointOutcome::NotPublished, written.error_};
    }
    if (written.outcome_ == MetadataWritebackOutcome::Clean) {
      break;
    }
  }
  auto submission = s.journal_.TryAppend(records);
  if (submission.admission_ != JournalAdmission::Accepted) {
    throw MetadataError(MetadataErrorCode::ResourceUnavailable, "checkpoint Journal admission failed");
  }
  submission.ticket_->Wait();
  const auto result = submission.ticket_->Result();
  if (result.outcome_ != JournalOutcome::Durable) {
    std::lock_guard<std::mutex> view(s.view_mutex_);
    s.ready_ = false;
    return {MetadataCheckpointOutcome::NotPublished, result.error_};
  }
  try {
    s.bootstrap_.PublishMetadataCheckpoint({s.journal_identity_, result.begin_});
  } catch (...) {
    std::lock_guard<std::mutex> view(s.view_mutex_);
    s.ready_ = false;
    return {MetadataCheckpointOutcome::Indeterminate, std::current_exception()};
  }
  // No subsequent Commit can run before this cycle is installed.
  s.checkpoint_lsn_ = covered;
  return {MetadataCheckpointOutcome::Durable, nullptr};
}

void MetadataEngine::Close() {
  if (!impl_) {
    return;
  }
  std::lock_guard<std::mutex> writer(impl_->writer_mutex_);
  std::lock_guard<std::mutex> writeback(impl_->writeback_mutex_);
  {
    std::lock_guard<std::mutex> view(impl_->view_mutex_);
    impl_->ready_ = false;
  }
  impl_->journal_.Close();
  impl_->started_ = true;
}
}  // namespace bustub
