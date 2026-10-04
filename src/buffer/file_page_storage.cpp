#include <stdexcept>
#include "buffer/page_storage.h"
#include "storage/disk/disk_manager.h"
namespace bustub {
namespace {
class FilePages final : public PageStorage {
 public:
  explicit FilePages(DiskManager *disk) : disk_(disk) {
    if (!disk) {
      throw std::invalid_argument("file page backend requires a disk manager");
    }
  }
  auto MemoryAlignment() const -> size_t override { return alignof(std::max_align_t); }
  auto MaxBatchPages() const -> size_t override { return 64; }
  auto WriteDomain(page_id_t page) const -> uint64_t override { return page; }
  void EnsurePages(uint64_t count) override { disk_->IncreaseDiskSpace(count); }
  void Read(const PageBuffer &b) override { disk_->ReadPage(b.page_, b.data_); }
  void Write(const std::vector<PageBuffer> &pages) override {
    for (const auto &b : pages) {
      disk_->WritePage(b.page_, b.data_);
    }
  }
  void Delete(page_id_t page) override { disk_->DeletePage(page); }

 private:
  DiskManager *disk_;
};
}  // namespace
auto FilePageStorage(DiskManager *disk) -> std::shared_ptr<PageStorage> { return std::make_shared<FilePages>(disk); }
}  // namespace bustub
