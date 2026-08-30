//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// table_heap_reopen_test.cpp
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"
#include "storage/table/table_heap.h"
#include "type/value_factory.h"

namespace bustub {

// M0-T01: recovery reopens the persisted page chain; it must not allocate a new first page.
TEST(TableHeapReopenTest, ReopensExistingMultiPageHeap) {
  const auto stem = "bustub-table-reopen-" + std::to_string(getpid());
  const auto db_path = std::filesystem::temp_directory_path() / (stem + ".bustub");
  const auto log_path = std::filesystem::temp_directory_path() / (stem + ".log");
  std::filesystem::remove(db_path);
  std::filesystem::remove(log_path);

  const Schema schema({Column("id", TypeId::INTEGER), Column("payload", TypeId::VARCHAR, 128)});
  page_id_t first_page_id = INVALID_PAGE_ID;
  constexpr size_t tuple_count = 300;
  {
    DiskManager disk(db_path);
    BufferPoolManager bpm(16, &disk);
    TableHeap table(&bpm);
    first_page_id = table.GetFirstPageId();
    for (size_t i = 0; i < tuple_count; i++) {
      Tuple tuple({ValueFactory::GetIntegerValue(static_cast<int32_t>(i)),
                   ValueFactory::GetVarcharValue(std::string(96, static_cast<char>('a' + (i % 26))))},
                  &schema);
      ASSERT_TRUE(table.InsertTuple({static_cast<timestamp_t>(i + 1), false}, tuple).has_value());
    }
    bpm.FlushAllPages();
    disk.ShutDown();
  }

  {
    DiskManager disk(db_path);
    BufferPoolManager bpm(8, &disk);
    auto table = TableHeap::Open(&bpm, first_page_id);
    EXPECT_EQ(table->GetFirstPageId(), first_page_id);
    size_t count = 0;
    for (auto iterator = table->MakeIterator(); !iterator.IsEnd(); ++iterator) {
      auto [meta, tuple] = iterator.GetTuple();
      EXPECT_EQ(meta.ts_, static_cast<timestamp_t>(count + 1));
      EXPECT_EQ(tuple.GetValue(&schema, 0).GetAs<int32_t>(), static_cast<int32_t>(count));
      count++;
    }
    EXPECT_EQ(count, tuple_count);
    disk.ShutDown();
  }

  std::filesystem::remove(db_path);
  std::filesystem::remove(log_path);
}

}  // namespace bustub
