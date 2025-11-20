//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API,
  // 当完成startWorkerThread后删除此行
  /*throw NotImplementedException(
      "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the "
      "throw exception line in `disk_scheduler.cpp`.");
  */

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  //直接将请求放入请求队列，互斥锁由Channel管理
  request_queue_.Put(std::optional<DiskRequest>(std::move(r)));
}

void DiskScheduler::StartWorkerThread() {
  while(true) {
    //从channel中获取请求,注意返回的是optional类型
    auto request_opt = request_queue_.Get();
    if (!request_opt.has_value()) {
      // 如果获取到的请求是std::nullopt，表示需要退出循环
      break;
    }

    DiskRequest request = std::move(request_opt).value();
    if(request.is_write_) {
      // 如果是写请求，调用DiskManager的WritePage方法
      disk_manager_->WritePage(request.page_id_, request.data_);
    } else {
      // 如果是读请求，调用DiskManager的ReadPage方法
      disk_manager_->ReadPage(request.page_id_, request.data_);
    }
      request.callback_.set_value(true);
  }
}

}  // namespace bustub
