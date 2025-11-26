//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  //初始化全局时间戳为0，当前可回收帧数量为0
  current_timestamp_ = 0;
  curr_size_ = 0;
}
//找出应被淘汰的frame（具有最大向后k距离的frame），没有就返回std::nullopt
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  // 加锁
  std::lock_guard<std::mutex> lock(latch_);

  // 遍历 node_store_，找出具有最大向后 k 距离的 frame
  frame_id_t evict_frame_id = -1;
  size_t max_k_distance = 0;
  bool has_infinite_distance = false;
  auto earliest_access_time_of_low_frequency_frames = std::numeric_limits<size_t>::max();

  for (const auto &pair : node_store_) {
    const LRUKNode &node = pair.second;
    if (!node.IsEvictable()) {
      // 如果 frame 不可回收，跳过
      continue;
    }

    size_t k_distance = 0;
    const auto &history = node.GetHistory();

    if (history.size() < k_) {
      // 如果访问历史少于 k 次，向后 k 距离为无穷大
      k_distance = std::numeric_limits<size_t>::max();
      if (!has_infinite_distance) {
        earliest_access_time_of_low_frequency_frames = *history.begin();
        // 记录第一个遇到的无穷大距离的 frame_id,不记录就会漏掉（因为后面的判断条件不够完善）
        evict_frame_id = node.GetFid();
        has_infinite_distance = true;
      }
    } else {
      // 计算向后 k 距离
      k_distance = current_timestamp_ - *std::next(history.begin(), history.size() - k_);
    }

    // 更新最大向后 k 距离及对应的 frame_id
    if (k_distance > max_k_distance && !has_infinite_distance) {
      max_k_distance = k_distance;
      evict_frame_id = node.GetFid();
    } else if (k_distance == std::numeric_limits<size_t>::max() && has_infinite_distance) {
      if (history.front() < earliest_access_time_of_low_frequency_frames) {
        earliest_access_time_of_low_frequency_frames = history.front();
        evict_frame_id = node.GetFid();
      }
    }
  }

  if (evict_frame_id != -1) {
    node_store_.erase(evict_frame_id);
    curr_size_ = std::max(curr_size_ - 1, static_cast<size_t>(0));
    return evict_frame_id;
  }
  return std::nullopt;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  //加锁
  std::lock_guard<std::mutex> lock(latch_);
  current_timestamp_++;
  //检查frame_id是否合法
  if (frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    throw Exception("Invalid frame_id");
  }
  //更新访问历史
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    //如果frame_id不存在，则创建新的LRUKNode
    auto &node = node_store_[frame_id];
    node.SetFid(frame_id);
    node.SetK(k_);
    node.GetHistoryMutable().push_back(current_timestamp_);
    //新节点默认不可回收
    node.SetIsEvictable(false);
  } else {
    //如果frame_id存在，则更新访问历史
    it->second.GetHistoryMutable().push_back(current_timestamp_);
    //如果访问历史超过k次，则移除最早的访问时间戳
    if (it->second.GetHistory().size() > k_) {
      it->second.GetHistoryMutable().pop_front();
    }
  }
  (void)access_type;  //避免未使用参数的编译警告
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  //加锁
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    throw Exception("Invalid frame_id");
  }
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    //如果frame_id不存在，直接返回
    return;
  }
  if (it->second.IsEvictable() != set_evictable) {
    //如果可回收状态发生变化，更新curr_size_
    it->second.SetIsEvictable(set_evictable);
    //如果变为可回收，curr_size_加一；否则减一
    if (set_evictable) {
      curr_size_++;
    } else if (curr_size_ > 0) {
      curr_size_--;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  //加锁
  std::lock_guard<std::mutex> lock(latch_);
  //检查frame_id是否合法
  if (frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    throw Exception("Invalid frame_id");
  }
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    //如果frame_id不存在，直接返回
    return;
  }
  if (!it->second.IsEvictable()) {
    //如果frame_id不可回收，抛出异常
    throw Exception("Frame is not evictable");
  }
  //移除frame_id及其访问历史
  node_store_.erase(it);
  curr_size_ = std::max(curr_size_ - 1, static_cast<size_t>(0));
}

auto LRUKReplacer::Size() -> size_t {
  //加锁
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
