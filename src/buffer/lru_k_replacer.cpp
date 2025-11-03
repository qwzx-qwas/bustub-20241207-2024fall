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
    //加锁
    std::lock_guard<std::mutex> lock(latch_);
    //遍历node_store_，找出具有最大向后k距离的frame
    frame_id_t evict_frame_id = -1;
    size_t max_k_distance = 0;
    for (const auto &pair : node_store_) {
        //获取储存在LRUKNode中的访问历史中的最近一次访问时间戳并计算k距离
        if (pair.second.history_.size() < k_) {
            //如果历史访问次数小于k，则视为+inf
            const auto &recent_history = std::numeric_limits<size_t>::max();
        } else {
            const auto &recent_history = pair.second.history_.back();
        }     
        size_t k_distance = current_timestamp_ - recent_history;
        //判断该frame是否可回收且k距离大于当前最大k距离
        if (pair.second.is_evictable_ && k_distance > max_k_distance) {
            max_k_distance = k_distance;
            evict_frame_id = pair.second.fid_;
        }
    }
    if (evict_frame_id != -1) {
        //找到应被淘汰的frame，更新相关数据结构并返回frame_id（算法驱逐）
        node_store_.erase(evict_frame_id);
        curr_size_--;
        return evict_frame_id;
    }
    return std::nullopt; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return 0; }

}  // namespace bustub
