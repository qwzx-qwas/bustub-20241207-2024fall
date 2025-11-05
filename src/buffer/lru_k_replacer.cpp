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
        const LRUKNode &node = pair.second;
        if (!node.IsEvictable()) {
            //如果frame不可回收，跳过
            continue;
        }
        size_t k_distance = 0;
        const auto &history = node.GetHistory();
        if (history.size() < k_) {
            //如果访问历史少于k次，向后k距离为无穷大
            k_distance = std::numeric_limits<size_t>::max();
        } else {
            //计算向后k距离
            k_distance = current_timestamp_ - history.back();
        }
        //更新最大向后k距离及对应的frame_id
        if (k_distance > max_k_distance) {
            max_k_distance = k_distance;
            evict_frame_id = node.GetFid();
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

void LRUKReplacer::RecordAccess(frame_id_t frame_id,  AccessType access_type) {
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
        LRUKNode new_node;
        new_node.SetFid(frame_id);
        new_node.SetK(k_);
        new_node.GetHistoryMutable().push_back(current_timestamp_);
        new_node.SetIsEvictable(true);
        node_store_[frame_id] = new_node;
    } else {
        //如果frame_id存在，则更新访问历史
        it->second.GetHistoryMutable().push_back(current_timestamp_);
        //如果访问历史超过k次，则移除最早的访问时间戳
        if (it->second.GetHistory().size() > k_) {
            it->second.GetHistoryMutable().pop_front();
        }
    } 
    (void)access_type; //避免未使用参数的编译警告 
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
        } else {
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
    curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { 
    //加锁
    std::lock_guard<std::mutex> lock(latch_);   
    return curr_size_; 
}

}  // namespace bustub
