//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
 private:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // 最近 K 次看到的时间戳历史记录。最不最近的时间戳保存在容器前端。
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.
  // 如果你开始使用这些成员，移除 [[maybe_unused]]。可以随意修改这些成员变量以满足实现需求。

   std::list<size_t> history_;
   size_t k_;
   //表示该节点对应的frame_id
   frame_id_t fid_;
   //表示该节点是否可以被移除
   bool is_evictable_{false};
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 * LRUKReplacer 实现了 LRU-k 回收策略。
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 * LRU-k 算法会回收（驱逐）具有最大向后 k 距离的帧。向后 k 距离定义为当前时间戳与第 k 次之前访问时间戳的差值。
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 * 如果某帧的历史访问次数少于 k 次，则其向后 k 距离视为 +inf。当多个帧的向后 k 距离均为 +inf 时，
 * 使用经典的 LRU 算法选择被回收对象。
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   * @param k number of historical references to consider for k-distance
   *
   * 构造函数：创建一个新的 LRUKReplacer。
   * @param num_frames LRUReplacer 需要管理的最大帧数。
   * @param k 计算 k-distance 时考虑的历史访问次数 k。
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   * 析构函数：销毁 LRUKReplacer。
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
   * based on LRU.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * 在候选帧中查找具有最大向后 k 距离的帧并将其回收。只有被标记为 "evictable" 的帧才是回收候选。
   * 若某帧的历史访问次数少于 k 次，则其向后 k 距离视为 +inf；当多个帧均为 +inf 时，
   * 基于 LRU（最早时间戳）选择回收对象。
   * 成功回收后应当减少 replacer 的大小并移除该帧的访问历史。
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict() -> std::optional<frame_id_t>;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   * @param access_type type of access that was received. This parameter is only needed for
   * leaderboard tests.
   *
   * 记录给定帧在当前时间戳被访问的事件。如果该帧之前未出现过，则为其创建访问历史记录条目。
   * 如果 frame_id 无效（例如大于 replacer_size_），应抛出异常或使用 BUSTUB_ASSERT 中止进程。
   */
  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   *
   * 切换帧的可回收状态（evictable / non-evictable）。此函数同时控制 replacer 的大小，
   * 注意：replacer 的大小等于当前可回收帧的数量。
   * 若某帧从可回收变为不可回收，应将大小减一；若从不可回收变为可回收，应将大小加一。
   * 若 frame_id 无效，应抛出异常或中止进程。对于其他情况，不应修改任何内容并直接返回。
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   *
   * 从 replacer 中移除一个可回收的帧及其访问历史。如果移除成功，应当同时将 replacer 的大小减一。
   * 注意：此函数与 Evict 不同，Evict 会移除具有最大向后 k 距离的帧；而 Remove 会移除指定的帧，无视其向后 k 距离。
   * 如果对一个不可回收的帧调用 Remove，应抛出异常或中止进程；若指定帧不存在，应直接返回。
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   *
   * 返回 replacer 的大小，即当前可回收帧的数量。
   */
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  // TODO（学生）：在此实现替换策略！你可以根据需要替换这些成员变量。
  // 如果开始使用这些成员，移除 [[maybe_unused]] 注解。

  //用于记录每个frame的访问历史，key是frame_id，value是对应的LRUKNode（记录访问历史）
   std::unordered_map<frame_id_t, LRUKNode> node_store_;
  //记录当前时间戳（全局时间戳）
  size_t current_timestamp_{0};
  //记录当前可驱逐帧的数量
  size_t curr_size_{0};
  //记录replacer的大小，即buffer pool的帧数
  size_t replacer_size_;
  //LRU-k中的k值
  size_t k_;
  //物理锁latch
  std::mutex latch_;
};

}  // namespace bustub
