//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
/**
 * @brief 构造 `FrameHeader`，将所有字段初始化为默认值。
 *
 * 有关 `FrameHeader` 的更多信息，请参阅 "buffer/buffer_pool_manager.h" 中的文档。
 *
 * @param frame_id 我们为其创建头信息的帧的帧 ID / 索引。
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
/**
 * @brief 获取指向该帧数据的只读原始指针。
 *
 * @return const char* 指向帧存储的不可变数据的指针。
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
/**
 * @brief 获取指向该帧数据的可变原始指针。
 *
 * @return char* 指向帧存储的可变数据的指针。
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
/**
 * @brief 重置 `FrameHeader` 的成员字段。
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param k_dist The backward k-distance for the LRU-K replacer.
 * @param log_manager The log manager. Please ignore this for P1.
 */
/**
 * @brief 创建一个新的 `BufferPoolManager` 实例并初始化所有字段。
 *
 * 有关 `BufferPoolManager` 的更多信息，请参阅 "buffer/buffer_pool_manager.h" 中的文档。
 *
 * ### 实现说明
 *
 * 我们已经按照参考实现为你实现了构造函数。你可以根据自己的实现自由修改此处内容。
 *
 * 但请注意：如果偏离我们提供的指导太多，我们将很难帮助你。建议先按照给出的分步指导实现缓冲池管理器。
 *
 * 一旦你有了完整可运行的解决方案（所有 Gradescope 测试通过），就可以尝试更有趣的改动。
 *
 * @param num_frames 缓冲池的帧数量。
 * @param disk_manager 磁盘管理器。
 * @param k_dist LRU-K 替换器使用的向后 k 距离。
 * @param log_manager 日志管理器。P1 可以忽略该参数。
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
                                     LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  // 不是严格必要的……
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  // 将单调递增的计数器初始化为 0。
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  // 预先分配所有内存中的帧。
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  // 页表应有恰好 `num_frames_` 个槽，对应恰好 `num_frames_` 个帧。
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  // 初始化所有帧头，并将所有可能的帧 ID 填入空闲帧列表（因为所有帧最初都是空闲的）。
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
/**
 * @brief 销毁 `BufferPoolManager`，释放缓冲池使用的所有内存。
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
/**
 * @brief 返回此缓冲池管理的帧数量。
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

// PinPage
auto BufferPoolManager::PinPage(frame_id_t frame_id) -> void {
  std::scoped_lock latch(*bpm_latch_);
  PinPageInternal(frame_id);
}

auto BufferPoolManager::PinPageInternal(frame_id_t frame_id) -> void {
  frames_[frame_id]->pin_count_.fetch_add(1);
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
}

// UnpinPage
auto BufferPoolManager::UnpinPage(frame_id_t frame_id) -> void {
  std::scoped_lock latch(*bpm_latch_);
  UnpinPageInternal(frame_id);
}

auto BufferPoolManager::UnpinPageInternal(frame_id_t frame_id) -> void {
  size_t old_pin_count = frames_[frame_id]->pin_count_.fetch_sub(1);
  if (old_pin_count == 0) {
    frames_[frame_id]->pin_count_.fetch_add(1);  //恢复原值
    return;
  }
  if (old_pin_count == 1) {
    //只标记为可驱逐，不进行驱逐操作
    replacer_->SetEvictable(frame_id, true);
  }
}

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * Also, make sure to read the documentation for `DeletePage`! You can assume that you will never run out of disk
 * space (via `DiskScheduler::IncreaseDiskSpace`), so this function _cannot_ fail.
 *
 * Once you have allocated the new page via the counter, make sure to call `DiskScheduler::IncreaseDiskSpace` so you
 * have enough space on disk!
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
/**
 * @brief 在磁盘上分配一个新页面。
 *
 * ### 实现说明
 *
 * 你需要维护一个线程安全的单调递增计数器，形式为 `std::atomic<page_id_t>`。
 * 有关原子类型的更多信息，请参阅官方文档。
 *
 * 还要确保阅读 `DeletePage` 的文档！你可以假设不会耗尽磁盘空间（可通过 `DiskScheduler::IncreaseDiskSpace`）。
 * 因此该函数不会失败。
 *
 * 通过计数器分配新页面后，记得调用 `DiskScheduler::IncreaseDiskSpace` 来确保磁盘上有足够空间。
 *
 * TODO(P1)：添加实现。
 *
 * @return 新分配页面的页面 ID。
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  //创建一个新的page id
  page_id_t new_page_id = next_page_id_.fetch_add(1);

  //通知disk scheduler增加磁盘空间
  disk_scheduler_->IncreaseDiskSpace(new_page_id + 1);

  return new_page_id;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places a page or a page's metadata could be, and use that to guide you on implementing this
 * function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * Ideally, we would want to ensure that all space on disk is used efficiently. That would mean the space that deleted
 * pages on disk used to occupy should somehow be made available to new pages allocated by `NewPage`.
 *
 * If you would like to attempt this, you are free to do so. However, for this implementation, you are allowed to
 * assume you will not run out of disk space and simply keep allocating disk space upwards in `NewPage`.
 *
 * For (nonexistent) style points, you can still call `DeallocatePage` in case you want to implement something slightly
 * more space-efficient in the future.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
/**
 * @brief 从数据库中删除一个页面，包括磁盘和内存中的内容。
 *
 * 如果页面在缓冲池中被固定（pinned），该函数什么也不做并返回
 * `false`。否则，从磁盘和内存中移除该页面（如果仍在缓冲池中），并返回 `true`。
 *
 * ### 实现说明
 *
 * 考虑页面或页面元数据可能存在的所有位置，并据此实现该函数。建议在实现 `CheckedReadPage` 和 `CheckedWritePage`
 * 之后实现此函数。
 *
 * 理想情况下，我们希望有效利用磁盘上的空间，即将已删除页面占用的空间回收并供 `NewPage` 分配使用。
 * 如果你想尝试这个优化，可以实现它；但当前实现允许假设不会耗尽磁盘空间，从而在 `NewPage` 中继续向上分配空间。
 *
 * 作为可选的风格改进，可以调用 `DeallocatePage` 以便未来实现更高效的空间管理。
 *
 * TODO(P1)：添加实现。
 *
 * @param page_id 要删除的页面 ID。
 * @return 如果页面存在但无法删除返回 `false`，如果页面不存在或删除成功返回 `true`。
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock latch(*bpm_latch_);
  //检查页面是否在页表中
  auto page_table_iter = page_table_.find(page_id);
  if (page_table_iter == page_table_.end()) {
    //页面不存在，返回true
    return true;
  }
  frame_id_t frame_id = page_table_iter->second;
  //检查页面是否被固定
  if (frames_[frame_id]->pin_count_.load() > 0) {
    //页面被固定，无法删除，返回false
    return false;
  }
  //页面未被固定，可以删除
  //如果页面是脏的，则先写回磁盘
  if (frames_[frame_id]->is_dirty_) {
    DoDiskIO(page_id, frames_[frame_id], true);
    frames_[frame_id]->is_dirty_ = false;  //清除脏标志
  }
  replacer_->Remove(frame_id);
  //清空当前帧数据
  frames_[frame_id]->Reset();
  //从页表中移除页面
  page_table_.erase(page_table_iter);
  //将帧加入空闲帧列表
  free_frames_.push_back(frame_id);
  return true;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are 3 main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
/**
 * @brief 获取对页面数据的可选写锁保护（WritePageGuard）。可根据需要指定 `AccessType`。
 *
 * 如果无法将页面数据加载到内存中，该函数将返回 `std::nullopt`。
 *
 * 页面数据只能通过页面保护器访问。`BufferPoolManager` 的使用者应根据需要获取 `ReadPageGuard` 或 `WritePageGuard`，
 * 以保证对数据的线程安全访问。
 *
 * 同一时间只允许一个 `WritePageGuard` 对页面进行读写。这使得数据访问可以是不可变或可变的，
 * 持有 `WritePageGuard` 的线程可以任意修改页面数据。
 * 如果用户想让多个线程同时读取页面，则应使用 `CheckedReadPage` 获取 `ReadPageGuard`。
 *
 * ### 实现说明
 *
 * 你需要处理三种主要情况。前两种比较简单：一种是内存充足，另一种是不需要执行额外 I/O。思考这两种情况的具体含义。
 *
 * 第三种情况最棘手，即当没有可用内存时。缓冲池需要使用之前实现的替换算法来寻找可用于驱逐的候选帧。
 *
 * 一旦缓冲池确定了要驱逐的帧，可能需要执行若干 I/O 操作以将目标页面加载到该帧中。
 *
 * `CheckedWritePage` 与 `CheckedReadPage` 之间可能会有大量共享代码，编写辅助函数可能会有帮助。
 *
 * 这两个函数是本项目的核心，我们不会提供更多提示。祝你好运！
 *
 * TODO(P1)：添加实现。
 *
 * @param page_id 要写入的页面的 ID。
 * @param access_type 页面访问类型。
 * @return std::optional<WritePageGuard> 如果没有可用空闲帧（内存耗尽）返回 `std::nullopt`，
 * 否则返回保证独占可变访问的 `WritePageGuard`。
 */

//二者的辅助方法

//从磁盘读取页面到帧
auto BufferPoolManager::DoDiskIO(page_id_t page_id, const std::shared_ptr<FrameHeader> &frame, bool rw) -> void {
  //创建promise和future用于异步操作
  std::promise<bool> promise;
  auto future = promise.get_future();

  //创建磁盘请求
  DiskRequest req;
  req.is_write_ = rw;
  req.page_id_ = page_id;
  req.data_ = const_cast<char *>(frame->GetData());
  req.callback_ = std::move(promise);
  //将请求添加到磁盘调度器
  disk_scheduler_->Schedule(std::move(req));
  //等待异步操作完成
  future.get();
}

auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  std::unique_lock latch(*bpm_latch_);
  auto page_table_iter = page_table_.find(page_id);
  if (page_table_iter != page_table_.end()) {
    //让pin_count+1
    PinPageInternal(page_table_iter->second);
    //返回WritePageGuard
    latch.unlock();
    return WritePageGuard(page_id, frames_[page_table_iter->second], replacer_, bpm_latch_);
  }
  if (!free_frames_.empty()) {
    //内存充足但页面不在内存中
    //从空闲帧列表中取出一个空闲帧
    frame_id_t frame_id = free_frames_.front();
    //将该帧从空闲列表中移除
    free_frames_.pop_front();
    //更新页表,即将page_id映射到frame_id
    page_table_[page_id] = frame_id;

    std::shared_ptr<FrameHeader> frame = frames_[frame_id];
    //从磁盘读取页面数据到该帧
    DoDiskIO(page_id, frame, false);
    //对pin_count进行+1操作
    PinPageInternal(frame_id);
    //返回WritePageGuard
    latch.unlock();
    return WritePageGuard(page_id, frame, replacer_, bpm_latch_);
  }

  while (true) {
    //内存不足需要驱逐页面
    //尝试从替换器中选择一个可驱逐的帧
    auto evict_frame_id_opt = replacer_->Evict();
    //检查是否成功选择了一个帧
    if (!evict_frame_id_opt.has_value()) {
      //没有可驱逐的帧，返回std::nullopt
      return std::nullopt;
    }
    //获取要驱逐的帧ID
    frame_id_t evict_frame_id = evict_frame_id_opt.value();

    //获取该帧对应的页面ID
    page_id_t evict_page_id = 0;
    for (const auto &entry : page_table_) {
      if (entry.second == evict_frame_id) {
        evict_page_id = entry.first;
        break;
      }
    }
    //如果该页面是脏的，则先写回磁盘
    if (frames_[evict_frame_id]->is_dirty_) {
      DoDiskIO(evict_page_id, frames_[evict_frame_id], true);
      frames_[evict_frame_id]->is_dirty_ = false;  //清除脏标志
    }
    frames_[evict_frame_id]->Reset();
    //从页表中移除被驱逐页面的映射
    page_table_.erase(evict_page_id);
    //更新页表,即将新的page_id映射到被驱逐的frame_id
    page_table_[page_id] = evict_frame_id;
    //从磁盘读取新页面数据到该帧
    DoDiskIO(page_id, frames_[evict_frame_id], false);
    //对pin_count进行+1操作，
    std::shared_ptr<FrameHeader> frame = frames_[evict_frame_id];
    PinPageInternal(evict_frame_id);
    latch.unlock();
    return WritePageGuard(page_id, frame, replacer_, bpm_latch_);
  }
}
/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
/**
 * @brief 获取对页面数据的可选读锁保护（ReadPageGuard）。可根据需要指定 `AccessType`。
 *
 * 如果无法将页面数据加载到内存中，该函数将返回 `std::nullopt`。
 *
 * 页面数据只能通过页面保护器访问。`BufferPoolManager` 的使用者应根据需要获取 `ReadPageGuard` 或 `WritePageGuard`，
 * 以保证对数据的线程安全访问。
 *
 * 可以同时存在任意数量的 `ReadPageGuard` 在不同线程中读取同一页面，但所有访问必须是只读的。如果需要修改页面数据，
 * 应使用 `CheckedWritePage` 获取 `WritePageGuard`。
 *
 * ### 实现说明
 *
 * 详见 `CheckedWritePage` 的实现细节。
 *
 * TODO(P1)：添加实现。
 *
 * @param page_id 要读取的页面的 ID。
 * @param access_type 页面访问类型。
 * @return std::optional<ReadPageGuard> 如果没有可用空闲帧（内存耗尽）返回 `std::nullopt`，否则返回保证共享只读访问的
 * `ReadPageGuard`。
 */
//找page_id对应的帧
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  std::unique_lock latch(*bpm_latch_);
  auto page_table_iter = page_table_.find(page_id);
  //先检查页面是否在页表中
  if (page_table_iter != page_table_.end()) {
    PinPageInternal(page_table_iter->second);
    latch.unlock();
    return ReadPageGuard(page_id, frames_[page_table_iter->second], replacer_, bpm_latch_);
  }
  //页面不在内存中
  if (!free_frames_.empty()) {
    //内存充足但页面不在内存中
    //从空闲帧列表中取出一个空闲帧
    frame_id_t frame_id = free_frames_.front();
    //将该帧从空闲列表中移除
    free_frames_.pop_front();
    //更新页表,即将page_id映射到frame_id
    page_table_[page_id] = frame_id;
    std::shared_ptr<FrameHeader> frame = frames_[frame_id];
    //从磁盘读取页面数据到该帧
    DoDiskIO(page_id, frame, false);
    PinPageInternal(frame_id);
    latch.unlock();
    return ReadPageGuard(page_id, frame, replacer_, bpm_latch_);
  }
  while (true) {
    //内存不足需要驱逐页面
    //尝试从替换器中选择一个可驱逐的帧
    auto evict_frame_id_opt = replacer_->Evict();
    //检查是否成功选择了一个帧
    if (!evict_frame_id_opt.has_value()) {
      //没有可驱逐的帧，返回std::nullopt
      return std::nullopt;
    }
    //获取要驱逐的帧ID
    frame_id_t evict_frame_id = evict_frame_id_opt.value();
    //获取该帧对应的页面ID
    page_id_t evict_page_id = -1;
    for (const auto &entry : page_table_) {
      if (entry.second == evict_frame_id) {
        evict_page_id = entry.first;
        break;
      }
    }
    frame_id_t old_evict_frame_id = evict_frame_id;

    //如果该页面是脏的，则先写回磁盘
    if (frames_[old_evict_frame_id]->is_dirty_) {
      DoDiskIO(evict_page_id, frames_[old_evict_frame_id], true);
      frames_[old_evict_frame_id]->is_dirty_ = false;  //清除脏标志
    }
    frames_[evict_frame_id]->Reset();
    //从页表中移除被驱逐页面的映射
    page_table_.erase(evict_page_id);
    //更新页表,即将新的page_id映射到被驱逐的frame_id
    page_table_[page_id] = evict_frame_id;
    //从磁盘读取新页面数据到该帧
    std::shared_ptr<FrameHeader> frame = frames_[evict_frame_id];
    DoDiskIO(page_id, frame, false);
    PinPageInternal(evict_frame_id);
    latch.unlock();
    return ReadPageGuard(page_id, frame, replacer_, bpm_latch_);
  }
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
/**
 * @brief 对 `CheckedWritePage` 的封装：如果存在值则解包返回。
 *
 * 如果 `CheckedWritePage` 返回 `std::nullopt`，**该函数将中止整个进程。**
 *
 * 该函数仅应用于测试或便捷场景。如果缓冲池管理器可能耗尽内存，应使用 `CheckedPageWrite` 来处理该情况。
 *
 * 有关实现的更多信息，请参见 `CheckedPageWrite` 的文档。
 *
 * @param page_id 要读取的页面 ID。
 * @param access_type 页面访问类型。
 * @return WritePageGuard 确保对页面数据的独占可变访问的页面保护器。
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
/**
 * @brief 对 `CheckedReadPage` 的封装：如果存在值则解包返回。
 *
 * 如果 `CheckedReadPage` 返回 `std::nullopt`，**该函数将中止整个进程。**
 *
 * 该函数仅应用于测试或便捷场景。如果缓冲池管理器可能耗尽内存，应使用 `CheckedPageWrite` 来处理该情况。
 *
 * 有关实现的更多信息，请参见 `CheckedPageRead` 的文档。
 *
 * @param page_id 要读取的页面 ID。
 * @param access_type 页面访问类型。
 * @return ReadPageGuard 确保对页面数据的共享只读访问的页面保护器。
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
/**
 * @brief 将页面数据刷新到磁盘。
 *
 * 如果页面已被修改，该函数会将页面数据写入磁盘。如果给定页面不在内存中，则返回 `false`。
 *
 * ### 实现说明
 *
 * 建议在完成 `CheckedReadPage` 和 `CheckedWritePage` 之后实现此函数，这样更容易理解应如何操作。
 *
 * TODO(P1)：添加实现。
 *
 * @param page_id 要刷新的页面 ID。
 * @return 如果页表中找不到页面返回 `false`，否则返回 `true`。
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  bool is_success = false;
  {
    std::scoped_lock latch(*bpm_latch_);

    auto page_table_iter = page_table_.find(page_id);
    if (page_table_iter == page_table_.end()) {
      //如果内存中找不到页面，返回false
      return false;
    }
    frame_id = page_table_iter->second;
    if (!frames_[frame_id]->is_dirty_) {
      return true;
    }

    DoDiskIO(page_id, frames_[frame_id], true);
    frames_[frame_id]->is_dirty_ = false;  //将页面标记为非脏
    is_success = true;                     // I/O 成功
  }

  return is_success;
}

/**
 * @brief Flushes all page data that is in memory to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
/**
 * @brief 将内存中的所有页面数据刷新到磁盘。
 *
 * ### 实现说明
 *
 * 建议在完成 `CheckedReadPage`、`CheckedWritePage` 和 `FlushPage` 之后实现此函数，这样更容易理解应如何操作。
 *
 * TODO(P1)：添加实现。
 */
void BufferPoolManager::FlushAllPages() {
  for (const auto &entry : page_table_) {
    FlushPage(entry.first);
  }
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple threads
 * access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely cause
 * problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds the
 * page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will still
 * need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists, otherwise `std::nullopt`.
 */
/**
 * @brief 获取页面的 pin 计数。如果页面不在内存中，则返回 `std::nullopt`。
 *
 * 该函数是线程安全的。调用者可以在多线程环境中调用该函数，多个线程可能同时访问同一页面。
 *
 * 该函数用于测试目的。如果实现不正确，将导致测试套件和自动评分器出现问题。
 *
 * # 实现说明
 *
 * 我们将使用该函数来测试你的缓冲池管理器是否正确管理 pin 计数。由于 `FrameHeader` 中的 `pin_count_` 字段是原子类型，
 * 因此你无需对持有该页面的帧加 latch；可以直接使用原子 `load` 安全读取其值。但你仍然需要获取缓冲池的 latch。
 *
 * 如果对原子类型不熟悉，请参阅 C++ 官方文档。
 *
 * TODO(P1)：添加实现。
 *
 * @param page_id 要获取 pin 计数的页面 ID。
 * @return std::optional<size_t> 如果页面存在则返回其 pin 计数，否则返回 `std::nullopt`。
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  frame_id_t frame_id{-1};

  // scoped_lcok接收一个mutex对象的引用，并在其作用域结束时自动释放锁，从而确保线程安全。
  //注意这里使用了bpm_latch_，它是一个shared_ptr对象，指向一个mutex对象，因此解引用后获得mutex对象的地址传递给scoped_lock。

  //后续考虑是否加入局部锁

  std::scoped_lock latch(*bpm_latch_);
  auto page_table_iter = page_table_.find(page_id);
  if (page_table_iter == page_table_.end()) {
    return std::nullopt;
  }
  frame_id = page_table_iter->second;

  return frames_[frame_id]->pin_count_.load();
}

}  // namespace bustub
