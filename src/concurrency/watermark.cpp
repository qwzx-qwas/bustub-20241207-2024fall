#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
 
  // TODO(fall2023): implement me! 

  //更新hash表
  current_reads_[read_ts]++;

  //watermark只需要维护所有活跃事务中最小的read_ts
  watermark_ = std::min(watermark_, read_ts);

}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  current_reads_[read_ts]--;
  if(current_reads_[read_ts] == 0){
    current_reads_.erase(read_ts);
  }
  //如果被移除的事务的read_ts正好是watermark，则需要重新计算watermark
  if(read_ts == watermark_){
    if(current_reads_.empty()){
      watermark_ = commit_ts_;
    }else{
      //从 watermark 往前走，找到第一个还存在的 read_ts
      //while 检查当前watermark是否在hash表中存在
      //如果不存在，watermark++，以此往前推进
      //实现了O(1)的watermark更新
      while(current_reads_.count(watermark_) == 0){
        watermark_++;
      }
    }
  }
}

}  // namespace bustub
