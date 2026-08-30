#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  current_reads_[read_ts]++;
  if (current_reads_.size() == 1 || read_ts < watermark_) {
    watermark_ = read_ts;
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  auto iterator = current_reads_.find(read_ts);
  if (iterator == current_reads_.end()) {
    throw Exception("removing an unknown read timestamp from watermark");
  }
  iterator->second--;
  if (iterator->second == 0) {
    current_reads_.erase(iterator);
  }
  if (current_reads_.empty()) {
    watermark_ = commit_ts_;
    return;
  }
  watermark_ = current_reads_.begin()->first;
}

auto Watermark::UpdateCommitTs(timestamp_t commit_ts) -> void {
  if (commit_ts < commit_ts_) {
    throw Exception("commit timestamp cannot move backwards");
  }
  commit_ts_ = commit_ts;
  if (current_reads_.empty()) {
    watermark_ = commit_ts;
  }
}

}  // namespace bustub
