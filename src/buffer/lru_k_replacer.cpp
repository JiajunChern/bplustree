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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  for (auto iter = inf_history_list_.begin(); iter != inf_history_list_.end(); iter++) {
    auto evict_id = *iter;
    if (non_evictable_set_.count(evict_id) == 0) {
      *frame_id = evict_id;
      count_map_.erase(evict_id);
      inf_history_list_.erase(iter);
      inf_history_iter_map_.erase(evict_id);
      curr_size_--;
      return true;
    }
  }
  for (auto iter = history_list_.begin(); iter != history_list_.end(); iter++) {
    auto evict_id = *iter;
    if (non_evictable_set_.count(evict_id) == 0) {
      *frame_id = evict_id;
      count_map_.erase(evict_id);
      history_list_.erase(iter);
      history_iter_map_.erase(evict_id);
      curr_size_--;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT((((size_t)(frame_id)) < replacer_size_ && frame_id >= 0), "frame id out of replacer size");
  std::scoped_lock<std::mutex> lock(latch_);
  // first record
  if (count_map_.count(frame_id) == 0) {
    inf_history_list_.push_back(frame_id);
    inf_history_iter_map_[frame_id] = --inf_history_list_.end();
    count_map_[frame_id] = 1;
    curr_size_++;
    return;
  }
  // record in not +inf list (LRU)
  if (count_map_[frame_id] >= k_) {
    // find it push back
    history_list_.erase(history_iter_map_[frame_id]);
    history_list_.push_back(frame_id);
    history_iter_map_[frame_id] = --history_list_.end();
    count_map_[frame_id]++;
    return;
  }
  // record in +inf list (FIFO)
  count_map_[frame_id]++;
  // change to not +inf list
  if (count_map_[frame_id] >= k_) {
    inf_history_list_.erase(inf_history_iter_map_[frame_id]);
    history_list_.push_back(frame_id);
    history_iter_map_[frame_id] = --history_list_.end();
    return;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT((((size_t)(frame_id)) < replacer_size_ && frame_id >= 0), "frame id out of replacer size");
  std::scoped_lock<std::mutex> lock(latch_);
  if (count_map_.count(frame_id) == 0) {
    return;
  }
  if (non_evictable_set_.count(frame_id) == 0 && !set_evictable) {
    non_evictable_set_.insert(frame_id);
    curr_size_--;
  }
  if (non_evictable_set_.count(frame_id) != 0 && set_evictable) {
    non_evictable_set_.erase(frame_id);
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT((((size_t)(frame_id)) < replacer_size_ && frame_id >= 0), "frame id out of replacer size");
  std::scoped_lock<std::mutex> lock(latch_);
  if (count_map_.count(frame_id) == 0) {
    return;
  }
  if (non_evictable_set_.count(frame_id) != 0) {
    return;
  }
  if (count_map_[frame_id] >= k_) {
    history_list_.erase(history_iter_map_[frame_id]);
    history_iter_map_.erase(frame_id);
  } else {
    inf_history_list_.erase(inf_history_iter_map_[frame_id]);
    inf_history_iter_map_.erase(frame_id);
  }
  count_map_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
