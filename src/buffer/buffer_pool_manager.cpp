//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
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

void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::make_optional<DiskRequest>(std::move(r))); }

void DiskScheduler::StartWorkerThread() {
  std::optional<DiskRequest> request;
  // 是否循环看request.has_value()
  while ((request = request_queue_.Get(), request.has_value())) {
    if (request->is_write_) {
      disk_manager_->WritePage(request->page_id_, request->data_);
    } else {
      disk_manager_->ReadPage(request->page_id_, request->data_);
    }
    // 请求已处理完成，值设为true
    request->callback_.set_value(true);
  }
}

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id{};
  if (!free_list_.empty()) {
    // find from the free list
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else if (replacer_->Evict(&frame_id)) {
    // write it back to the disk
    if (pages_[frame_id].IsDirty()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(), std::move(promise)});
      future.get();
    }
    page_table_.erase(pages_[frame_id].GetPageId());
  } else {
    return nullptr;
  }
  // call the AllocatePage() method to get a new page id.
  *page_id = AllocatePage();
  // reset the memory and metadata
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  // record the access history of the frame and "Pin" the frame in the replacer
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_[*page_id] = frame_id;
  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id{};
  if (page_table_.count(page_id) != 0) {
    frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  if (!free_list_.empty()) {
    // find from the free list
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else if (replacer_->Evict(&frame_id)) {
    // write it back to the disk
    if (pages_[frame_id].IsDirty()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(), std::move(promise)});
      future.get();
    }
    page_table_.erase(pages_[frame_id].GetPageId());
  } else {
    return nullptr;
  }
  // reset the memory and metadata
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(), std::move(promise)});
  future.get();
  // record the access history of the frame in the replacer
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_[page_id] = frame_id;
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  std::scoped_lock<std::mutex> lock(latch_);
  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ <= 0) {
    return false;
  }
  pages_[frame_id].pin_count_--;

  // set the dirty flag
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }
  if (pages_[frame_id].pin_count_ <= 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::scoped_lock<std::mutex> lock(latch_);
  // empty
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  // flush a page to disk
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(), std::move(promise)});
  future.get();
  // unset the dirty flag
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    FlushPage(pages_[frame_id].GetPageId());
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }
  replacer_->Remove(frame_id);
  page_table_.erase(page_id);
  free_list_.emplace_back(frame_id);
  // reset the memory and metadata
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
