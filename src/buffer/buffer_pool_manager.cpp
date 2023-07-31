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

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
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
  std::scoped_lock latch(latch_);
  Page *page;
  frame_id_t fid;
  page_id_t pid;
  if (free_list_.size()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&fid)) {
    page = &pages_[fid];
    pid = page->GetPageId();
    BUSTUB_ASSERT(!page->GetPinCount(), "Pin count should be 0.");
    if (page->IsDirty()) disk_manager_->WritePage(pid, page->GetData());
    page->ResetMemory();
    page_table_.erase(pid);
  } else {
    return nullptr;
  }
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  page = &pages_[fid];
  pid = AllocatePage();
  page->page_id_ = pid;
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  page_table_[pid] = fid;
  *page_id = pid;
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock latch(latch_);
  Page *page;
  frame_id_t fid;
  page_id_t pid;
  if (page_table_.count(page_id)) {
    fid = page_table_[page_id];
  } else {
    if (free_list_.size()) {
      fid = free_list_.front();
      free_list_.pop_front();
    } else if (replacer_->Evict(&fid)) {
      page = &pages_[fid];
      pid = page->GetPageId();
      BUSTUB_ASSERT(!page->GetPinCount(), "Pin count should be 0.");
      if (page->IsDirty()) disk_manager_->WritePage(pid, page->GetData());
      page->ResetMemory();
      page_table_.erase(pid);
    } else {
      return nullptr;
    }
    page = &pages_[fid];
    disk_manager_->ReadPage(page_id, page->GetData());
    page->page_id_ = page_id;
    page->is_dirty_ = false;
  }
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  pages_[fid].pin_count_++;
  page_table_[page_id] = fid;
  return &pages_[fid];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock latch(latch_);
  if (!page_table_.count(page_id)) return false;
  auto fid = page_table_[page_id];
  if (!pages_[fid].GetPinCount()) return false;
  pages_[fid].pin_count_--;
  if (!pages_[fid].GetPinCount()) {
    replacer_->SetEvictable(fid, true);
  }
  pages_[fid].is_dirty_ |= is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock latch(latch_);
  if (!page_table_.count(page_id)) return false;
  auto fid = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[fid].GetData());
  pages_[fid].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { return false; }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  page->rwlatch_.RLock();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  page->rwlatch_.WLock();
  return {this, nullptr};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
