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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k), less_than_k_heap_(k), equal_to_k_heap_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::scoped_lock latch(latch_);
    if (!curr_size_) return false;
    std::shared_ptr<LRUKNode> node;
    if (less_than_k_heap_.Size()) {
        node = less_than_k_heap_.Pop();
    } else if (equal_to_k_heap_.Size()) {
        node = equal_to_k_heap_.Pop();
    } else {
        throw bustub::Exception("BUG.");
    }
    *frame_id = node->fid_;
    node_store_.erase(node->fid_);
    curr_size_--;
    return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    std::scoped_lock latch(latch_);
    BUSTUB_ASSERT(frame_id >= 0 && size_t(frame_id) < replacer_size_, "frame_id out of range.");
    if (!node_store_.count(frame_id)) node_store_[frame_id] = std::make_shared<LRUKNode>(frame_id);
    auto &node = node_store_[frame_id];
    auto &history = node->history_;
    auto less_than_k = history.size() < k_;
    history.push_back(current_timestamp_++);
    if (!less_than_k) history.pop_front();
    if (node->is_evictable_) {
        less_than_k ?
            less_than_k_heap_.Remove(node->i_) :
            equal_to_k_heap_.Remove(node->i_);
        history.size() < k_ ?
            less_than_k_heap_.Push(node) :
            equal_to_k_heap_.Push(node);
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock latch(latch_);
    BUSTUB_ASSERT(frame_id >= 0 && size_t(frame_id) < replacer_size_, "frame_id out of range.");
    BUSTUB_ASSERT(node_store_.count(frame_id), "frame_id not in the replacer.");
    auto &node = node_store_[frame_id];
    if (node->is_evictable_ == set_evictable) return;
    if (set_evictable) {
        node->history_.size() < k_ ? 
            less_than_k_heap_.Push(node) :
            equal_to_k_heap_.Push(node);
    } else {
        node->history_.size() < k_ ?
            less_than_k_heap_.Remove(node->i_) :
            equal_to_k_heap_.Remove(node->i_);
    }
    set_evictable ? curr_size_++ : curr_size_--;
    node->is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock latch(latch_);
    if (!node_store_.count(frame_id)) return;
    auto &node = node_store_[frame_id];
    BUSTUB_ASSERT(node->is_evictable_, "can't remove a non-evictable frame.");
    node->history_.size() < k_ ?
        less_than_k_heap_.Remove(node->i_) :
        equal_to_k_heap_.Remove(node->i_);
    node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
    std::scoped_lock latch(latch_);
    return curr_size_;
}

}  // namespace bustub
