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
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Get, Scan };

class LRUKNode {
  friend class LRUHeap;
  friend class LRUKReplacer;
  public:
    LRUKNode(frame_id_t fid) : fid_(fid) {}
  private:
    frame_id_t fid_;
    std::list<size_t> history_;
    bool is_evictable_{false};
    int i_{-1};
};

class LRUHeap {
  public:
    LRUHeap(size_t k) : k_(k) {}
    void Push(std::shared_ptr<LRUKNode> &node) {
      auto n = heap_.size();
      heap_.push_back(node);
      node->i_ = n;
      up(n);
    }
    void Remove(int i) {
      auto n = heap_.size();
      BUSTUB_ASSERT(i >= 0 && size_t(i) < n, "index out of range.");
      heap_[i]->i_ = -1;
      heap_[n-1]->i_ = i;
      heap_[i] = heap_[n-1];
      heap_.pop_back();
      down(i);
    }
    std::shared_ptr<LRUKNode> Pop() {
      auto n = heap_.size();
      BUSTUB_ASSERT(n > 0, "can't pop empty heap.");
      auto node = heap_.front();
      heap_[n-1]->i_ = 0;
      heap_[0] = std::move(heap_[n-1]);
      heap_.pop_back();
      down(0);
      node->i_ = -1;
      return node;
    }
    size_t Size() {
      return heap_.size();
    }
  private:
    int compare_(std::shared_ptr<LRUKNode> &lhs, std::shared_ptr<LRUKNode> &rhs) {
      // >0: lhs > rhs
      // <0: rhs > lhs
      if (lhs->history_.size() < k_) {
        BUSTUB_ASSERT(rhs->history_.size() < k_, "BUG.");
        return rhs->history_.front() - lhs->history_.front();
      } else {
        BUSTUB_ASSERT(rhs->history_.size() == k_, "BUG.");
        return (lhs->history_.back() - lhs->history_.front()) - (rhs->history_.back() - rhs->history_.front());
      }
    }
    void up(int i) {
      while (1) {
        if (i == 0) return;
        auto j = (i - 1) / 2;
        if (compare_(heap_[j], heap_[i]) > 0) return;
        heap_[j]->i_ = i;
        heap_[i]->i_ = j;
        std::swap(heap_[j], heap_[i]);
        i = j;
      }
    }
    void down(int i) {
      while (1) {
        auto j = i * 2 + 1;
        if (size_t(j) >= heap_.size()) return;
        if (size_t(j) + 1 < heap_.size() && compare_(heap_[j+1], heap_[j]) > 0) j = j + 1;
        if (compare_(heap_[i], heap_[j]) > 0) return;
        heap_[i]->i_ = j;
        heap_[j]->i_ = i;
        std::swap(heap_[i], heap_[j]);
        i = j;
      }
    }
    std::vector<std::shared_ptr<LRUKNode>> heap_;
    size_t k_;
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multipe frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
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
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

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
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  std::unordered_map<frame_id_t, std::shared_ptr<LRUKNode>> node_store_;
  size_t current_timestamp_{0};
  size_t curr_size_{0};
  size_t replacer_size_;
  size_t k_;
  LRUHeap less_than_k_heap_;
  LRUHeap equal_to_k_heap_;
  std::mutex latch_;
};

}  // namespace bustub
