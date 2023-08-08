//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetSize(0);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "index out of range.");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType& B_PLUS_TREE_LEAF_PAGE_TYPE::At(int index) const {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "index out of range.");
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IndexOfFirstKeyEqualOrGreaterThan(const KeyType &key, const KeyComparator &comparator) const -> std::pair<int, bool> {
  auto n = GetSize();
  int i = 0, j = n - 1;
  while (i <= j) {
    int mid = i + (j - i) / 2;
    auto res = comparator(key, array_[mid].first);
    if (res < 0) {
      j = mid - 1;
    } else if (res > 0) {
      i = mid + 1;
    } else {
      return std::make_pair(mid, true);
    }
  }
  return std::make_pair(i, false);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *val, const KeyComparator &comparator) const -> bool {
  if (!GetSize()) return false;
  auto res = IndexOfFirstKeyEqualOrGreaterThan(key, comparator);
  if (res.second) {
    *val = array_[res.first].second;
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &val, const KeyComparator &comparator) -> bool {
  auto n = GetSize(), i = 0;
  if (n) {
    auto res = IndexOfFirstKeyEqualOrGreaterThan(key, comparator);
    if (res.second) return false;
    i = res.first;
  }
  BUSTUB_ASSERT(n + 1 <= GetMaxSize(), "Can't insert to a full leaf.");
  for (auto j = n - 1; j >= i; j--) array_[j+1] = std::move(array_[j]);
  array_[i] = std::make_pair(key, val);
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAt(const KeyType &key, const ValueType &val, int i) {
  auto n = GetSize();
  BUSTUB_ASSERT(n + 1 <= GetMaxSize(), "Can't insert to a full leaf.");
  BUSTUB_ASSERT(i >= 0 && i <= GetSize(), "Can only insert at index 0 to n, where n is the current size of the leaf.");
  for (auto j = n - 1; j >= i; j--) array_[j+1] = std::move(array_[j]);
  array_[i] = std::make_pair(key, val);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(int i) {
  auto n = GetSize();
  BUSTUB_ASSERT(i < n, "i is out of range.");
  for (auto j = i; j < n - 1; j++) array_[j] = std::move(array_[j+1]);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *dst) {
  auto n = GetSize();
  dst->CopyNFrom(n, array_);
  IncreaseSize(-n);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *dst) {
  auto n = GetSize() / 2;
  dst->CopyNFrom(n, &array_[GetSize()-n]);
  IncreaseSize(-n);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveBackToFrontOf(void *data) {
  auto dst = static_cast<BPlusTreeLeafPage *>(data);
  auto n = GetSize();
  BUSTUB_ASSERT(n > 0, "Can't move an empty internal node.");
  dst->CopyToFront(&array_[n-1]);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFrontToBackOf(void *data) {
  auto dst = static_cast<BPlusTreeLeafPage *>(data);
  auto n = GetSize();
  BUSTUB_ASSERT(n > 0, "Can't move an empty internal node.");
  dst->CopyToBack(array_);
  for (auto i = 1; i < n; i++) array_[i-1] = std::move(array_[i]);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(int n, MappingType *data) {
  auto n_cur = GetSize(), n_max = GetMaxSize();
  BUSTUB_ASSERT(n_cur + n <= n_max, "Not enough space to copy.");
  for (auto i = n_cur; i < n_cur + n; i++) array_[i] = std::move(data[i-n_cur]);
  IncreaseSize(n);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyToFront(MappingType *data) {
  auto n = GetSize();
  BUSTUB_ASSERT(n + 1 <= GetMaxSize(), "Not enough space to copy.");
  for (auto i = n - 1; i >= 0; i--) array_[i+1] = std::move(array_[i]);
  array_[0] = std::move(*data);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyToBack(MappingType *data) {
  auto n = GetSize();
  BUSTUB_ASSERT(n + 1 <= GetMaxSize(), "Not enough space to copy.");
  array_[n] = std::move(*data);
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
