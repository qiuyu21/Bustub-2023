//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetSize(0);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size, const ValueType &lhs, const KeyType &mid, const ValueType &rhs) {
  Init(max_size);
  array_[0].second = lhs;
  array_[1] = std::make_pair(mid, rhs);
  SetSize(2);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "index out of range.");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index > 0 && index < GetSize(), "index out of range.");
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "index out of range.");
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> std::pair<ValueType, int> {
  auto n = GetSize();
  auto i = 1, j = n - 1;
  while (i <= j) {
    auto mid = i + (j - i) / 2;
    auto res = comparator(key, array_[mid].first);
    if (res < 0) {
      j = mid - 1;
    } else if (res > 0) {
      i = mid + 1;
    } else {
      return std::make_pair(array_[mid].second, j);
    }
  }
  return std::make_pair(array_[j].second, j);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *dst) {
  auto n = GetSize() / 2;
  dst->CopyNFrom(n, &array_[GetSize()-n]);
  IncreaseSize(-n);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveBackToFrontOf(void *data) {
  auto dst = static_cast<BPlusTreeInternalPage *>(data);
  auto n = GetSize();
  BUSTUB_ASSERT(n > 0, "Can't move an empty internal node.");
  dst->CopyToFront(&array_[n-1]);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFrontToBackOf(void *data) {
  auto dst = static_cast<BPlusTreeInternalPage *>(data);
  auto n = GetSize();
  BUSTUB_ASSERT(n > 0, "Can't move an empty internal node.");
  dst->CopyToBack(array_);
  for (auto i = 1; i < n; i++) array_[i-1] = std::move(array_[i]);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAt(const KeyType &key, const ValueType &val, int i) {
  auto n = GetSize();
  BUSTUB_ASSERT(n + 1 <= GetMaxSize(), "Can't insert to a full leaf.");
  BUSTUB_ASSERT(i >= 0 && i <= GetSize(), "Can only insert at index 0 to n, where n is the current size of the leaf.");
  for (auto j = n - 1; j >= i; j--) array_[j+1] = std::move(array_[j]);
  array_[i] = std::make_pair(key, val);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(int n, MappingType *data) {
  auto n_cur = GetSize(), n_max = GetMaxSize();
  BUSTUB_ASSERT(n_cur + n <= n_max, "Not enough space to copy.");
  for (auto i = n_cur; i < n_cur + n; i++) array_[i] = std::move(data[i-n_cur]);
  IncreaseSize(n);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyToFront(MappingType *data) {
  auto n = GetSize();
  BUSTUB_ASSERT(n + 1 <= GetMaxSize(), "Not enough space to copy.");
  for (auto i = n - 1; i >= 0; i--) array_[i+1] = std::move(array_[i]);
  array_[0] = std::move(*data);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyToBack(MappingType *data) {
  auto n = GetSize();
  BUSTUB_ASSERT(n + 1 <= GetMaxSize(), "Not enough space to copy.");
  array_[n] = std::move(*data);
  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
