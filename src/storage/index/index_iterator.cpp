/**
 * index_iterator.cpp
 */
#include <cassert>

#include <mutex>
#include "storage/index/index_iterator.h"

namespace bustub {

#define ITERATOR_CONSTRUCTOR(cond) ReadPageGuard rg = bpm_->FetchPageRead(header_page_id); \
                                    auto root_page = rg.As<BPlusTreeHeaderPage>(); \
                                    auto pid = root_page->root_page_id_; \
                                    if (pid == INVALID_PAGE_ID) return; \
                                    page_ = bpm_->FetchPage(pid); \
                                    page_->RLatch(); \
                                    rg.Drop(); \
                                    while (1) { \
                                        auto internal_page = reinterpret_cast<const InternalPage *>(page_->GetData()); \
                                        if (internal_page->IsLeafPage()) break; \
                                        pid = cond; \
                                        Page *child = bpm_->FetchPage(pid); \
                                        child->RLatch(); \
                                        page_->RUnlatch(); \
                                        page_ = child; \
                                    } \

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t header_page_id, BufferPoolManager *bpm) : bpm_(bpm) {
    ITERATOR_CONSTRUCTOR(internal_page->ValueAt(0))
};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t header_page_id, BufferPoolManager *bpm, const KeyType &key, const KeyComparator &comparator) : bpm_(bpm) {
    ITERATOR_CONSTRUCTOR(internal_page->Lookup(key, comparator).first)
    page_->RLatch();
    auto leaf_page = reinterpret_cast<const LeafPage *>(page_->GetData());
    i_ = leaf_page->IndexOfFirstKeyEqualOrGreaterThan(key, comparator).first;
    page_->RUnlatch();
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
    bool isEnd = true;
    if (page_) {
        page_->RLatch();
        auto leaf_page = reinterpret_cast<const LeafPage *>(page_->GetData());
        if (i_ < leaf_page->GetSize() || leaf_page->GetNextPageId() != INVALID_PAGE_ID) isEnd = false;
        page_->RUnlatch();
    }
    return isEnd;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool { return page_ == itr.page_ && i_ == itr.i_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return page_ != itr.page_ || i_ != itr.i_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
    page_->RLatch();
    auto leaf_page = reinterpret_cast<const LeafPage *>(page_->GetData());
    const MappingType& res = leaf_page->At(i_);
    page_->RUnlatch();
    return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
    auto cur = page_;
    cur->RLatch();
    auto leaf_page = reinterpret_cast<const LeafPage *>(page_->GetData());
    if (i_ < leaf_page->GetSize() - 1) {
        i_++;
    } else {
        auto pid = leaf_page->GetNextPageId();
        i_ = 0;
        page_ = pid == INVALID_PAGE_ID ? nullptr : bpm_->FetchPage(pid);
    }
    cur->RUnlatch();
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
