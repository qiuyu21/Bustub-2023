#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return true; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  ReadPageGuard rg = bpm_->FetchPageRead(header_page_id_);
  auto root_page = rg.As<BPlusTreeHeaderPage>();
  auto pid = root_page->root_page_id_;
  if (pid == INVALID_PAGE_ID) return false;
  ReadPageGuard rg_1 = bpm_->FetchPageRead(pid);
  rg.Drop();
  while (1) {
    auto internal_page = rg_1.As<InternalPage>();
    if (internal_page->IsLeafPage()) {
      auto leaf_page = rg_1.As<LeafPage>();
      ValueType val;
      if (!leaf_page->Lookup(key, &val, comparator_)) return false;
      result->push_back(val);
      return true;
    } else {
      pid = internal_page->Lookup(key, comparator_).first;
      ReadPageGuard rg_2 = bpm_->FetchPageRead(pid);
      rg_1 = std::move(rg_2);
    }
  }
}

#define PRE_INSERT_AND_REMOVE WritePageGuard header_wg = bpm_->FetchPageWrite(header_page_id_); \
                              auto header_page = header_wg.As<BPlusTreeHeaderPage>();           \
                              auto pid = header_page->root_page_id_;                            \
                              std::deque<WritePageGuard> guards;                                \
                              auto release_all = [&]{                                           \
                                header_wg.Drop();                                               \
                                while (guards.size()) guards.pop_front();                       \
                              };                                                                \

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  PRE_INSERT_AND_REMOVE
  if (pid == INVALID_PAGE_ID) {
    Page *page = bpm_->NewPage(&pid);
    BUSTUB_ASSERT(page, "Failed to create new page.");
    WritePageGuard leaf_wg = {bpm_, page};
    auto leaf_page = leaf_wg.AsMut<LeafPage>();
    leaf_page->Init(leaf_max_size_);
    leaf_page->Insert(key, value, comparator_);
    auto header_page = header_wg.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = page->GetPageId();
    return true;
  }
  std::deque<int> indexes;
  do {
    WritePageGuard wg = bpm_->FetchPageWrite(pid);
    auto internal_page = wg.As<InternalPage>();
    if (internal_page->GetSize() < internal_page->GetMaxSize()) release_all();
    guards.push_back(std::move(wg));
    if (internal_page->IsLeafPage()) break;
    auto res = internal_page->Lookup(key, comparator_);
    pid = res.first;
    indexes.push_back(res.second);
  } while (1);
  // 
  page_id_t last;
  std::pair<KeyType, page_id_t> up;
  auto &wg = guards.back();
  auto leaf_page = wg.As<LeafPage>();
  auto res = leaf_page->IndexOfFirstKeyEqualOrGreaterThan(key, comparator_);
  if (res.second) {
    release_all();
    return false;
  } else if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    auto leaf_page_w = wg.AsMut<LeafPage>();
    leaf_page_w->InsertAt(key, value, res.first);
    return true;
  } else {
    Page *page = bpm_->NewPage(&pid);
    BUSTUB_ASSERT(page, "Failed to create new page.");
    WritePageGuard wg1 = {bpm_, page};
    auto leaf_page_new = wg1.AsMut<LeafPage>();
    auto leaf_page_cur = wg.AsMut<LeafPage>();
    leaf_page_new->Init(leaf_max_size_);
    leaf_page_cur->MoveHalfTo(leaf_page_new);
    leaf_page_new->SetNextPageId(leaf_page_cur->GetNextPageId());
    leaf_page_cur->SetNextPageId(pid);
    res.first <= leaf_page_cur->GetSize() ?
      leaf_page_cur->InsertAt(key, value, res.first) :
      leaf_page_new->InsertAt(key, value, res.first - leaf_page_cur->GetSize());
    last = wg.PageId();
    up = std::make_pair(leaf_page_new->KeyAt(0), pid);
    guards.pop_back();
  }
  while (guards.size()) {
    WritePageGuard wg = std::move(guards.back());
    guards.pop_back();
    last = wg.PageId();
    auto internal_page_cur = wg.AsMut<InternalPage>();
    auto i = indexes.back();
    indexes.pop_back();
    if (internal_page_cur->GetSize() < internal_page_cur->GetMaxSize()) {
      internal_page_cur->InsertAt(up.first, up.second, i + 1);
      return true;
    }
    Page *page = bpm_->NewPage(&pid);
    BUSTUB_ASSERT(page, "Failed to create new page.");
    WritePageGuard wg1 = {bpm_, page};
    auto internal_page_new = wg1.AsMut<InternalPage>();
    internal_page_new->Init(internal_max_size_);
    internal_page_cur->MoveHalfTo(internal_page_new);
    i < internal_page_cur->GetSize() ?
      internal_page_cur->InsertAt(up.first, up.second, i + 1) :
      internal_page_new->InsertAt(up.first, up.second, i - internal_page_cur->GetSize() + 1);
    if (internal_page_new->GetSize() < internal_page_new->GetMinSize()) {
      internal_page_cur->MoveBackToFrontOf(internal_page_new);
    }
    up = std::make_pair(internal_page_new->KeyAt(0), pid);
  }
  Page *page = bpm_->NewPage(&pid);
  BUSTUB_ASSERT(page, "Failed to create new page.");
  WritePageGuard wg1 = {bpm_, page};
  auto internal_page_new = wg1.AsMut<InternalPage>();
  internal_page_new->Init(internal_max_size_, last, up.first, up.second);
  auto header_page_w = header_wg.AsMut<BPlusTreeHeaderPage>();
  header_page_w->root_page_id_ = page->GetPageId();
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
* If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  PRE_INSERT_AND_REMOVE
  if (pid == INVALID_PAGE_ID) return;
  std::deque<int> indexes;
  do {
    WritePageGuard wg = bpm_->FetchPageWrite(pid);
    auto internal_page = wg.As<InternalPage>();
    if (internal_page->GetSize() > internal_page->GetMinSize()) release_all();
    guards.push_back(std::move(wg));
    if (internal_page->IsLeafPage()) break;
    auto res = internal_page->Lookup(key, comparator_);
    pid = res.first;
    indexes.push_back(res.second);
  } while(1);
  auto &wg_leaf = guards.back();
  auto leaf_page = wg_leaf.As<LeafPage>();
  auto res = leaf_page->IndexOfFirstKeyEqualOrGreaterThan(key, comparator_);
  if (!res.second) {
    release_all();
    return;
  }
  auto leaf_page_cur = wg_leaf.AsMut<LeafPage>();
  leaf_page_cur->Remove(res.first);
  if (leaf_page_cur->GetSize() >= leaf_page_cur->GetMinSize()) return;
  auto isChildLeaf = true;
  while (guards.size() >= 2) {
    auto wg_child = std::move(guards.back());
    guards.pop_back();
    auto &wg_parent = guards.back();
    if (Borrow(wg_parent, wg_child, indexes.back(), isChildLeaf)) {
      release_all();
      return; 
    }
    Merge(wg_parent, wg_child, indexes.back(), isChildLeaf);
    isChildLeaf = false;
    indexes.pop_back();
  }
  WritePageGuard &wg = guards.back();
  auto internal_page_r = wg.As<InternalPage>();
  if (internal_page_r->GetSize() >= internal_page_r->GetMinSize() || internal_page_r->IsLeafPage()) {
    return;
  } else if (internal_page_r->GetSize() == 1) {
    auto internal_page = wg.AsMut<InternalPage>();
    auto header_page_w = header_wg.AsMut<BPlusTreeHeaderPage>();
    header_page_w->root_page_id_ = internal_page->ValueAt(0);
  }
}

#define BORROW(type) auto cur_page = child.AsMut<type>();                               \
                     auto sibling_page = sibling_pg.AsMut<type>();                      \
                     if (i == 0) {                                                      \
                      sibling_page->MoveBackToFrontOf(cur_page);                        \
                      parent_page->SetKeyAt(childIndex, cur_page->KeyAt(0));            \
                     } else {                                                           \
                      sibling_page->MoveFrontToBackOf(cur_page);                        \
                      parent_page->SetKeyAt(childIndex + 1, sibling_page->KeyAt(0));    \
                     }                                                                  \

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Borrow(WritePageGuard &parent, WritePageGuard &child, int childIndex, bool isChildLeaf) -> bool {
  auto parent_page_r = parent.As<InternalPage>();
  int indexes[2] = {-1, -1};
  if (childIndex > 0) indexes[0] = childIndex - 1;
  if (childIndex < parent_page_r->GetSize() - 1) indexes[1] = childIndex + 1;
  for (auto i = 0; i < 2; i++) {
    if (indexes[i] == -1) continue;
    BasicPageGuard sibling_pg = bpm_->FetchPageBasic(parent_page_r->ValueAt(indexes[i]));
    auto sibling_page_r = sibling_pg.As<BPlusTreePage>();
    if (!sibling_page_r->CanBorrow()) continue;
    auto parent_page = parent.AsMut<InternalPage>();
    if (isChildLeaf) {
      BORROW(LeafPage)
    } else {
      BORROW(InternalPage)
    }
    return true;
  }
  return false;
}

#define MERGE(type) auto cur_page = child.AsMut<type>();          \
                    auto sibling_page = sibling_pg.AsMut<type>(); \
                    r == childIndex ?                             \
                      cur_page->MoveAllTo(sibling_page) :         \
                      sibling_page->MoveAllTo(cur_page);          \

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(WritePageGuard &parent, WritePageGuard &child, int childIndex, bool isChildLeaf) {
  auto parent_page = parent.AsMut<InternalPage>();
  int r = childIndex;
  // TODO:
  if (child.AsMut<InternalPage>()->GetSize()) {
    int l = childIndex > 0 ? childIndex - 1 : childIndex;
    r = l + 1;
    BasicPageGuard sibling_pg = r == childIndex ?
                                  bpm_->FetchPageBasic(parent_page->ValueAt(l)) : 
                                  bpm_->FetchPageBasic(parent_page->ValueAt(r));
    if (isChildLeaf) {
      auto cur_page = child.AsMut<LeafPage>();
      auto sibling_page = sibling_pg.AsMut<LeafPage>();
      r == childIndex ?
        sibling_page->SetNextPageId(cur_page->GetNextPageId()) :
        cur_page->SetNextPageId(sibling_page->GetNextPageId());
    } else {
      r == childIndex ?
        child.AsMut<InternalPage>()->SetKeyAt(0, parent_page->KeyAt(r)) :
        sibling_pg.AsMut<InternalPage>()->SetKeyAt(0, parent_page->KeyAt(r));
    }
    if (isChildLeaf) {
      MERGE(LeafPage)
    } else {
      MERGE(InternalPage)
    }
  }
  // TODO: deallocate the page
  parent_page->Remove(r);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(header_page_id_, bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(header_page_id_, bpm_, key, comparator_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE();
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard header_rg = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_rg.As<BPlusTreeHeaderPage>();
  auto pid = header_page->root_page_id_;
  return pid;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
