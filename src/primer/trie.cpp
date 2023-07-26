#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if (!root_) return nullptr;
  auto node = root_;
  for (auto ch = key.begin(); ch != key.end(); ch++) {
    auto &children = node->children_;
    if (!children.count(*ch)) return nullptr;
    node = children.at(*ch);
  }
  if (!node->is_value_node_) return nullptr;
  auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  if (!value_node) return nullptr;
  return value_node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  Trie t;
  auto val_ptr = std::make_shared<T>(std::move(value));
  auto node = root_ ? root_->Clone() : std::make_shared<TrieNode>();
  std::shared_ptr<TrieNode> parent;
  t.root_ = node;
  for (auto ch = key.begin(); ch != key.end(); ch++) {
    auto &children = node->children_;
    parent = node;
    node = children.count(*ch) ? node = children[*ch]->Clone() : std::make_shared<TrieNode>();
    children[*ch] = node;
  }
  if (key.size()) {
    auto ch = key.back();
    parent->children_[ch] = std::make_shared<TrieNodeWithValue<T>>(std::move(node->children_), std::move(val_ptr));
  } else {
    t.root_ = std::make_shared<TrieNodeWithValue<T>>(std::move(node->children_), std::move(val_ptr));
  }
  return t;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  Trie t;
  if (!root_) return t;
  std::vector<std::shared_ptr<const TrieNode>> vec;
  vec.push_back(root_);
  for (auto ch = key.begin(); ch != key.end(); ch++) {
    auto &children = vec.back()->children_;
    if (!children.count(*ch)) break;
    vec.push_back(children.at(*ch));
  }
  if (vec.size() != key.size() + 1 || !vec.back()->is_value_node_) {
    t.root_ = root_->Clone();
    return t;
  }
  std::shared_ptr<TrieNode> last;
  if (vec.back()->children_.size()) last = std::make_shared<TrieNode>(vec.back()->children_);
  vec.pop_back();
  for (auto ch = key.rbegin(); ch != key.rend(); ch++) {
    if (last) {
      auto node = vec.back()->Clone();
      node->children_[*ch] = last;
      last = std::move(node);
    } else if (vec.back()->children_.size() > 1 || vec.back()->is_value_node_) {
      auto node = vec.back()->Clone();
      node->children_.erase(*ch);
      last = std::move(node);
    } else {
      last = nullptr;
    }
    vec.pop_back();
  }
  t.root_ = last;
  return t;
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
