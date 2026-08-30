#include "primer/trie.h"
#include <string_view>

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  auto node = root_;
  for (char character : key) {
    if (node == nullptr) {
      return nullptr;
    }
    const auto child = node->children_.find(character);
    if (child == node->children_.end()) {
      return nullptr;
    }
    node = child->second;
  }
  const auto *value_node = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  return value_node == nullptr ? nullptr : value_node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  std::vector<std::shared_ptr<const TrieNode>> path(key.size() + 1);
  path[0] = root_;
  for (size_t index = 0; index < key.size(); index++) {
    if (path[index] == nullptr) {
      break;
    }
    const auto child = path[index]->children_.find(key[index]);
    if (child == path[index]->children_.end()) {
      break;
    }
    path[index + 1] = child->second;
  }

  auto children = path.back() == nullptr ? std::map<char, std::shared_ptr<const TrieNode>>{} : path.back()->children_;
  std::shared_ptr<const TrieNode> replacement =
      std::make_shared<TrieNodeWithValue<T>>(std::move(children), std::make_shared<T>(std::move(value)));

  for (size_t index = key.size(); index > 0; index--) {
    auto parent = path[index - 1] == nullptr ? std::make_unique<TrieNode>() : path[index - 1]->Clone();
    parent->children_[key[index - 1]] = std::move(replacement);
    replacement = std::shared_ptr<const TrieNode>(std::move(parent));
  }
  return Trie(std::move(replacement));
}

auto Trie::Remove(std::string_view key) const -> Trie {
  std::vector<std::shared_ptr<const TrieNode>> path(key.size() + 1);
  path[0] = root_;
  for (size_t index = 0; index < key.size(); index++) {
    if (path[index] == nullptr) {
      return *this;
    }
    const auto child = path[index]->children_.find(key[index]);
    if (child == path[index]->children_.end()) {
      return *this;
    }
    path[index + 1] = child->second;
  }
  if (path.back() == nullptr || !path.back()->is_value_node_) {
    return *this;
  }

  std::shared_ptr<const TrieNode> replacement;
  if (!path.back()->children_.empty()) {
    replacement = std::make_shared<TrieNode>(path.back()->children_);
  }
  for (size_t index = key.size(); index > 0; index--) {
    auto parent = path[index - 1]->Clone();
    if (replacement == nullptr) {
      parent->children_.erase(key[index - 1]);
    } else {
      parent->children_[key[index - 1]] = std::move(replacement);
    }
    if (parent->children_.empty() && !parent->is_value_node_) {
      replacement = nullptr;
    } else {
      replacement = std::shared_ptr<const TrieNode>(std::move(parent));
    }
  }
  return Trie(std::move(replacement));
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
