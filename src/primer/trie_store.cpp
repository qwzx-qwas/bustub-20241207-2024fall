#include "primer/trie_store.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  Trie root;
  {
    std::lock_guard lock(root_lock_);
    root = root_;
  }
  const auto *value = root.Get<T>(key);
  if (value == nullptr) {
    return std::nullopt;
  }
  return ValueGuard<T>(std::move(root), *value);
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  std::lock_guard write_guard(write_lock_);
  Trie root;
  {
    std::lock_guard root_guard(root_lock_);
    root = root_;
  }
  auto new_root = root.Put<T>(key, std::move(value));
  {
    std::lock_guard root_guard(root_lock_);
    root_ = std::move(new_root);
  }
}

void TrieStore::Remove(std::string_view key) {
  std::lock_guard write_guard(write_lock_);
  Trie root;
  {
    std::lock_guard root_guard(root_lock_);
    root = root_;
  }
  auto new_root = root.Remove(key);
  {
    std::lock_guard root_guard(root_lock_);
    root_ = std::move(new_root);
  }
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
