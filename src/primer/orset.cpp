#include "primer/orset.h"
#include <algorithm>
#include <string>
#include <vector>
#include "fmt/format.h"

namespace bustub {

template <typename T>
auto ORSet<T>::Contains(const T &elem) const -> bool {
  const auto iterator = add_tags_.find(elem);
  if (iterator == add_tags_.end()) {
    return false;
  }
  return std::any_of(iterator->second.begin(), iterator->second.end(),
                     [this](uid_t tag) { return removed_tags_.count(tag) == 0; });
}

template <typename T>
void ORSet<T>::Add(const T &elem, uid_t uid) {
  add_tags_[elem].insert(uid);
}

template <typename T>
void ORSet<T>::Remove(const T &elem) {
  const auto iterator = add_tags_.find(elem);
  if (iterator != add_tags_.end()) {
    removed_tags_.insert(iterator->second.begin(), iterator->second.end());
  }
}

template <typename T>
void ORSet<T>::Merge(const ORSet<T> &other) {
  for (const auto &[elem, tags] : other.add_tags_) {
    add_tags_[elem].insert(tags.begin(), tags.end());
  }
  removed_tags_.insert(other.removed_tags_.begin(), other.removed_tags_.end());
}

template <typename T>
auto ORSet<T>::Elements() const -> std::vector<T> {
  std::vector<T> elements;
  for (const auto &[elem, tags] : add_tags_) {
    if (std::any_of(tags.begin(), tags.end(), [this](uid_t tag) { return removed_tags_.count(tag) == 0; })) {
      elements.push_back(elem);
    }
  }
  return elements;
}

template <typename T>
auto ORSet<T>::ToString() const -> std::string {
  auto elements = Elements();
  std::sort(elements.begin(), elements.end());
  return fmt::format("{{{}}}", fmt::join(elements, ", "));
}

template class ORSet<int>;
template class ORSet<std::string>;

}  // namespace bustub
