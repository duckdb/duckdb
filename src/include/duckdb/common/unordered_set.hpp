//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/unordered_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <absl/container/node_hash_set.h>
#include <unordered_set>

namespace duckdb {
template <class T, class Hash = typename absl::container_internal::NodeHashSetPolicy<T>::DefaultHash,
          class Eq = typename absl::container_internal::NodeHashSetPolicy<T>::DefaultEq,
          class Alloc = typename absl::container_internal::NodeHashSetPolicy<T>::DefaultAlloc>
using unordered_set = absl::node_hash_set<T, Hash, Eq, Alloc>;
}
