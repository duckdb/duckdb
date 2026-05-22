//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/unordered_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <absl/container/node_hash_map.h>
#include <unordered_map>

namespace duckdb {
template <class Key, class Value,
          class Hash = typename absl::container_internal::NodeHashMapPolicy<Key, Value>::DefaultHash,
          class Eq = typename absl::container_internal::NodeHashMapPolicy<Key, Value>::DefaultEq,
          class Alloc = typename absl::container_internal::NodeHashMapPolicy<Key, Value>::DefaultAlloc>
using unordered_map = absl::node_hash_map<Key, Value, Hash, Eq, Alloc>;
}
