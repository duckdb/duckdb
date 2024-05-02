//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/insertion_order_preserving_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

template <typename K, typename V>
class InsertionOrderPreservingMap : public vector<V> {
public:
	InsertionOrderPreservingMap() {
	}
	case_insensitive_map_t<K> map_idx;

public:
	vector<string> Keys() const {
		vector<string> keys;
		keys.resize(this->size());
		for (auto &kv : map_idx) {
			keys[kv.second] = kv.first;
		}

		return keys;
	}
};

} // namespace duckdb
