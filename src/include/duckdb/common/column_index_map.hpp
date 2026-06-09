#pragma once

#include "duckdb/common/column_index.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/queue.hpp"

namespace duckdb {

struct ColumnIndexHashFunction {
	uint64_t operator()(const ColumnIndex &index) const {
		auto index_hasher = std::hash<idx_t>();
		auto field_hasher = std::hash<string>();
		queue<reference<const ColumnIndex>> to_hash;

		hash_t result = 0;
		to_hash.push(std::ref(index));
		while (!to_hash.empty()) {
			auto &current = to_hash.front();
			auto &children = current.get().GetChildIndexes();
			for (auto &child : children) {
				to_hash.push(child);
			}

			if (current.get().HasPrimaryIndex()) {
				result ^= index_hasher(current.get().GetPrimaryIndex());
			} else {
				result ^= field_hasher(current.get().GetFieldName());
			}
			to_hash.pop();
		}
		return result;
	}
};

struct ColumnIndexEquality {
	bool operator()(const ColumnIndex &a, const ColumnIndex &b) const {
		return a == b;
	}
};

template <class T>
using column_index_map = unordered_map<ColumnIndex, T, ColumnIndexHashFunction, ColumnIndexEquality>;

using column_index_set = unordered_set<ColumnIndex, ColumnIndexHashFunction, ColumnIndexEquality>;

} // namespace duckdb
