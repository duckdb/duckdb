#pragma once

#include "duckdb/common/column_index.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

struct ColumnIndexHashFunction {
	uint64_t operator()(const ColumnIndex &index) const {
		auto hasher = std::hash<idx_t>();
		std::queue<reference<const ColumnIndex>> to_hash;

		hash_t result = 0;
		to_hash.push(std::ref(index));
		while (!to_hash.empty()) {
			auto &current = to_hash.front();
			auto &children = current.get().GetChildIndexes();
			for (auto &child : children) {
				to_hash.push(child);
			}
			result ^= hasher(current.get().GetPrimaryIndex());
			to_hash.pop();
		}
		return result;
	}
};

struct ColumnIndexEquality {
	bool operator()(const ColumnIndex &a, const ColumnIndex &b) const {
		std::queue<std::pair<reference<const ColumnIndex>, reference<const ColumnIndex>>> to_check;

		to_check.push(std::make_pair(std::ref(a), std::ref(b)));
		while (!to_check.empty()) {
			auto &current = to_check.front();
			auto &current_a = current.first;
			auto &current_b = current.second;

			if (current_a.get().GetPrimaryIndex() != current_b.get().GetPrimaryIndex()) {
				return false;
			}
			auto &a_children = current_a.get().GetChildIndexes();
			auto &b_children = current_b.get().GetChildIndexes();
			if (a_children.size() != b_children.size()) {
				return false;
			}
			for (idx_t i = 0; i < a_children.size(); i++) {
				to_check.push(std::make_pair(std::ref(a_children[i]), std::ref(b_children[i])));
			}
			to_check.pop();
		}
		return true;
	}
};

template <class T>
using column_index_map = unordered_map<ColumnIndex, T, ColumnIndexHashFunction, ColumnIndexEquality>;

using column_index_set = unordered_set<ColumnIndex, ColumnIndexHashFunction, ColumnIndexEquality>;

} // namespace duckdb
