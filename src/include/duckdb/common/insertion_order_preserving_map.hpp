//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/insertion_order_preserving_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

template <typename V>
class InsertionOrderPreservingMap {
public:
	typedef vector<pair<string, V>> VECTOR_TYPE; // NOLINT: matching name of std
	typedef string key_type;                     // NOLINT: matching name of std

public:
	InsertionOrderPreservingMap() {
	}

private:
	VECTOR_TYPE map;
	case_insensitive_map_t<idx_t> map_idx;

public:
	vector<string> Keys() const {
		vector<string> keys;
		keys.resize(this->size());
		for (auto &kv : map_idx) {
			keys[kv.second] = kv.first;
		}

		return keys;
	}

	typename VECTOR_TYPE::iterator begin() { // NOLINT: match stl API
		return map.begin();
	}

	typename VECTOR_TYPE::iterator end() { // NOLINT: match stl API
		return map.end();
	}

	typename VECTOR_TYPE::const_iterator begin() const { // NOLINT: match stl API
		return map.begin();
	}

	typename VECTOR_TYPE::const_iterator end() const { // NOLINT: match stl API
		return map.end();
	}

	typename VECTOR_TYPE::reverse_iterator rbegin() { // NOLINT: match stl API
		return map.rbegin();
	}

	typename VECTOR_TYPE::reverse_iterator rend() { // NOLINT: match stl API
		return map.rend();
	}

	typename VECTOR_TYPE::iterator find(const string &key) { // NOLINT: match stl API
		auto entry = map_idx.find(key);
		if (entry == map_idx.end()) {
			return map.end();
		}
		return map.begin() + static_cast<typename VECTOR_TYPE::difference_type>(entry->second);
	}

	typename VECTOR_TYPE::const_iterator find(const string &key) const { // NOLINT: match stl API
		auto entry = map_idx.find(key);
		if (entry == map_idx.end()) {
			return map.end();
		}
		return map.begin() + static_cast<typename VECTOR_TYPE::difference_type>(entry->second);
	}

	idx_t size() const { // NOLINT: match stl API
		return map_idx.size();
	}

	bool empty() const { // NOLINT: match stl API
		return map_idx.empty();
	}

	void resize(idx_t nz) { // NOLINT: match stl API
		map.resize(nz);
	}

	void insert(const string &key, V &&value) { // NOLINT: match stl API
		map.emplace_back(key, std::move(value));
		map_idx[key] = map.size() - 1;
	}

	void insert(const string &key, const V &value) { // NOLINT: match stl API
		map.emplace_back(key, value);
		map_idx[key] = map.size() - 1;
	}

	void insert(pair<string, V> &&value) { // NOLINT: match stl API
		map_idx[value.first] = map.size();
		map.push_back(std::move(value));
	}

	void erase(typename VECTOR_TYPE::iterator it) { // NOLINT: match stl API
		auto key = it->first;
		auto idx = map_idx[it->first];
		map.erase(it);
		map_idx.erase(key);
		for (auto &kv : map_idx) {
			if (kv.second > idx) {
				kv.second--;
			}
		}
	}

	bool contains(const string &key) const { // NOLINT: match stl API
		return map_idx.find(key) != map_idx.end();
	}

	const V &at(const string &key) const { // NOLINT: match stl API
		return map[map_idx.at(key)].second;
	}

	V &operator[](const string &key) {
		if (!contains(key)) {
			auto v = V();
			insert(key, std::move(v));
		}
		return map[map_idx[key]].second;
	}
};

} // namespace duckdb
