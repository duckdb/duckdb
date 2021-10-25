//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/table_index_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {

class TableIndexList {
public:
	//! Scan the catalog set, invoking the callback method for every entry
	template <class T>
	void Scan(T &&callback) {
		// lock the catalog set
		lock_guard<mutex> lock(indexes_lock);
		for (auto &index : indexes) {
			if (callback(*index)) {
				break;
			}
		}
	}

	void AddIndex(unique_ptr<Index> index) {
		D_ASSERT(index);
		lock_guard<mutex> lock(indexes_lock);
		indexes.push_back(move(index));
	}

	void RemoveIndex(Index *index) {
		D_ASSERT(index);
		lock_guard<mutex> lock(indexes_lock);

		for (idx_t index_idx = 0; index_idx < indexes.size(); index_idx++) {
			auto &index_entry = indexes[index_idx];
			if (index_entry.get() == index) {
				indexes.erase(indexes.begin() + index_idx);
				break;
			}
		}
	}

	void Clear() {
		lock_guard<mutex> lock(indexes_lock);
		indexes.clear();
	}

	bool Empty() {
		lock_guard<mutex> lock(indexes_lock);
		return indexes.empty();
	}

	idx_t Count() {
		lock_guard<mutex> lock(indexes_lock);
		return indexes.size();
	}

private:
	//! Indexes associated with the current table
	mutex indexes_lock;
	vector<unique_ptr<Index>> indexes;
};

}
