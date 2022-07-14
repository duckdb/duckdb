//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/storage/index.hpp"
namespace duckdb {
class TableIndex {
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

	void AddIndex(unique_ptr<Index> index);

	void RemoveIndex(Index *index);

	bool Empty();

	idx_t Count();

	Index *FindForeignKeyIndex(const vector<idx_t> &fk_keys, ForeignKeyType fk_type);

	//! Serialize all indexes owned by this table, returns a vector of block info of all indexes
	vector<BlockPointer> SerializeIndexes(duckdb::MetaBlockWriter &writer);

private:
	//! Indexes associated with the current table
	mutex indexes_lock;
	vector<unique_ptr<Index>> indexes;
};
} // namespace duckdb
