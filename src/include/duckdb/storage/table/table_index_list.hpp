//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table_index_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {

class ConflictManager;

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

	const vector<unique_ptr<Index>> &Indexes() const {
		return indexes;
	}

	void AddIndex(unique_ptr<Index> index);

	void RemoveIndex(Index &index);

	bool Empty();

	idx_t Count();

	void Move(TableIndexList &other);

	Index *FindForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, ForeignKeyType fk_type);
	void VerifyForeignKey(const vector<PhysicalIndex> &fk_keys, DataChunk &chunk, ConflictManager &conflict_manager);

	//! Serialize all indexes owned by this table, returns a vector of block info of all indexes
	vector<BlockPointer> SerializeIndexes(duckdb::MetadataWriter &writer);

	vector<column_t> GetRequiredColumns();

private:
	//! Indexes associated with the current table
	mutex indexes_lock;
	vector<unique_ptr<Index>> indexes;
};
} // namespace duckdb
