//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/table_index_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {

class ConflictManager;
struct IndexStorageInfo;
struct DataTableInfo;

class TableIndexList {
public:
	//! Scan the indexes, invoking the callback method for every entry
	template <class T>
	void Scan(T &&callback) {
		lock_guard<mutex> lock(indexes_lock);
		for (auto &index : indexes) {
			if (callback(*index)) {
				break;
			}
		}
	}

	//! Scan the indexes, invoking the callback method for every bound entry of a specific type
	template <class T, class FUNC>
	void ScanBound(FUNC &&callback) {
		lock_guard<mutex> lock(indexes_lock);
		for (auto &index : indexes) {
			if (index->IsBound() && T::TYPE_NAME == index->GetIndexType()) {
				if (callback(index->Cast<T>())) {
					break;
				}
			}
		}
	}

	// Bind any unbound indexes of the specified type and invoke the callback method for every bound entry of the
	// specified type, regardless if it was bound before or not
	template <class T, class FUNC>
	void BindAndScan(ClientContext &context, DataTableInfo &table_info, FUNC &&callback) {
		// FIXME: optimize this by only looping through the indexes once without re-acquiring the lock
		InitializeIndexes(context, table_info, T::TYPE_NAME);
		ScanBound<T>(callback);
	}

	//! Returns a reference to the indexes of this table
	const vector<unique_ptr<Index>> &Indexes() const {
		return indexes;
	}
	//! Adds an index to the list of indexes of this table
	void AddIndex(unique_ptr<Index> index);
	//! Removes an index from the list of indexes of this table
	void RemoveIndex(const string &name);
	//! Completely removes all remaining memory of an index after dropping the catalog entry
	void CommitDrop(const string &name);
	//! Returns true, if the index name does not exist
	bool NameIsUnique(const string &name);
	//! Initializes unknown indexes that might now be present after an extension load, optionally throwing an exception
	//! if a index cant be initialized
	void InitializeIndexes(ClientContext &context, DataTableInfo &table_info, const char *index_type = nullptr);
	bool Empty();
	idx_t Count();
	void Move(TableIndexList &other);

	Index *FindForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, ForeignKeyType fk_type);
	void VerifyForeignKey(const vector<PhysicalIndex> &fk_keys, DataChunk &chunk, ConflictManager &conflict_manager);

	//! Serialize all indexes of this table
	vector<IndexStorageInfo> GetStorageInfos();

	vector<column_t> GetRequiredColumns();

private:
	//! Indexes associated with the current table
	mutex indexes_lock;
	vector<unique_ptr<Index>> indexes;
};
} // namespace duckdb
