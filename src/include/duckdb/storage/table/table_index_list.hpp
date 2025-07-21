//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/table_index_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {

class ConflictManager;
class LocalTableStorage;
struct IndexStorageInfo;
struct DataTableInfo;

class TableIndexList {
public:
	//! Scan the indexes, invoking the callback method for every entry.
	template <class T>
	void Scan(T &&callback) {
		lock_guard<mutex> lock(indexes_lock);
		for (auto &index : indexes) {
			if (callback(*index)) {
				break;
			}
		}
	}

	//! Adds an index to the list of indexes.
	void AddIndex(unique_ptr<Index> index);
	//! Removes an index from the list of indexes.
	void RemoveIndex(const string &name);
	//! Removes all remaining memory of an index after dropping the catalog entry.
	void CommitDrop(const string &name);
	//! Returns true, if the index name does not exist.
	bool NameIsUnique(const string &name);
	//! Returns an optional pointer to the index matching the name.
	optional_ptr<BoundIndex> Find(const string &name);
	//! Binds unbound indexes possibly present after loading an extension.
	void Bind(ClientContext &context, DataTableInfo &table_info, const char *index_type = nullptr);
	//! Returns true, if there are no indexes in this list.
	bool Empty();
	//! Returns the number of indexes in this list.
	idx_t Count();
	//! Overwrite this list with the other list.
	void Move(TableIndexList &other);
	//! Find the foreign key matching the keys.
	optional_ptr<Index> FindForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, const ForeignKeyType fk_type);
	//! Verify a foreign key constraint.
	void VerifyForeignKey(optional_ptr<LocalTableStorage> storage, const vector<PhysicalIndex> &fk_keys,
	                      DataChunk &chunk, ConflictManager &conflict_manager);
	//! Get the combined column ids of the indexes in this list.
	unordered_set<column_t> GetRequiredColumns();
	//! Serialize all indexes of this table.
	vector<IndexStorageInfo> SerializeToDisk(QueryContext context, const case_insensitive_map_t<Value> &options);

private:
	//! A lock to prevent any concurrent changes to the indexes.
	mutex indexes_lock;
	//! Indexes associated with the table.
	vector<unique_ptr<Index>> indexes;
};

} // namespace duckdb
