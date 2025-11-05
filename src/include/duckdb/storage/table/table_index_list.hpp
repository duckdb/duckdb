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

//! IndexBindState to transition index binding phases preventing lock order inversion.
enum class IndexBindState : uint8_t { UNBOUND, BINDING, BOUND };

//! IndexEntry contains an atomic in addition to the index to ensure correct binding.
struct IndexEntry {
	explicit IndexEntry(unique_ptr<Index> index);
	atomic<IndexBindState> bind_state;
	unique_ptr<Index> index;
};

class TableIndexList {
public:
	//! Scan the index entries, invoking the callback method for every entry.
	template <class T>
	void Scan(T &&callback) {
		lock_guard<mutex> lock(index_entries_lock);
		for (auto &entry : index_entries) {
			if (callback(*entry->index)) {
				break;
			}
		}
	}

	//! Adds an index entry to the list of index entries.
	void AddIndex(unique_ptr<Index> index);
	//! Removes an index entry from the list of index entries.
	void RemoveIndex(const string &name);
	//! Removes all remaining memory of an index after dropping the catalog entry.
	void CommitDrop(const string &name);
	//! Returns true, if the index name does not exist.
	bool NameIsUnique(const string &name);
	//! Returns an optional pointer to the index matching the name.
	optional_ptr<BoundIndex> Find(const string &name);
	//! Binds unbound indexes possibly present after loading an extension.
	void Bind(ClientContext &context, DataTableInfo &table_info, const char *index_type = nullptr);
	//! Returns true, if there are no index entries.
	bool Empty() {
		lock_guard<mutex> lock(index_entries_lock);
		return index_entries.empty();
	}
	//! Returns the number of index entries.
	idx_t Count() {
		lock_guard<mutex> lock(index_entries_lock);
		return index_entries.size();
	}
	//! Returns true, if there are unbound indexes.
	bool HasUnbound() {
		lock_guard<mutex> lock(index_entries_lock);
		return unbound_count != 0;
	}
	//! Overwrite this list with the other list.
	void Move(TableIndexList &other) {
		D_ASSERT(index_entries.empty());
		index_entries = std::move(other.index_entries);
	}
	//! Returns true, if all indexes
	//! Find the foreign key matching the keys.
	optional_ptr<Index> FindForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, const ForeignKeyType fk_type);
	//! Verify a foreign key constraint.
	void VerifyForeignKey(optional_ptr<LocalTableStorage> storage, const vector<PhysicalIndex> &fk_keys,
	                      DataChunk &chunk, ConflictManager &conflict_manager);
	//! Get the combined column ids of the indexes.
	unordered_set<column_t> GetRequiredColumns();
	//! Serialize all indexes of the table.
	vector<IndexStorageInfo> SerializeToDisk(QueryContext context, const case_insensitive_map_t<Value> &options);

public:
	//! Initialize an index_chunk from a table.
	static void InitializeIndexChunk(DataChunk &index_chunk, const vector<LogicalType> &table_types,
	                                 vector<StorageIndex> &mapped_column_ids, DataTableInfo &data_table_info);
	//! Reference the indexed columns of a table chunk.
	static void ReferenceIndexChunk(DataChunk &table_chunk, DataChunk &index_chunk,
	                                vector<StorageIndex> &mapped_column_ids);

private:
	//! A lock to prevent any concurrent changes to the index entries.
	mutex index_entries_lock;
	//! The index entries of the table.
	vector<unique_ptr<IndexEntry>> index_entries;
	//! Contains the number of unbound indexes.
	idx_t unbound_count = 0;
};

} // namespace duckdb
