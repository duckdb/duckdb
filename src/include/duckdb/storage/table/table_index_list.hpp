//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/table_index_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/enums/index_removal_type.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/storage/table/index_entry.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {

class ConflictManager;
class ConflictInfo;
class IndexEntry;
class TableIndexList;
template <class TARGET>
class IndexReadHandle;
template <class TARGET>
class IndexWriteHandle;
class IndexBinder;
struct IndexStorageInfo;
struct DataTableInfo;
template <class T>
class TableIndexIterationHelper;

struct IndexSerializationInfo {
	case_insensitive_map_t<Value> options;
};

// IndexStorageInfo is move-only. Keep every serialized info in owned_infos and expose stable ordered references.
struct IndexSerializationResult {
	//! The ordered list of references to serialize - preserves iteration order of index_entries
	vector<reference<const IndexStorageInfo>> ordered_infos;
	//! Storage for index infos to keep the references in ordered_infos alive.
	vector<IndexStorageInfo> owned_infos;
};

class TableIndexList {
public:
	~TableIndexList();

	//! Iterates over shared ownership of stable index entries while holding the entry-list lock.
	TableIndexIterationHelper<shared_ptr<IndexEntry>> IndexEntries() const;
	//! Adds an index entry to the list of index entries.
	void AddIndex(unique_ptr<Index> index);
	//! Initializes the transaction-local delete and append indexes.
	void InitializeLocalIndexes(TableIndexList &delete_indexes, TableIndexList &append_indexes) const;
	//! Appends a chunk to all index entries.
	void Append(DataChunk &chunk, Vector &row_ids);
	//! Appends a table chunk with generated row IDs, using delete and checkpoint indexes where required.
	ErrorData Append(optional_ptr<TableIndexList> delete_indexes, DataChunk &chunk, row_t row_start,
	                 IndexAppendMode append_mode, optional_idx active_checkpoint);
	//! Reverts an append to all index entries.
	void RevertAppend(DataChunk &chunk, Vector &row_ids);
	//! Reverts an append made directly to all bound physical indexes.
	void RevertIndexAppend(DataChunk &chunk, row_t row_start);
	//! Appends deleted rows to all unique indexes.
	void AppendToDeleteIndexes(DataChunk &chunk, Vector &row_ids);
	//! Applies a removal or removal rollback to all index entries.
	void RemoveFromIndexes(DataChunk &chunk, Vector &row_ids, IndexRemovalType removal_type,
	                       optional_idx active_checkpoint = optional_idx());
	//! Removes an index entry from the list of index entries and release any storage the index owns.
	void RemoveIndex(const Identifier &name);
	//! Returns true, if the index name does not exist.
	bool NameIsUnique(const string &name) const;
	//! Returns true if an index with the given name exists.
	bool Contains(const Identifier &name) const;
	//! Returns shared ownership of the stable logical index entry matching the name.
	shared_ptr<IndexEntry> FindEntry(const Identifier &name) const;
	//! Binds unbound indexes possibly present after loading an extension.
	void Bind(ClientContext &context, DataTableInfo &table_info, const optional<string> &index_type = {});
	//! Returns true, if there are no index entries.
	bool Empty() const {
		return Count() == 0;
	}
	//! Returns the number of index entries.
	idx_t Count() const {
		annotated_lock_guard lock(index_entries_lock);
		return index_entries.size();
	}
	//! Returns true, if there are unbound indexes.
	bool HasUnbound() const {
		annotated_lock_guard lock(index_entries_lock);
		return unbound_count != 0;
	}
	//! Returns true, if there are unique indexes.
	bool HasUniqueIndexes() const;
	//! Verifies all unique ART indexes, optionally recording conflicts.
	void VerifyUniqueIndexes(optional_ptr<const TableIndexList> delete_indexes, DataChunk &chunk,
	                         optional_ptr<ConflictManager> manager) const;
	//! Vacuums all bound indexes.
	void Vacuum();
	//! Rebuilds all indexes with chunks supplied by the scan callback.
	void Rebuild(const IndexRebuildScan &scan);
	//! Verifies the buffers of all bound indexes and their delete deltas.
	void VerifyBuffers() const;
	//! Verifies that no index is updated by the given columns.
	void VerifyUpdate(const vector<PhysicalIndex> &column_ids) const;
	//! Returns table storage metadata for all indexes.
	vector<IndexInfo> GetStorageInfo() const;
	//! Returns the combined in-memory size of all bound indexes.
	idx_t GetInMemorySize() const;
	//! Returns the set of distinct index types across all bound indexes.
	unordered_set<string> DistinctIndexTypes() const;
	//! Returns true if every index is bound and has the given type (vacuously true for an empty list).
	bool AllIndexesBoundOfType(const string &index_type) const;
	//! Overwrite this list with the other list.
	void Move(TableIndexList &other) DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
		D_ASSERT(this != &other);
		annotated_unique_lock lock(index_entries_lock, std::defer_lock);
		annotated_unique_lock other_lock(other.index_entries_lock, std::defer_lock);
		std::lock(lock, other_lock);
		D_ASSERT(index_entries.empty());
		index_entries = std::move(other.index_entries);
		unbound_count = other.unbound_count;
		other.unbound_count = 0;
	}
	//! Merge any changes added to deltas during a checkpoint back into the main indexes
	void MergeCheckpointDeltas(optional_idx checkpoint_id) const;
	//! Returns true, if all indexes
	//! Find the foreign key matching the keys.
	shared_ptr<IndexEntry> FindForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, const ForeignKeyType fk_type);
	//! Verify a foreign key constraint.
	void VerifyForeignKey(optional_ptr<const TableIndexList> delete_indexes, const vector<PhysicalIndex> &fk_keys,
	                      DataChunk &chunk, ConflictManager &conflict_manager);
	//! Returns the physical table columns referenced by any index.
	unordered_set<column_t> GetIndexedColumns() const;
	//! Returns the column sets of unique indexes matching the conflict target.
	vector<unordered_set<column_t>> GetConflictTargetColumns(const ConflictInfo &conflict_info) const;
	//! Get the combined column ids of the unique indexes.
	unordered_set<column_t> GetUniqueIndexColumns() const;
	//! Serialize all indexes of the table.
	IndexSerializationResult SerializeToDisk(QueryContext context, const IndexSerializationInfo &info);
	//! Serializes the index matching the name for the write-ahead log, if it exists.
	unique_ptr<IndexStorageInfo> SerializeToWAL(const Identifier &name, const case_insensitive_map_t<Value> &options);

public:
	//! Initialize an index_chunk from a table.
	static void InitializeIndexChunk(DataChunk &index_chunk, const vector<LogicalType> &table_types,
	                                 vector<StorageIndex> &mapped_column_ids, DataTableInfo &data_table_info);
	//! Reference the indexed columns of a table chunk.
	static void ReferenceIndexChunk(DataChunk &table_chunk, DataChunk &index_chunk,
	                                vector<StorageIndex> &mapped_column_ids);

private:
	template <class>
	friend class TableIndexIterationHelper;

	//! A lock to prevent any concurrent changes to the index entries.
	mutable annotated_mutex index_entries_lock;
	//! The index entries of the table.
	vector<shared_ptr<IndexEntry>> index_entries DUCKDB_GUARDED_BY(index_entries_lock);
	//! Contains the number of unbound indexes.
	idx_t unbound_count DUCKDB_GUARDED_BY(index_entries_lock) = 0;
};

template <class T>
class TableIndexIterationHelper {
public:
	explicit TableIndexIterationHelper(const TableIndexList &index_list);

private:
	annotated_unique_lock<annotated_mutex> lock;
	const vector<shared_ptr<IndexEntry>> &index_entries;

private:
	class TableIndexIterator {
	public:
		explicit TableIndexIterator(optional_ptr<const vector<shared_ptr<IndexEntry>>> index_entries);

		optional_ptr<const vector<shared_ptr<IndexEntry>>> index_entries;
		optional_idx index;

	public:
		TableIndexIterator &operator++();
		bool operator!=(const TableIndexIterator &other) const;
		T operator*() const;
	};

public:
	TableIndexIterator begin() { // NOLINT: match stl API
		return TableIndexIterator(&index_entries);
	}
	TableIndexIterator end() { // NOLINT: match stl API
		return TableIndexIterator(nullptr);
	}
};

template <>
shared_ptr<IndexEntry> TableIndexIterationHelper<shared_ptr<IndexEntry>>::TableIndexIterator::operator*() const;

} // namespace duckdb
