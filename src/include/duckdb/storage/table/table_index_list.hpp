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
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/storage_lock.hpp"

namespace duckdb {

class ConflictManager;
class IndexEntry;
class IndexEntryReadGuard;
class IndexEntryWriteGuard;
class LocalTableStorage;
struct IndexStorageInfo;
struct DataTableInfo;
template <class T>
class TableIndexIterationHelper;

template <class T>
struct TableIndexIterationResult {
	using type = T &;
};

template <>
struct TableIndexIterationResult<IndexEntryReadGuard> {
	using type = IndexEntryReadGuard;
};

template <>
struct TableIndexIterationResult<IndexEntryWriteGuard> {
	using type = IndexEntryWriteGuard;
};

//! IndexBindState to transition index binding phases preventing lock order inversion.
enum class IndexBindState : uint8_t { UNBOUND, BINDING, BOUND };

//! IndexEntryReadGuard provides shared access to a stable physical index.
//! Other readers can access the entry concurrently, while replacement and entry-level mutations are blocked.
class IndexEntryReadGuard {
public:
	IndexEntryReadGuard(IndexEntryReadGuard &&) = default;
	IndexEntryReadGuard &operator=(IndexEntryReadGuard &&) = delete;

	Index &GetIndex() &;
	const Index &GetIndex() const &;
	Index &GetIndex() && = delete;
	const Index &GetIndex() const && = delete;
	optional_ptr<BoundIndex> DeletedRowsInUse() const;
	optional_ptr<BoundIndex> AddedDataDuringCheckpoint() const;
	optional_ptr<BoundIndex> RemovedDataDuringCheckpoint() const;
	bool ShouldUseDeltaIndexes(optional_idx active_checkpoint) const;

protected:
	friend class IndexEntry;
	IndexEntryReadGuard(IndexEntry &entry, unique_ptr<StorageLockKey> lock);

protected:
	unique_ptr<StorageLockKey> lock;
	IndexEntry &entry;
};

//! IndexEntryWriteGuard provides exclusive access to the physical index and checkpoint state.
class IndexEntryWriteGuard : public IndexEntryReadGuard {
public:
	IndexEntryWriteGuard(IndexEntryWriteGuard &&) = default;
	IndexEntryWriteGuard &operator=(IndexEntryWriteGuard &&) = delete;

	unique_ptr<BoundIndex> &DeletedRowsInUse();
	unique_ptr<BoundIndex> &AddedDataDuringCheckpoint();
	unique_ptr<BoundIndex> &RemovedDataDuringCheckpoint();
	void MarkWrittenForCheckpoint(transaction_t checkpoint_id);
	void ReplaceIndex(unique_ptr<Index> index);

private:
	friend class IndexEntry;
	IndexEntryWriteGuard(IndexEntry &entry, unique_ptr<StorageLockKey> lock);
};

//! IndexEntry contains an atomic in addition to the index to ensure correct binding.
//! The IndexEntry provides a stable logical identity which refers to an interchangeable snapshot of an index.
class IndexEntry {
public:
	explicit IndexEntry(unique_ptr<Index> index);

public:
	//! Acquire shared access to a stable physical index.
	IndexEntryReadGuard ReadLock() {
		return IndexEntryReadGuard(*this, lock.GetSharedLock());
	}
	//! Acquire exclusive access to the physical index and checkpoint state.
	IndexEntryWriteGuard WriteLock() {
		return IndexEntryWriteGuard(*this, lock.GetExclusiveLock());
	}
	IndexBindState GetBindState() const {
		return bind_state.load();
	}
	void SetBindState(IndexBindState state) {
		bind_state = state;
	}

private:
	friend class IndexEntryReadGuard;
	friend class IndexEntryWriteGuard;
	atomic<IndexBindState> bind_state;
	//! Phase-fair lock protecting the physical index and all delta indexes owned by this entry.
	StorageLock lock;
	//! The last checkpoint index that was written with this index
	optional_idx last_written_checkpoint;
	//! The physical index owned by this stable logical entry.
	unique_ptr<Index> owned_index;
	unique_ptr<BoundIndex> deleted_rows_in_use;
	//! Data that was added to the index during the last checkpoint
	unique_ptr<BoundIndex> added_data_during_checkpoint;
	//! Data that was removed from the index during the last checkpoint
	unique_ptr<BoundIndex> removed_data_during_checkpoint;
};

struct IndexSerializationInfo {
	case_insensitive_map_t<Value> options;
	transaction_t checkpoint_id;
};

// When serializing indexes, new IndexStorageInfos are created upon BoundIndex serialization, whereas for
// UnboundIndex, IndexStorageInfo already exists inside the UnboundIndex.
// We want to serialize IndexStorageInfo's in the same order that we serialized indexes, which is stored as
// a vector of references in the ordered_infos field here.
// UnboundIndexes still "own" the IndexStorageInfo and so a reference can just be directly pushed.
// For BoundIndexes, however, we need to keep the newly created IndexStorageInfo's alive, and so they
// are stored in this result type. When a BoundIndex is added to bound_infos, a reference to this is then
// pushed to ordered_infos.
struct IndexSerializationResult {
	//! The ordered list of references to serialize - preserves iteration order of index_entries
	vector<reference<const IndexStorageInfo>> ordered_infos;
	//! Storage for bound index infos to keep them alive.
	vector<IndexStorageInfo> bound_infos;
};

class TableIndexList {
public:
	TableIndexIterationHelper<IndexEntry> IndexEntries() const;
	TableIndexIterationHelper<IndexEntryReadGuard> ReadLockedIndexes() const;
	TableIndexIterationHelper<IndexEntryWriteGuard> WriteLockedIndexes() const;
	//! Returns shared ownership of the stable logical index entries.
	vector<shared_ptr<IndexEntry>> GetEntries() const;
	//! Adds an index entry to the list of index entries.
	void AddIndex(unique_ptr<Index> index);
	//! Removes an index entry from the list of index entries and release any storage the index owns.
	void RemoveIndex(const Identifier &name);
	//! Returns true, if the index name does not exist.
	bool NameIsUnique(const string &name) const;
	//! Returns shared ownership of the stable logical index entry matching the name.
	shared_ptr<IndexEntry> FindEntry(const Identifier &name) const;
	//! Binds unbound indexes possibly present after loading an extension.
	void Bind(ClientContext &context, DataTableInfo &table_info, const char *index_type = nullptr);
	//! Returns true, if there are no index entries.
	bool Empty() const {
		return Count() == 0;
	}
	//! Returns the number of index entries.
	idx_t Count() const {
		lock_guard<mutex> lock(index_entries_lock);
		return index_entries.size();
	}
	//! Returns true, if there are unbound indexes.
	bool HasUnbound() const {
		lock_guard<mutex> lock(index_entries_lock);
		return unbound_count != 0;
	}
	//! Returns the set of distinct index types across all bound indexes.
	unordered_set<string> DistinctIndexTypes() const;
	//! Returns true if every index is bound and has the given type (vacuously true for an empty list).
	bool AllIndexesBoundOfType(const char *index_type) const;
	//! Overwrite this list with the other list.
	void Move(TableIndexList &other) {
		D_ASSERT(index_entries.empty());
		index_entries = std::move(other.index_entries);
	}
	//! Merge any changes added to deltas during a checkpoint back into the main indexes
	void MergeCheckpointDeltas(transaction_t checkpoint_id) const;
	//! Returns true, if all indexes
	//! Find the foreign key matching the keys.
	shared_ptr<IndexEntry> FindForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, const ForeignKeyType fk_type);
	//! Verify a foreign key constraint.
	void VerifyForeignKey(optional_ptr<LocalTableStorage> storage, const vector<PhysicalIndex> &fk_keys,
	                      DataChunk &chunk, ConflictManager &conflict_manager);
	//! Get the combined column ids of the indexes.
	unordered_set<column_t> GetRequiredColumns() const;
	//! Serialize all indexes of the table.
	IndexSerializationResult SerializeToDisk(QueryContext context, const IndexSerializationInfo &info);

public:
	//! Initialize an index_chunk from a table.
	static void InitializeIndexChunk(DataChunk &index_chunk, const vector<LogicalType> &table_types,
	                                 vector<StorageIndex> &mapped_column_ids, DataTableInfo &data_table_info);
	//! Reference the indexed columns of a table chunk.
	static void ReferenceIndexChunk(DataChunk &table_chunk, DataChunk &index_chunk,
	                                vector<StorageIndex> &mapped_column_ids);

private:
	//! A lock to prevent any concurrent changes to the index entries.
	mutable mutex index_entries_lock;
	//! The index entries of the table.
	vector<shared_ptr<IndexEntry>> index_entries;
	//! Contains the number of unbound indexes.
	idx_t unbound_count = 0;
};

template <class T>
class TableIndexIterationHelper {
public:
	TableIndexIterationHelper(mutex &index_lock, const vector<shared_ptr<IndexEntry>> &index_entries);

private:
	unique_lock<mutex> lock;
	const vector<shared_ptr<IndexEntry>> &index_entries;

private:
	class TableIndexIterator {
	public:
		explicit TableIndexIterator(optional_ptr<const vector<shared_ptr<IndexEntry>>> index_entries);

		using result_type = typename TableIndexIterationResult<T>::type;

		optional_ptr<const vector<shared_ptr<IndexEntry>>> index_entries;
		optional_idx index;

	public:
		TableIndexIterator &operator++();
		bool operator!=(const TableIndexIterator &other) const;
		result_type operator*() const;
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
IndexEntry &TableIndexIterationHelper<IndexEntry>::TableIndexIterator::operator*() const;

template <>
IndexEntryReadGuard TableIndexIterationHelper<IndexEntryReadGuard>::TableIndexIterator::operator*() const;

template <>
IndexEntryWriteGuard TableIndexIterationHelper<IndexEntryWriteGuard>::TableIndexIterator::operator*() const;

} // namespace duckdb
