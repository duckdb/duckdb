//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/table_index_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/storage_lock.hpp"

#include <type_traits>

namespace duckdb {

class ConflictManager;
class ConflictInfo;
class IndexEntry;
class IndexEntryReadGuard;
class IndexEntryWriteGuard;
class IndexBinder;
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

//! IndexBindState transitions index binding phases while preventing lock order inversion.
enum class IndexBindState : uint8_t { UNBOUND, BINDING, BOUND };
//! Identifies a checkpoint or transaction delta owned by an IndexEntry.
enum class IndexEntryDelta : uint8_t {
	DELETED_ROWS_IN_USE,
	ADDED_DATA_DURING_CHECKPOINT,
	REMOVED_DATA_DURING_CHECKPOINT
};

//! IndexEntryReadGuard provides shared access to a stable physical index.
//! Other readers can access the entry concurrently, while replacement and entry-level mutations are blocked.
class IndexEntryReadGuard {
public:
	IndexEntryReadGuard(IndexEntryReadGuard &&) = default;
	IndexEntryReadGuard &operator=(IndexEntryReadGuard &&) = delete;

	template <class TARGET, class RESULT, class... METHOD_ARGS, class... CALL_ARGS>
	auto Invoke(RESULT (TARGET::*method)(METHOD_ARGS...) const, CALL_ARGS &&...args) const &;
	template <class TARGET, class RESULT, class... METHOD_ARGS, class... CALL_ARGS>
	auto Invoke(RESULT (TARGET::*method)(METHOD_ARGS...) const, CALL_ARGS &&...args) const && = delete;

	template <class TARGET, class RESULT, class... METHOD_ARGS, class... CALL_ARGS>
	auto InvokeDelta(IndexEntryDelta delta, RESULT (TARGET::*method)(METHOD_ARGS...) const,
	                 CALL_ARGS &&...args) const &;
	template <class TARGET, class RESULT, class... METHOD_ARGS, class... CALL_ARGS>
	auto InvokeDelta(IndexEntryDelta delta, RESULT (TARGET::*method)(METHOD_ARGS...) const,
	                 CALL_ARGS &&...args) const && = delete;

	bool HasDelta(IndexEntryDelta delta) const;
	bool ShouldUseDeltaIndexes(optional_idx active_checkpoint) const;

private:
	friend class IndexEntry;
	friend class IndexEntryWriteGuard;
	IndexEntryReadGuard(shared_ptr<IndexEntry> entry, unique_ptr<StorageLockKey> lock);

	const BoundIndex &GetDelta(IndexEntryDelta delta) const;

	//! Declared before lock so the lock is released before the entry can be destroyed.
	shared_ptr<IndexEntry> entry;
	unique_ptr<StorageLockKey> lock;
};

//! IndexEntryWriteGuard provides exclusive access to the physical index and checkpoint state.
class IndexEntryWriteGuard : public IndexEntryReadGuard {
public:
	IndexEntryWriteGuard(IndexEntryWriteGuard &&) = default;
	IndexEntryWriteGuard &operator=(IndexEntryWriteGuard &&) = delete;

	using IndexEntryReadGuard::Invoke;
	using IndexEntryReadGuard::InvokeDelta;

	template <class TARGET, class RESULT, class... METHOD_ARGS, class... CALL_ARGS>
	auto Invoke(RESULT (TARGET::*method)(METHOD_ARGS...), CALL_ARGS &&...args) &;
	template <class TARGET, class RESULT, class... METHOD_ARGS, class... CALL_ARGS>
	auto Invoke(RESULT (TARGET::*method)(METHOD_ARGS...), CALL_ARGS &&...args) && = delete;

	template <class TARGET, class RESULT, class... METHOD_ARGS, class... CALL_ARGS>
	auto InvokeDelta(IndexEntryDelta delta, RESULT (TARGET::*method)(METHOD_ARGS...), CALL_ARGS &&...args) &;
	template <class TARGET, class RESULT, class... METHOD_ARGS, class... CALL_ARGS>
	auto InvokeDelta(IndexEntryDelta delta, RESULT (TARGET::*method)(METHOD_ARGS...), CALL_ARGS &&...args) && = delete;

	void SetDelta(IndexEntryDelta delta, unique_ptr<BoundIndex> index);
	void ResetDelta(IndexEntryDelta delta);
	void MergeRemovedDataDuringCheckpoint();
	ErrorData MergeAddedDataDuringCheckpoint(IndexAppendMode append_mode);
	void MarkWrittenForCheckpoint(transaction_t checkpoint_id);
	void ReplaceIndex(unique_ptr<Index> index);

private:
	friend class IndexEntry;
	IndexEntryWriteGuard(shared_ptr<IndexEntry> entry, unique_ptr<StorageLockKey> lock);

	BoundIndex &GetDelta(IndexEntryDelta delta);
};

//! IndexEntry contains an atomic in addition to the index to ensure correct binding.
//! The IndexEntry provides a stable logical identity which refers to an interchangeable snapshot of an index.
class IndexEntry : public enable_shared_from_this<IndexEntry> {
public:
	explicit IndexEntry(unique_ptr<Index> index);

public:
	template <class TARGET, class FUNC>
	auto Read(FUNC &&func) const {
		lock.GetSharedLock();
		const auto &index = owned_index->Cast<TARGET>();
		return func(index);
	}
	//! Acquire shared access to a stable physical index.
	IndexEntryReadGuard ReadLock() {
		return IndexEntryReadGuard(shared_from_this(), lock.GetSharedLock());
	}
	//! Acquire exclusive access to the physical index and checkpoint state.
	IndexEntryWriteGuard WriteLock() {
		return IndexEntryWriteGuard(shared_from_this(), lock.GetExclusiveLock());
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
	mutable StorageLock lock;
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

template <class TARGET, class RESULT, class... METHOD_ARGS, class... CALL_ARGS>
auto IndexEntryReadGuard::Invoke(RESULT (TARGET::*method)(METHOD_ARGS...) const, CALL_ARGS &&...args) const & {
	static_assert(!std::is_pointer_v<RESULT>, "Locked index operations cannot return pointers");
	const auto &index = entry->owned_index->Cast<TARGET>();
	return (index.*method)(std::forward<CALL_ARGS>(args)...);
}

template <class TARGET, class RESULT, class... METHOD_ARGS, class... CALL_ARGS>
auto IndexEntryReadGuard::InvokeDelta(IndexEntryDelta delta, RESULT (TARGET::*method)(METHOD_ARGS...) const,
                                      CALL_ARGS &&...args) const & {
	static_assert(!std::is_pointer_v<RESULT>, "Locked index operations cannot return pointers");
	const auto &index = GetDelta(delta).Cast<TARGET>();
	return (index.*method)(std::forward<CALL_ARGS>(args)...);
}

template <class TARGET, class RESULT, class... METHOD_ARGS, class... CALL_ARGS>
auto IndexEntryWriteGuard::Invoke(RESULT (TARGET::*method)(METHOD_ARGS...), CALL_ARGS &&...args) & {
	static_assert(!std::is_pointer_v<RESULT>, "Locked index operations cannot return pointers");
	auto &index = entry->owned_index->Cast<TARGET>();
	return (index.*method)(std::forward<CALL_ARGS>(args)...);
}

template <class TARGET, class RESULT, class... METHOD_ARGS, class... CALL_ARGS>
auto IndexEntryWriteGuard::InvokeDelta(IndexEntryDelta delta, RESULT (TARGET::*method)(METHOD_ARGS...),
                                       CALL_ARGS &&...args) & {
	static_assert(!std::is_pointer_v<RESULT>, "Locked index operations cannot return pointers");
	auto &index = GetDelta(delta).Cast<TARGET>();
	return (index.*method)(std::forward<CALL_ARGS>(args)...);
}

struct IndexSerializationInfo {
	case_insensitive_map_t<Value> options;
	transaction_t checkpoint_id;
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
