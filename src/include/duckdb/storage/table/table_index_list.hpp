//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/table_index_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/storage_lock.hpp"

#include <memory>

namespace duckdb {

class ConflictManager;
class ConflictInfo;
class IndexEntry;
template <class TARGET>
class IndexHandle;
template <class TARGET>
class MutableIndexHandle;
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
struct TableIndexIterationResult<IndexHandle<Index>> {
	using type = IndexHandle<Index>;
};

template <>
struct TableIndexIterationResult<MutableIndexHandle<Index>> {
	using type = MutableIndexHandle<Index>;
};

template <>
struct TableIndexIterationResult<shared_ptr<IndexEntry>> {
	using type = shared_ptr<IndexEntry>;
};

//! IndexBindState transitions index binding phases while preventing lock order inversion.
enum class IndexBindState : uint8_t { UNBOUND, BINDING, BOUND };
//! Owns the optional indexes used to represent transaction and checkpoint deltas for an IndexEntry.
class IndexDeltas {
public:
	optional_ptr<const BoundIndex> Find(IndexDeltaType type) const;
	optional_ptr<BoundIndex> Find(IndexDeltaType type);
	BoundIndex &GetOrCreate(BoundIndex &index, IndexDeltaType type);
	bool ShouldUse(optional_idx active_checkpoint) const;
	ErrorData MergeCheckpointDeltas(BoundIndex &index);
	void MarkWritten(transaction_t checkpoint_id);

private:
	const unique_ptr<BoundIndex> &GetPointer(IndexDeltaType type) const;
	unique_ptr<BoundIndex> &GetPointer(IndexDeltaType type);

	struct CheckpointDeltas {
		optional_idx last_written_checkpoint;
		unique_ptr<BoundIndex> added_data;
		unique_ptr<BoundIndex> removed_data;
	};

	unique_ptr<BoundIndex> deleted_rows_in_use;
	CheckpointDeltas checkpoint;
};

//! IndexReadHandle provides shared access to a stable physical index.
//! Other readers can access the entry concurrently, while replacement and entry-level mutations are blocked.
template <class TARGET>
class IndexHandle {
public:
	IndexHandle(IndexHandle &&) = default;
	IndexHandle &operator=(IndexHandle &&) = delete;

	const TARGET *operator->() const &;
	const TARGET *operator->() const && = delete;

	template <class OTHER>
	[[nodiscard]] IndexHandle<OTHER> Into() &&;
	template <class OTHER>
	IndexHandle<OTHER> Into() & = delete;

	template <class OTHER = BoundIndex>
	optional_ptr<const OTHER> FindDelta(IndexDeltaType type) const &;
	template <class OTHER = BoundIndex>
	optional_ptr<const OTHER> FindDelta(IndexDeltaType type) const && = delete;

private:
	friend class IndexEntry;
	template <class>
	friend class IndexHandle;
	template <class>
	friend class MutableIndexHandle;
	IndexHandle(shared_ptr<IndexEntry> entry, unique_ptr<StorageLockKey> lock);

	bool IsValid() const;
	const IndexEntry &GetEntry() const;

	//! Declared before lock so the lock is released before the entry can be destroyed.
	shared_ptr<IndexEntry> entry;
	unique_ptr<StorageLockKey> lock;
};

//! IndexWriteHandle provides exclusive access to the physical index and checkpoint state.
template <class TARGET>
class MutableIndexHandle : public IndexHandle<TARGET> {
public:
	MutableIndexHandle(MutableIndexHandle &&) = default;
	MutableIndexHandle &operator=(MutableIndexHandle &&) = delete;

	using IndexHandle<TARGET>::FindDelta;
	using IndexHandle<TARGET>::operator->;

	TARGET *operator->() &;
	TARGET *operator->() && = delete;

	template <class OTHER>
	[[nodiscard]] MutableIndexHandle<OTHER> Into() &&;
	template <class OTHER>
	MutableIndexHandle<OTHER> Into() & = delete;

	template <class OTHER = BoundIndex>
	optional_ptr<OTHER> FindDelta(IndexDeltaType type) &;
	template <class OTHER = BoundIndex>
	optional_ptr<OTHER> FindDelta(IndexDeltaType type) && = delete;

	bool ShouldUseDeltaIndexes(optional_idx active_checkpoint);
	BoundIndex &GetOrCreateDelta(IndexDeltaType type) &;
	BoundIndex &GetOrCreateDelta(IndexDeltaType type) && = delete;
	ErrorData MergeCheckpointDeltas();
	void MarkWrittenForCheckpoint(transaction_t checkpoint_id);
	void ReplaceIndex(unique_ptr<Index> index);

private:
	friend class IndexEntry;
	template <class>
	friend class MutableIndexHandle;
	MutableIndexHandle(shared_ptr<IndexEntry> entry, unique_ptr<StorageLockKey> lock);

	IndexEntry &GetMutableEntry();
};

//! IndexEntry contains an atomic in addition to the index to ensure correct binding.
//! The IndexEntry provides a stable logical identity which refers to an interchangeable snapshot of an index.
class IndexEntry : public enable_shared_from_this<IndexEntry> {
public:
	explicit IndexEntry(unique_ptr<Index> index);
	//! Append a chunk to the physical index, buffering it while the index is unbound.
	void Append(DataChunk &chunk, Vector &row_ids);
	//! Returns whether the physical index enforces a unique constraint.
	bool IsUnique() const;
	//! Vacuums the physical index if it is bound.
	void Vacuum();
	//! Verifies the buffers of the physical index and its delete delta.
	void VerifyBuffers();

public:
	//! Acquire shared access to a stable physical index.
	template <class TARGET = Index>
	IndexHandle<TARGET> GetHandle() {
		return IndexHandle<TARGET>(shared_from_this(), lock.GetSharedLock());
	}
	//! Acquire exclusive access to the physical index and checkpoint state.
	template <class TARGET = Index>
	MutableIndexHandle<TARGET> GetMutableHandle() {
		return MutableIndexHandle<TARGET>(shared_from_this(), lock.GetExclusiveLock());
	}
	IndexBindState GetBindState() const {
		return bind_state.load();
	}
	void SetBindState(IndexBindState state) {
		bind_state = state;
	}

private:
	template <class>
	friend class IndexHandle;
	template <class>
	friend class MutableIndexHandle;

	atomic<IndexBindState> bind_state;
	//! Phase-fair lock protecting the physical index and all delta indexes owned by this entry.
	mutable StorageLock lock;
	//! The physical index owned by this stable logical entry.
	unique_ptr<Index> owned_index;
	IndexDeltas deltas;
};

template <class TARGET>
IndexHandle<TARGET>::IndexHandle(shared_ptr<IndexEntry> entry_p, unique_ptr<StorageLockKey> lock_p)
    : entry(std::move(entry_p)), lock(std::move(lock_p)) {
	D_ASSERT(IsValid());
}

template <class TARGET>
bool IndexHandle<TARGET>::IsValid() const {
	D_ASSERT(bool(entry) == bool(lock));
	return entry != nullptr;
}

template <class TARGET>
const IndexEntry &IndexHandle<TARGET>::GetEntry() const {
	D_ASSERT(IsValid());
	return *entry;
}

template <class TARGET>
const TARGET *IndexHandle<TARGET>::operator->() const & {
	return std::addressof(GetEntry().owned_index->template Cast<TARGET>());
}

template <class TARGET>
template <class OTHER>
IndexHandle<OTHER> IndexHandle<TARGET>::Into() && {
	GetEntry().owned_index->template Cast<OTHER>();
	auto result = IndexHandle<OTHER>(std::move(entry), std::move(lock));
	D_ASSERT(!entry);
	D_ASSERT(!lock);
	return result;
}

template <class TARGET>
template <class OTHER>
optional_ptr<const OTHER> IndexHandle<TARGET>::FindDelta(IndexDeltaType type) const & {
	auto index = GetEntry().deltas.Find(type);
	if (!index) {
		return nullptr;
	}
	return index->template Cast<OTHER>();
}

template <class TARGET>
MutableIndexHandle<TARGET>::MutableIndexHandle(shared_ptr<IndexEntry> entry_p, unique_ptr<StorageLockKey> lock_p)
    : IndexHandle<TARGET>(std::move(entry_p), std::move(lock_p)) {
}

template <class TARGET>
IndexEntry &MutableIndexHandle<TARGET>::GetMutableEntry() {
	D_ASSERT(this->IsValid());
	return *this->entry;
}

template <class TARGET>
TARGET *MutableIndexHandle<TARGET>::operator->() & {
	return std::addressof(GetMutableEntry().owned_index->template Cast<TARGET>());
}

template <class TARGET>
template <class OTHER>
MutableIndexHandle<OTHER> MutableIndexHandle<TARGET>::Into() && {
	GetMutableEntry().owned_index->template Cast<OTHER>();
	auto result = MutableIndexHandle<OTHER>(std::move(this->entry), std::move(this->lock));
	D_ASSERT(!this->entry);
	D_ASSERT(!this->lock);
	return result;
}

template <class TARGET>
template <class OTHER>
optional_ptr<OTHER> MutableIndexHandle<TARGET>::FindDelta(IndexDeltaType type) & {
	auto index = GetMutableEntry().deltas.Find(type);
	if (!index) {
		return nullptr;
	}
	return index->template Cast<OTHER>();
}

template <class TARGET>
bool MutableIndexHandle<TARGET>::ShouldUseDeltaIndexes(optional_idx active_checkpoint) {
	return GetMutableEntry().deltas.ShouldUse(active_checkpoint);
}

template <class TARGET>
BoundIndex &MutableIndexHandle<TARGET>::GetOrCreateDelta(IndexDeltaType type) & {
	auto &index = GetMutableEntry().owned_index->template Cast<BoundIndex>();
	return GetMutableEntry().deltas.GetOrCreate(index, type);
}

template <class TARGET>
ErrorData MutableIndexHandle<TARGET>::MergeCheckpointDeltas() {
	return GetMutableEntry().deltas.MergeCheckpointDeltas(*this->operator->());
}

template <class TARGET>
void MutableIndexHandle<TARGET>::MarkWrittenForCheckpoint(transaction_t checkpoint_id) {
	GetMutableEntry().deltas.MarkWritten(checkpoint_id);
}

template <class TARGET>
void MutableIndexHandle<TARGET>::ReplaceIndex(unique_ptr<Index> index) {
	GetMutableEntry().owned_index = std::move(index);
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
	TableIndexIterationHelper<IndexHandle<Index>> IndexHandles() const;
	TableIndexIterationHelper<MutableIndexHandle<Index>> MutableIndexHandles() const;
	//! Iterates over shared ownership of stable index entries while holding the entry-list lock.
	TableIndexIterationHelper<shared_ptr<IndexEntry>> IndexEntries() const;
	//! Returns shared ownership of the stable logical index entries.
	vector<shared_ptr<IndexEntry>> GetEntries() const;
	//! Adds an index entry to the list of index entries.
	void AddIndex(unique_ptr<Index> index);
	//! Adds an empty copy of an index for transaction-local storage.
	void AddLocalIndex(const IndexHandle<BoundIndex> &source);
	//! Appends a chunk to all index entries.
	void Append(DataChunk &chunk, Vector &row_ids);
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
	//! Vacuums all bound indexes.
	void Vacuum();
	//! Verifies the buffers of all bound indexes and their delete deltas.
	void VerifyBuffers() const;
	//! Returns the set of distinct index types across all bound indexes.
	unordered_set<string> DistinctIndexTypes() const;
	//! Returns true if every index is bound and has the given type (vacuously true for an empty list).
	bool AllIndexesBoundOfType(const char *index_type) const;
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
IndexHandle<Index> TableIndexIterationHelper<IndexHandle<Index>>::TableIndexIterator::operator*() const;

template <>
MutableIndexHandle<Index> TableIndexIterationHelper<MutableIndexHandle<Index>>::TableIndexIterator::operator*() const;

template <>
shared_ptr<IndexEntry> TableIndexIterationHelper<shared_ptr<IndexEntry>>::TableIndexIterator::operator*() const;

} // namespace duckdb
