//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/column_data_collection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/mutex.hpp"
#include <functional>

namespace duckdb {
class BufferManager;
class BlockHandle;
class ClientContext;
struct ColumnDataCopyFunction;
class ColumnDataAllocator;
class ColumnDataCollection;
class ColumnDataCollectionSegment;
class ColumnDataRowCollection;

enum class ColumnDataAllocatorType { BUFFER_MANAGER_ALLOCATOR, IN_MEMORY_ALLOCATOR };

struct ChunkManagementState {
	unordered_map<idx_t, BufferHandle> handles;
};

struct ColumnDataAppendState {
	ChunkManagementState current_chunk_state;
	vector<VectorData> vector_data;
};

struct ColumnDataScanState {
	ChunkManagementState current_chunk_state;
	idx_t segment_index;
	idx_t chunk_index;
	idx_t current_row_index;
	idx_t next_row_index;
	vector<column_t> column_ids;
};

struct ColumnDataParallelScanState {
	ColumnDataScanState scan_state;
	mutex lock;
};

struct ColumnDataLocalScanState {
	ChunkManagementState current_chunk_state;
	idx_t current_row_index;
};

class ColumnDataRow {
public:
	ColumnDataRow(DataChunk &chunk, idx_t row_index, idx_t base_index);

	DataChunk &chunk;
	idx_t row_index;
	idx_t base_index;

public:
	Value GetValue(idx_t column_index) const;
	idx_t RowIndex() const;
};

//! The ColumnDataCollection represents a set of (buffer-managed) data stored in columnar format
//! It is efficient to read and scan
class ColumnDataCollection {
public:
	//! Constructs an in-memory column data collection from an allocator
	ColumnDataCollection(Allocator &allocator, vector<LogicalType> types);
	//! Constructs a buffer-managed column data collection
	ColumnDataCollection(BufferManager &buffer_manager, vector<LogicalType> types);
	//! Constructs either an in-memory or a buffer-managed column data collection
	ColumnDataCollection(ClientContext &context, vector<LogicalType> types,
	                     ColumnDataAllocatorType type = ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);
	//! Creates a column data collection that inherits the blocks to write to. This allows blocks to be shared
	//! between multiple column data collections and prevents wasting space.
	//! Note that after one CDC inherits blocks from another, the other
	//! cannot be written to anymore (i.e. we take ownership of the half-written blocks).
	ColumnDataCollection(ColumnDataCollection &parent);
	ColumnDataCollection(shared_ptr<ColumnDataAllocator> allocator, vector<LogicalType> types);
	~ColumnDataCollection();

public:
	DUCKDB_API vector<LogicalType> &Types() {
		return types;
	}
	DUCKDB_API const vector<LogicalType> &Types() const {
		return types;
	}

	//! The amount of rows in the ColumnDataCollection
	DUCKDB_API const idx_t &Count() const {
		return count;
	}

	//! The amount of columns in the ColumnDataCollection
	DUCKDB_API idx_t ColumnCount() const {
		return types.size();
	}

	//! Initializes an Append state - useful for optimizing many appends made to the same column data collection
	DUCKDB_API void InitializeAppend(ColumnDataAppendState &state);
	//! Append a DataChunk to this ColumnDataCollection using the specified append state
	DUCKDB_API void Append(ColumnDataAppendState &state, DataChunk &new_chunk);

	//! Initializes a chunk with the correct types that can be used to call Scan
	DUCKDB_API void InitializeScanChunk(DataChunk &chunk) const;
	//! Initializes a chunk with the correct types for a given scan state
	DUCKDB_API void InitializeScanChunk(ColumnDataScanState &state, DataChunk &chunk) const;
	//! Initializes a Scan state for scanning all columns
	DUCKDB_API void InitializeScan(ColumnDataScanState &state) const;
	//! Initializes a Scan state for scanning a subset of the columns
	DUCKDB_API void InitializeScan(ColumnDataScanState &state, vector<column_t> column_ids) const;
	//! Initialize a parallel scan over the column data collection over all columns
	DUCKDB_API void InitializeScan(ColumnDataParallelScanState &state) const;
	//! Initialize a parallel scan over the column data collection over a subset of the columns
	DUCKDB_API void InitializeScan(ColumnDataParallelScanState &state, vector<column_t> column_ids) const;
	//! Scans a DataChunk from the ColumnDataCollection
	DUCKDB_API bool Scan(ColumnDataScanState &state, DataChunk &result) const;
	//! Scans a DataChunk from the ColumnDataCollection
	DUCKDB_API bool Scan(ColumnDataParallelScanState &state, ColumnDataLocalScanState &lstate, DataChunk &result) const;

	//! Performs a scan of the ColumnDataCollection, invoking the callback for each chunk
	DUCKDB_API void Scan(const std::function<void(DataChunk &)> &callback);
	//! Performs a scan of the ColumnDataCollection, projecting the specified columns
	DUCKDB_API void Scan(vector<column_t> column_ids, const std::function<void(DataChunk &)> &callback);

	//! Append a DataChunk directly to this ColumnDataCollection - calls InitializeAppend and Append internally
	DUCKDB_API void Append(DataChunk &new_chunk);

	//! Appends the other ColumnDataCollection to this, destroying the other data collection
	DUCKDB_API void Combine(ColumnDataCollection &other);

	DUCKDB_API void Verify();

	DUCKDB_API string ToString() const;
	DUCKDB_API void Print() const;

	DUCKDB_API void Reset();

	DUCKDB_API idx_t ChunkCount() const;
	DUCKDB_API void FetchChunk(idx_t chunk_idx, DataChunk &result) const;

	//! Returns a set of all of the rows in the column data collection
	//! Note that usage of this is slow - avoid using this unless the amount of rows is small, or if you do not care
	//! about performance
	DUCKDB_API ColumnDataRowCollection GetRows() const;

	//! Compare two column data collections to another. If they are equal according to result equality rules,
	//! return true. That means null values are equal, and approx equality is used for floating point values.
	//! If they are not equal, return false and fill in the error message.
	static bool ResultEquals(const ColumnDataCollection &left, const ColumnDataCollection &right,
	                         string &error_message);

private:
	//! Initialize the column data collection
	void Initialize(vector<LogicalType> types);

	//! Creates a new segment within the ColumnDataCollection
	void CreateSegment();

	static ColumnDataCopyFunction GetCopyFunction(const LogicalType &type);

	//! Obtains the next scan index to scan from
	bool NextScanIndex(ColumnDataScanState &state, idx_t &chunk_index, idx_t &segment_index, idx_t &row_index) const;

private:
	//! The Column Data Allocator
	buffer_ptr<ColumnDataAllocator> allocator;
	//! The types of the stored entries
	vector<LogicalType> types;
	//! The number of entries stored in the column data collection
	idx_t count;
	//! The data segments of the column data collection
	vector<unique_ptr<ColumnDataCollectionSegment>> segments;
	//! The set of copy functions
	vector<ColumnDataCopyFunction> copy_functions;
	//! When the column data collection is marked as finished - new tuples can no longer be appended to it
	bool finished_append;

private:
	class ColumnDataRowIterator;

	class ColumnDataRowIterator {
	public:
		explicit ColumnDataRowIterator(ColumnDataCollection *collection_p);

		ColumnDataCollection *collection;
		ColumnDataScanState scan_state;
		shared_ptr<DataChunk> scan_chunk;
		ColumnDataRow current_row;

	public:
		void Next();

		ColumnDataRowIterator &operator++();
		bool operator!=(const ColumnDataRowIterator &other) const;
		const ColumnDataRow &operator*() const;
	};

public:
	DUCKDB_API ColumnDataRowIterator begin() {
		return ColumnDataRowIterator(this);
	}
	DUCKDB_API ColumnDataRowIterator end() {
		return ColumnDataRowIterator(nullptr);
	}
};

class ColumnDataRowCollection {
public:
	DUCKDB_API ColumnDataRowCollection(const ColumnDataCollection &collection);

public:
	DUCKDB_API Value GetValue(idx_t column, idx_t index) const;

public:
	// container API
	DUCKDB_API bool empty() const;
	DUCKDB_API idx_t size() const;

	DUCKDB_API ColumnDataRow &operator[](idx_t i);
	DUCKDB_API const ColumnDataRow &operator[](idx_t i) const;

	DUCKDB_API vector<ColumnDataRow>::iterator begin() {
		return rows.begin();
	}
	DUCKDB_API vector<ColumnDataRow>::iterator end() {
		return rows.end();
	}
	DUCKDB_API vector<ColumnDataRow>::const_iterator cbegin() const {
		return rows.cbegin();
	}
	DUCKDB_API vector<ColumnDataRow>::const_iterator cend() const {
		return rows.cend();
	}
	DUCKDB_API vector<ColumnDataRow>::const_iterator begin() const {
		return rows.begin();
	}
	DUCKDB_API vector<ColumnDataRow>::const_iterator end() const {
		return rows.end();
	}

private:
	vector<ColumnDataRow> rows;
	vector<unique_ptr<DataChunk>> chunks;
};

} // namespace duckdb
