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

namespace duckdb {
class BufferManager;
class BlockHandle;
class ClientContext;

struct ChunkManagementState {
	unordered_map<idx_t, unique_ptr<BufferHandle>> handles;
};

struct ColumnDataAppendState {
	ChunkManagementState current_chunk_state;
	vector<VectorData> vector_data;
};

struct ColumnDataScanState {
	ChunkManagementState current_chunk_state;
	idx_t segment_index;
	idx_t chunk_index;
};

struct ColumnDataCopyFunction;
class ColumnDataCollectionSegment;

//! The ColumnDataCollection represents a set of (buffer-managed) data stored in columnar format
//! It is efficient to read and scan
class ColumnDataCollection {
public:
	ColumnDataCollection(BufferManager &buffer_manager, vector<LogicalType> types);
	ColumnDataCollection(ClientContext &context, vector<LogicalType> types);
	~ColumnDataCollection();

public:
	//! The amount of columns in the ChunkCollection
	DUCKDB_API vector<LogicalType> &Types() {
		return types;
	}
	const vector<LogicalType> &Types() const {
		return types;
	}

	//! The amount of rows in the ChunkCollection
	DUCKDB_API const idx_t &Count() const {
		return count;
	}

	//! The amount of columns in the ChunkCollection
	DUCKDB_API idx_t ColumnCount() const {
		return types.size();
	}

	//! Initializes an Append state - useful for optimizing many appends made to the same column data collection
	DUCKDB_API void InitializeAppend(ColumnDataAppendState &state);
	//! Append a DataChunk to this ColumnDataCollection using the specified append state
	DUCKDB_API void Append(ColumnDataAppendState &state, DataChunk &new_chunk);

	//! Initializes a Scan state
	DUCKDB_API void InitializeScan(ColumnDataScanState &state);
	//! Scans a DataChunk from the ColumnDataCollection
	DUCKDB_API void Scan(ColumnDataScanState &state, DataChunk &result);

	//! Append a DataChunk directly to this ColumnDataCollection - calls InitializeAppend and Append internally
	DUCKDB_API void Append(DataChunk &new_chunk);

	//! Appends the other ColumnDataCollection to this, destroying the other data collection
	DUCKDB_API void Combine(ColumnDataCollection &other);

	DUCKDB_API void Verify();

	DUCKDB_API string ToString() const;
	DUCKDB_API void Print() const;

	DUCKDB_API idx_t ChunkCount() const;

	DUCKDB_API void Reset();

private:
	//! Creates a new segment within the ColumnDataCollection
	void CreateSegment();

	static ColumnDataCopyFunction GetCopyFunction(const LogicalType &type);

private:
	//! BufferManager
	BufferManager &buffer_manager;
	//! The types of the stored entries
	vector<LogicalType> types;
	//! The number of entries stored in the column data collection
	idx_t count;
	//! The data segments of the column data collection
	vector<ColumnDataCollectionSegment> segments;
	//! The set of copy functions
	vector<ColumnDataCopyFunction> copy_functions;
};

} // namespace duckdb
