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

struct VectorMetaData {
	//! Where the vector data lives
	uint32_t block_id;
	uint32_t offset;
	//! The number of entries present in this vector
	uint16_t count;

	//! Child of this vector (used only for lists and structs)
	idx_t child_data = -1;
	//! Next vector entry (in case there is more data - used only in case of children of lists)
	idx_t next_data = -1;
};

struct ChunkMetaData {
	//! The set of vectors of the chunk
	vector<idx_t> vector_data;
	//! The block ids referenced by the chunk
	unordered_set<uint32_t> block_ids;
	//! The number of entries in the chunk
	uint16_t count;
};

struct BlockMetaData {
	//! The underlying block handle
	shared_ptr<BlockHandle> handle;
	//! How much space is currently used within the block
	uint32_t size;
	//! How much space is available in the block
	uint32_t capacity;

	uint32_t Capacity();
};

struct InternalColumnData {
	//! The number of entries in the internal column data
	idx_t count;
	//! The set of blocks used by the column data collection
	vector<BlockMetaData> blocks;
	//! Set of chunk meta data
	vector<ChunkMetaData> chunk_data;
	//! Set of vector meta data
	vector<VectorMetaData> vector_data;
	//! The string heap for the column data collection
	StringHeap heap;
};

struct ChunkManagementState {
	unordered_map<idx_t, unique_ptr<BufferHandle>> handles;
};

struct ColumnDataAppendState {
	ChunkManagementState current_chunk_state;
	DataChunk append_chunk;
};

struct ColumnDataScanState {
	ChunkManagementState current_chunk_state;
	idx_t internal_data_index;
	idx_t chunk_index;
};

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
	void CreateInternalData();
	void AllocateNewChunk(InternalColumnData &idata);
	idx_t AllocateVector(InternalColumnData &idata, const LogicalType &type, ChunkMetaData &chunk_data);

	void AllocateBlock(InternalColumnData &idata);
	void AllocateData(InternalColumnData &idata, idx_t size, uint32_t &block_id, uint32_t &offset);

	void InitializeChunkState(InternalColumnData &idata, idx_t chunk_index, ChunkManagementState &state);
	void InitializeChunk(InternalColumnData &idata, idx_t chunk_index, ChunkManagementState &state, DataChunk &chunk);

	void InitializeVector(ChunkManagementState &state, VectorMetaData &vdata, Vector &result);

private:
	//! BufferManager
	BufferManager &buffer_manager;
	//! The types of the stored entries
	vector<LogicalType> types;
	//! The number of entries stored in the column data collection
	idx_t count;
	//! The internal meta data of the column data collection
	vector<InternalColumnData> internal_data;
};

} // namespace duckdb
