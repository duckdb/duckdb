//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/chunk_collection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {
class Allocator;
class ClientContext;

//!  A ChunkCollection represents a set of DataChunks that all have the same
//!  types
/*!
    A ChunkCollection represents a set of DataChunks concatenated together in a
   list. Individual values of the collection can be iterated over using the
   iterator. It is also possible to iterate directly over the chunks for more
   direct access.
*/
class ChunkCollection {
public:
	explicit ChunkCollection(Allocator &allocator);
	explicit ChunkCollection(ClientContext &context);

	//! The types of columns in the ChunkCollection
	vector<LogicalType> &Types() {
		return types;
	}
	const vector<LogicalType> &Types() const {
		return types;
	}

	//! The amount of rows in the ChunkCollection
	const idx_t &Count() const {
		return count;
	}

	//! The amount of columns in the ChunkCollection
	idx_t ColumnCount() const {
		return types.size();
	}

	//! Append a new DataChunk directly to this ChunkCollection
	DUCKDB_API void Append(DataChunk &new_chunk);

	//! Append a new DataChunk directly to this ChunkCollection
	DUCKDB_API void Append(unique_ptr<DataChunk> new_chunk);

	//! Append another ChunkCollection directly to this ChunkCollection
	DUCKDB_API void Append(ChunkCollection &other);

	//! Merge is like Append but messes up the order and destroys the other collection
	DUCKDB_API void Merge(ChunkCollection &other);

	//! Fuse adds new columns to the right of the collection
	DUCKDB_API void Fuse(ChunkCollection &other);

	DUCKDB_API void Verify();

	//! Gets the value of the column at the specified index
	DUCKDB_API Value GetValue(idx_t column, idx_t index);
	//! Sets the value of the column at the specified index
	DUCKDB_API void SetValue(idx_t column, idx_t index, const Value &value);

	//! Copy a single cell to a target vector
	DUCKDB_API void CopyCell(idx_t column, idx_t index, Vector &target, idx_t target_offset);

	DUCKDB_API string ToString() const;
	DUCKDB_API void Print() const;

	//! Gets a reference to the chunk at the given index
	DataChunk &GetChunkForRow(idx_t row_index) {
		return *chunks[LocateChunk(row_index)];
	}

	//! Gets a reference to the chunk at the given index
	DataChunk &GetChunk(idx_t chunk_index) {
		D_ASSERT(chunk_index < chunks.size());
		return *chunks[chunk_index];
	}
	const DataChunk &GetChunk(idx_t chunk_index) const {
		D_ASSERT(chunk_index < chunks.size());
		return *chunks[chunk_index];
	}

	const vector<unique_ptr<DataChunk>> &Chunks() {
		return chunks;
	}

	idx_t ChunkCount() const {
		return chunks.size();
	}

	void Reset() {
		count = 0;
		chunks.clear();
		types.clear();
	}

	unique_ptr<DataChunk> Fetch() {
		if (ChunkCount() == 0) {
			return nullptr;
		}

		auto res = std::move(chunks[0]);
		chunks.erase(chunks.begin() + 0);
		return res;
	}

	//! Locates the chunk that belongs to the specific index
	idx_t LocateChunk(idx_t index) {
		idx_t result = index / STANDARD_VECTOR_SIZE;
		D_ASSERT(result < chunks.size());
		return result;
	}

	Allocator &GetAllocator() {
		return allocator;
	}

private:
	Allocator &allocator;
	//! The total amount of elements in the collection
	idx_t count;
	//! The set of data chunks in the collection
	vector<unique_ptr<DataChunk>> chunks;
	//! The types of the ChunkCollection
	vector<LogicalType> types;
};
} // namespace duckdb
