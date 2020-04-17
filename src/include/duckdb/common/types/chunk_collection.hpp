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

namespace duckdb {

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
	ChunkCollection() : count(0) {
	}

	//! The total amount of elements in the collection
	idx_t count;
	//! The set of data chunks in the collection
	vector<unique_ptr<DataChunk>> chunks;
	//! The types of the ChunkCollection
	vector<TypeId> types;

	//! The amount of columns in the ChunkCollection
	idx_t column_count() {
		return types.size();
	}

	//! Append a new DataChunk directly to this ChunkCollection
	void Append(DataChunk &new_chunk);

	//! Append another ChunkCollection directly to this ChunkCollection
	void Append(ChunkCollection &other);

	void Verify();

	//! Gets the value of the column at the specified index
	Value GetValue(idx_t column, idx_t index);
	//! Sets the value of the column at the specified index
	void SetValue(idx_t column, idx_t index, Value value);

	vector<Value> GetRow(idx_t index);

	string ToString() const {
		return chunks.size() == 0 ? "ChunkCollection [ 0 ]"
		                          : "ChunkCollection [ " + std::to_string(count) + " ]: \n" + chunks[0]->ToString();
	}
	void Print();

	//! Gets a reference to the chunk at the given index
	DataChunk &GetChunk(idx_t index) {
		return *chunks[LocateChunk(index)];
	}

	void Sort(vector<OrderType> &desc, idx_t result[]);
	//! Reorders the rows in the collection according to the given indices. NB: order is changed!
	void Reorder(idx_t order[]);

	void MaterializeSortedChunk(DataChunk &target, idx_t order[], idx_t start_offset);

	//! Returns true if the ChunkCollections are equivalent
	bool Equals(ChunkCollection &other);

	//! Locates the chunk that belongs to the specific index
	idx_t LocateChunk(idx_t index) {
		idx_t result = index / STANDARD_VECTOR_SIZE;
		assert(result < chunks.size());
		return result;
	}

	void Heap(vector<OrderType> &desc, idx_t heap[], idx_t heap_size);
	idx_t MaterializeHeapChunk(DataChunk &target, idx_t order[], idx_t start_offset, idx_t heap_size);
};
} // namespace duckdb
