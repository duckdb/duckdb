//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/types/chunk_collection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/enums/order_type.hpp"
#include "common/types/data_chunk.hpp"

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
	index_t count;
	//! The set of data chunks in the collection
	vector<unique_ptr<DataChunk>> chunks;
	//! The types of the ChunkCollection
	vector<TypeId> types;

	//! The amount of columns in the ChunkCollection
	index_t column_count() {
		return types.size();
	}

	//! Append a new DataChunk directly to this ChunkCollection
	void Append(DataChunk &new_chunk);

	//! Gets the value of the column at the specified index
	Value GetValue(index_t column, index_t index);
	//! Sets the value of the column at the specified index
	void SetValue(index_t column, index_t index, Value value);

	vector<Value> GetRow(index_t index);

	string ToString() const {
		return chunks.size() == 0 ? "ChunkCollection [ 0 ]"
		                          : "ChunkCollection [ " + std::to_string(count) + " ]: \n" + chunks[0]->ToString();
	}
	void Print();

	//! Gets a reference to the chunk at the given index
	DataChunk &GetChunk(index_t index) {
		return *chunks[LocateChunk(index)];
	}

	void Sort(vector<OrderType> &desc, index_t result[]);
	//! Reorders the rows in the collection according to the given indices. NB: order is changed!
	void Reorder(index_t order[]);

	void MaterializeSortedChunk(DataChunk &target, index_t order[], index_t start_offset);

	//! Returns true if the ChunkCollections are equivalent
	bool Equals(ChunkCollection &other);

	//! Locates the chunk that belongs to the specific index
	index_t LocateChunk(index_t index) {
		index_t result = index / STANDARD_VECTOR_SIZE;
		assert(result < chunks.size());
		return result;
	}
};

} // namespace duckdb
