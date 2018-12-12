//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/types/chunk_collection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "parser/query_node.hpp"

namespace duckdb {

//!  A ChunkCollection represents a set of DataChunks that all have the same
//!  types
/*!
    A ChunkCollection represents a set of DataChunks concatenated together in a
   list. Individual values of the collection can be iterated over using the
   iterator. It is also possible to iterate directly over the chunks for more
   direct access.
*/
class ChunkCollection : public Printable {
public:
	ChunkCollection() : count(0) {
	}

	//! The total amount of elements in the collection
	size_t count;
	//! The set of data chunks in the collection
	vector<unique_ptr<DataChunk>> chunks;
	//! The types of the ChunkCollection
	vector<TypeId> types;

	//! The amount of columns in the ChunkCollection
	size_t column_count() {
		return types.size();
	}

	//! Append a new DataChunk directly to this ChunkCollection
	void Append(DataChunk &new_chunk);

	//! Gets the value of the column at the specified index
	Value GetValue(size_t column, size_t index);
	//! Sets the value of the column at the specified index
	void SetValue(size_t column, size_t index, Value value);

	string ToString() const {
		return chunks.size() == 0 ? "ChunkCollection [ 0 ]"
		                          : "ChunkCollection [ " + std::to_string(count) + " ]: \n" + chunks[0]->ToString();
	}
	//! Gets a reference to the chunk at the given index
	DataChunk &GetChunk(size_t index) {
		return *chunks[LocateChunk(index)];
	}

	void Sort(OrderByDescription &desc, uint64_t result[]);
	//! Reorders the rows in the collection according to the given indices. NB: order is changed!
	void Reorder(uint64_t order[]);


private:
	//! Locates the chunk that belongs to the specific index
	size_t LocateChunk(size_t index) {
		size_t result = index / STANDARD_VECTOR_SIZE;
		assert(result < chunks.size());
		return result;
	}
};

} // namespace duckdb
