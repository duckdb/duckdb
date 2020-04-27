//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/data_chunk.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/vector.hpp"

#include <vector>

namespace duckdb {

//!  A Data Chunk represents a set of vectors.
/*!
    The data chunk class is the intermediate representation used by the
   execution engine of DuckDB. It effectively represents a subset of a relation.
   It holds a set of vectors that all have the same length.

    DataChunk is initialized using the DataChunk::Initialize function by
   providing it with a vector of TypeIds for the Vector members. By default,
   this function will also allocate a chunk of memory in the DataChunk for the
   vectors and all the vectors will be referencing vectors to the data owned by
   the chunk. The reason for this behavior is that the underlying vectors can
   become referencing vectors to other chunks as well (i.e. in the case an
   operator does not alter the data, such as a Filter operator which only adds a
   selection vector).

    In addition to holding the data of the vectors, the DataChunk also owns the
   selection vector that underlying vectors can point to.
*/
class DataChunk {
public:
	//! Creates an empty DataChunk
	DataChunk();

	//! The vectors owned by the DataChunk.
	vector<Vector> data;

public:
	idx_t size() const {
		return count;
	}
	idx_t column_count() const {
		return data.size();
	}
	void SetCardinality(idx_t count) {
		assert(count <= STANDARD_VECTOR_SIZE);
		this->count = count;
	}
	void SetCardinality(const DataChunk &other) {
		this->count = other.size();
	}

	Value GetValue(idx_t col_idx, idx_t index) const;
	void SetValue(idx_t col_idx, idx_t index, Value val);

	//! Set the DataChunk to reference another data chunk
	void Reference(DataChunk &chunk);

	//! Initializes the DataChunk with the specified types to an empty DataChunk
	//! This will create one vector of the specified type for each TypeId in the
	//! types list. The vector will be referencing vector to the data owned by
	//! the DataChunk.
	void Initialize(vector<TypeId> &types);
	//! Initializes an empty DataChunk with the given types. The vectors will *not* have any data allocated for them.
	void InitializeEmpty(vector<TypeId> &types);
	//! Append the other DataChunk to this one. The column count and types of
	//! the two DataChunks have to match exactly. Throws an exception if there
	//! is not enough space in the chunk.
	void Append(DataChunk &other);
	//! Destroy all data and columns owned by this DataChunk
	void Destroy();

	//! Copies the data from this vector to another vector.
	void Copy(DataChunk &other, idx_t offset = 0);

	//! Turn all the vectors from the chunk into flat vectors
	void Normalify();

	unique_ptr<VectorData[]> Orrify();

	void Slice(const SelectionVector &sel_vector, idx_t count);
	void Slice(DataChunk &other, const SelectionVector &sel, idx_t count, idx_t col_offset = 0);

	//! Resets the DataChunk to its state right after the DataChunk::Initialize
	//! function was called. This sets the count to 0, and resets each member
	//! Vector to point back to the data owned by this DataChunk.
	void Reset();

	//! Serializes a DataChunk to a stand-alone binary blob
	void Serialize(Serializer &serializer);
	//! Deserializes a blob back into a DataChunk
	void Deserialize(Deserializer &source);

	//! Hashes the DataChunk to the target vector
	void Hash(Vector &result);

	//! Returns a list of types of the vectors of this data chunk
	vector<TypeId> GetTypes();

	//! Converts this DataChunk to a printable string representation
	string ToString() const;
	void Print();

	DataChunk(const DataChunk &) = delete;

	//! Verify that the DataChunk is in a consistent, not corrupt state. DEBUG
	//! FUNCTION ONLY!
	void Verify();

private:
	idx_t count;
};
} // namespace duckdb
