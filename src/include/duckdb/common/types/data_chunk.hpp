//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/data_chunk.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {
class Allocator;
class ClientContext;
class ExecutionContext;
class VectorCache;
class Serializer;
class Deserializer;

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
	DUCKDB_API DataChunk();
	DUCKDB_API ~DataChunk();

	//! The vectors owned by the DataChunk.
	vector<Vector> data;

public:
	inline idx_t size() const { // NOLINT
		return count;
	}
	inline idx_t ColumnCount() const {
		return data.size();
	}
	inline void SetCardinality(idx_t count_p) {
		D_ASSERT(count_p <= capacity);
		this->count = count_p;
	}
	inline void SetCardinality(const DataChunk &other) {
		SetCardinality(other.size());
	}
	inline void SetCapacity(idx_t capacity_p) {
		this->capacity = capacity_p;
	}
	inline void SetCapacity(const DataChunk &other) {
		SetCapacity(other.capacity);
	}

	DUCKDB_API Value GetValue(idx_t col_idx, idx_t index) const;
	DUCKDB_API void SetValue(idx_t col_idx, idx_t index, const Value &val);

	//! Returns true if all vectors in the DataChunk are constant
	DUCKDB_API bool AllConstant() const;

	//! Set the DataChunk to reference another data chunk
	DUCKDB_API void Reference(DataChunk &chunk);
	//! Set the DataChunk to own the data of data chunk, destroying the other chunk in the process
	DUCKDB_API void Move(DataChunk &chunk);

	//! Initializes the DataChunk with the specified types to an empty DataChunk
	//! This will create one vector of the specified type for each LogicalType in the
	//! types list. The vector will be referencing vector to the data owned by
	//! the DataChunk.
	DUCKDB_API void Initialize(Allocator &allocator, const vector<LogicalType> &types,
	                           idx_t capacity = STANDARD_VECTOR_SIZE);
	DUCKDB_API void Initialize(ClientContext &context, const vector<LogicalType> &types,
	                           idx_t capacity = STANDARD_VECTOR_SIZE);
	//! Initializes an empty DataChunk with the given types. The vectors will *not* have any data allocated for them.
	DUCKDB_API void InitializeEmpty(const vector<LogicalType> &types);

	DUCKDB_API void InitializeEmpty(vector<LogicalType>::const_iterator begin, vector<LogicalType>::const_iterator end);
	DUCKDB_API void Initialize(Allocator &allocator, vector<LogicalType>::const_iterator begin,
	                           vector<LogicalType>::const_iterator end, idx_t capacity = STANDARD_VECTOR_SIZE);
	DUCKDB_API void Initialize(ClientContext &context, vector<LogicalType>::const_iterator begin,
	                           vector<LogicalType>::const_iterator end, idx_t capacity = STANDARD_VECTOR_SIZE);

	//! Append the other DataChunk to this one. The column count and types of
	//! the two DataChunks have to match exactly. Throws an exception if there
	//! is not enough space in the chunk and resize is not allowed.
	DUCKDB_API void Append(const DataChunk &other, bool resize = false, SelectionVector *sel = nullptr,
	                       idx_t count = 0);

	//! Destroy all data and columns owned by this DataChunk
	DUCKDB_API void Destroy();

	//! Copies the data from this vector to another vector.
	DUCKDB_API void Copy(DataChunk &other, idx_t offset = 0) const;
	DUCKDB_API void Copy(DataChunk &other, const SelectionVector &sel, const idx_t source_count,
	                     const idx_t offset = 0) const;

	//! Splits the DataChunk in two
	DUCKDB_API void Split(DataChunk &other, idx_t split_idx);

	//! Fuses a DataChunk onto the right of this one, and destroys the other. Inverse of Split.
	DUCKDB_API void Fuse(DataChunk &other);

	//! Makes this DataChunk reference the specified columns in the other DataChunk
	DUCKDB_API void ReferenceColumns(DataChunk &other, const vector<column_t> &column_ids);

	//! Turn all the vectors from the chunk into flat vectors
	DUCKDB_API void Flatten();

	// FIXME: this is DUCKDB_API, might need conversion back to regular unique ptr?
	DUCKDB_API unsafe_unique_array<UnifiedVectorFormat> ToUnifiedFormat();

	DUCKDB_API void Slice(const SelectionVector &sel_vector, idx_t count);

	//! Slice all Vectors from other.data[i] to data[i + 'col_offset']
	//! Turning all Vectors into Dictionary Vectors, using 'sel'
	DUCKDB_API void Slice(DataChunk &other, const SelectionVector &sel, idx_t count, idx_t col_offset = 0);

	//! Resets the DataChunk to its state right after the DataChunk::Initialize
	//! function was called. This sets the count to 0, and resets each member
	//! Vector to point back to the data owned by this DataChunk.
	DUCKDB_API void Reset();

	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API void Deserialize(Deserializer &source);

	//! Hashes the DataChunk to the target vector
	DUCKDB_API void Hash(Vector &result);
	//! Hashes specific vectors of the DataChunk to the target vector
	DUCKDB_API void Hash(vector<idx_t> &column_ids, Vector &result);

	//! Returns a list of types of the vectors of this data chunk
	DUCKDB_API vector<LogicalType> GetTypes();

	//! Converts this DataChunk to a printable string representation
	DUCKDB_API string ToString() const;
	DUCKDB_API void Print() const;

	DataChunk(const DataChunk &) = delete;

	//! Verify that the DataChunk is in a consistent, not corrupt state. DEBUG
	//! FUNCTION ONLY!
	DUCKDB_API void Verify();

private:
	//! The amount of tuples stored in the data chunk
	idx_t count;
	//! The amount of tuples that can be stored in the data chunk
	idx_t capacity;
	//! Vector caches, used to store data when ::Initialize is called
	vector<VectorCache> vector_caches;
};
} // namespace duckdb
