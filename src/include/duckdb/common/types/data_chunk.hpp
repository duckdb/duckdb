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
#include "duckdb/common/winapi.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/arrow_wrapper.hpp"

struct ArrowArray;

namespace duckdb {
class Allocator;
class ClientContext;
class ExecutionContext;
class VectorCache;

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
		this->count = other.size();
	}
	inline void SetCapacity(const DataChunk &other) {
		this->capacity = other.capacity;
	}

	DUCKDB_API Value GetValue(idx_t col_idx, idx_t index) const;
	DUCKDB_API void SetValue(idx_t col_idx, idx_t index, const Value &val);

	//! Set the DataChunk to reference another data chunk
	DUCKDB_API void Reference(DataChunk &chunk);
	//! Set the DataChunk to own the data of data chunk, destroying the other chunk in the process
	DUCKDB_API void Move(DataChunk &chunk);

	//! Initializes the DataChunk with the specified types to an empty DataChunk
	//! This will create one vector of the specified type for each LogicalType in the
	//! types list. The vector will be referencing vector to the data owned by
	//! the DataChunk.
	DUCKDB_API void Initialize(Allocator &allocator, const vector<LogicalType> &types);
	DUCKDB_API void Initialize(ClientContext &context, const vector<LogicalType> &types);
	//! Initializes an empty DataChunk with the given types. The vectors will *not* have any data allocated for them.
	DUCKDB_API void InitializeEmpty(const vector<LogicalType> &types);
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

	//! Turn all the vectors from the chunk into flat vectors
	DUCKDB_API void Flatten();

	DUCKDB_API unique_ptr<UnifiedVectorFormat[]> ToUnifiedFormat();

	DUCKDB_API void Slice(const SelectionVector &sel_vector, idx_t count);
	DUCKDB_API void Slice(DataChunk &other, const SelectionVector &sel, idx_t count, idx_t col_offset = 0);

	//! Resets the DataChunk to its state right after the DataChunk::Initialize
	//! function was called. This sets the count to 0, and resets each member
	//! Vector to point back to the data owned by this DataChunk.
	DUCKDB_API void Reset();

	//! Serializes a DataChunk to a stand-alone binary blob
	DUCKDB_API void Serialize(Serializer &serializer);
	//! Deserializes a blob back into a DataChunk
	DUCKDB_API void Deserialize(Deserializer &source);

	//! Hashes the DataChunk to the target vector
	DUCKDB_API void Hash(Vector &result);

	//! Returns a list of types of the vectors of this data chunk
	DUCKDB_API vector<LogicalType> GetTypes();

	//! Converts this DataChunk to a printable string representation
	DUCKDB_API string ToString() const;
	DUCKDB_API void Print();

	DataChunk(const DataChunk &) = delete;

	//! Verify that the DataChunk is in a consistent, not corrupt state. DEBUG
	//! FUNCTION ONLY!
	DUCKDB_API void Verify();

	//! export data chunk as a arrow struct array that can be imported as arrow record batch
	DUCKDB_API void ToArrowArray(ArrowArray *out_array);

private:
	//! The amount of tuples stored in the data chunk
	idx_t count;
	//! The amount of tuples that can be stored in the data chunk
	idx_t capacity;
	//! Vector caches, used to store data when ::Initialize is called
	vector<VectorCache> vector_caches;
};
} // namespace duckdb
