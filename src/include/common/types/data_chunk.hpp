//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/types/data_chunk.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/internal_types.hpp"
#include "common/printable.hpp"
#include "common/types/vector.hpp"

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
class DataChunk : public Printable {
  public:
	//! The amount of elements in the chunk. Every vector holds this amount of
	//! elements.
	oid_t count;
	//! The amount of vectors that are part of this DataChunk.
	oid_t column_count;
	//! The vectors owned by the DataChunk.
	std::unique_ptr<std::unique_ptr<Vector>[]> data;
	//! The maximum amount of data that can be stored in each of the vectors
	//! owned by the DataChunk.
	oid_t maximum_size;
	//! The (optional) selection vector of the DataChunk. Each of the member
	//! vectors reference this selection vector.
	std::unique_ptr<sel_t[]> sel_vector;

	DataChunk();
	~DataChunk();

	//! Initializes the DataChunk with the specified types to an empty DataChunk
	//! that holds at most maximum_chunk_size elements.
	//! This will create one vector of the specified type for each TypeId in the
	//! types list. The vector will be referencing vector to the data owned by
	//! the DataChunk.
	//! If zero_data is set to true, the data is zero-initialized.
	void Initialize(std::vector<TypeId> &types,
	                oid_t maximum_chunk_size = STANDARD_VECTOR_SIZE,
	                bool zero_data = false);
	//! Append the other DataChunk to this one. The column count and types of
	//! the two DataChunks have to match exactly. Can potentially cause
	//! Vector::Resize on the underlying vectors, which causes them to become
	//! owning vectors.
	void Append(DataChunk &other);
	//! Destroy all data and columns owned by this DataChunk
	void Destroy();

	//! Forces the DataChunk to use only data that it owns itself
	void ForceOwnership();

	//! Resets the DataChunk to its state right after the DataChunk::Initialize
	//! function was called. This sets the count to 0, and resets each member
	//! Vector to point back to the data owned by this DataChunk.
	void Reset();

	//! Returns a list of types of the vectors of this data chunk
	std::vector<TypeId> GetTypes();

	//! Converts this DataChunk to a printable string representation
	std::string ToString() const;

	DataChunk(const DataChunk &) = delete;

  private:
	//! The data owned by this DataChunk. This data is typically referenced by
	//! the member vectors.
	std::unique_ptr<char[]> owned_data;
};
} // namespace duckdb
