//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/types/tuple.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/types/data_chunk.hpp"

namespace duckdb {

//! A tuple contains a byte reprensetation of a single tuple stored row-wise
struct Tuple {
	size_t size;
	std::unique_ptr<uint8_t[]> data;
};

template <bool inline_varlength> class TupleSerializer {
  public:
	TupleSerializer(const std::vector<TypeId> &types,
	                std::vector<size_t> columns);

	//! Serialize a DataChunk to a set of tuples. Memory is allocated for the
	//! tuple data.
	void Serialize(DataChunk &chunk, Tuple targets[]);
	//! Serialize a DataChunk to a set of memory locations
	void Serialize(DataChunk &chunk, const char *targets[]);

	//! Compares two tuples. Returns 0 if they are equal, or else returns an
	//! ordering of the tuples. Both should have been constructed by this
	//! TupleSerializer.
	inline int Compare(Tuple &a, Tuple &b);
	//! Compare two tuple locations in memory. Can only be called if either (1)
	//! inline varlength is FALSE OR (2) no variable length columns are there
	inline int Compare(const char *a, const char *b);

  private:
	//! Serialize a single column of a chunk with potential variable columns to
	//! the target tuples
	void SerializeColumn(DataChunk &chunk, const char *targets[], size_t column,
	                     size_t offsets[]);
	//! Single a single column of a chunk
	void SerializeColumn(DataChunk &chunk, const char *targets[], size_t column,
	                     size_t &offset);

	std::vector<size_t> type_sizes;
	//! The columns to use into the chunks
	std::vector<size_t> columns;
	//! Base size of tuples
	size_t base_size;
	//! Set of variable-length columns included in the set
	std::vector<bool> is_variable;
	//! Whether or not the Serializer contains variable-length columns
	bool has_variable_columns;
};

} // namespace duckdb
