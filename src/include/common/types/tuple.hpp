//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/types/tuple.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"

#include <memory>
#include <set>

namespace duckdb {

//! A tuple contains a byte representation of a single tuple stored row-wise
struct Tuple {
	index_t size;
	unique_ptr<data_t[]> data;
};

class TupleSerializer {
public:
	TupleSerializer();
	TupleSerializer(const vector<TypeId> &types, vector<index_t> columns = {});

	//! Initialize the TupleSerializer, should only be called if the empty
	//! constructor is used
	void Initialize(const vector<TypeId> &types, vector<index_t> columns = {});

	//! Serialize a DataChunk to a set of tuples. Memory is allocated for the
	//! tuple data.
	void Serialize(DataChunk &chunk, Tuple targets[]);
	//! Serialize a DataChunk to a set of memory locations
	void Serialize(DataChunk &chunk, data_ptr_t targets[]);
	//! Serializes a tuple from a set of columns to a single memory location
	void Serialize(vector<data_ptr_t> &columns, index_t offset, data_ptr_t target);
	//! Deserialize a DataChunk from a set of memory locations
	void Deserialize(Vector &source, DataChunk &chunk);

	//! Deserialize a tuple from a single memory location to a set of columns
	void Deserialize(vector<data_ptr_t> &columns, index_t offset, data_ptr_t target);
	//! Serializes a set of tuples (specified by the indices vector) to a set of
	//! memory location. Targets[] should have enough spaces to hold
	//! indices.count tuples
	void Serialize(vector<data_ptr_t> &columns, Vector &indices, data_ptr_t targets[]);

	//! Returns the constant per-tuple size (only if the size is constant)
	inline index_t TupleSize() {
		return base_size;
	}

	inline index_t TypeSize() {
		return type_sizes.size();
	}

	//! Compares two tuples. Returns 0 if they are equal, or else returns an
	//! ordering of the tuples. Both should have been constructed by this
	//! TupleSerializer.
	int Compare(Tuple &a, Tuple &b);
	//! Compare two tuple locations in memory. Can only be called if either (1)
	//! inline varlength is FALSE OR (2) no variable length columns are there
	int Compare(const_data_ptr_t a, const_data_ptr_t b);

	//! Serialize a single column of a chunk
	void SerializeColumn(DataChunk &chunk, data_ptr_t targets[], index_t column_index, index_t &offset);
	//! Deserialize a single column of a chunk
	void DeserializeColumn(Vector &source, index_t column_index, Vector &target);

private:
	//! Types of the generated tuples
	vector<TypeId> types;
	//! The type sizes
	vector<index_t> type_sizes;
	//! The column indexes of the chunks
	vector<index_t> columns;
	//! Base size of tuples
	index_t base_size;
	//! Set of variable-length columns included in the set
	vector<bool> is_variable;
	//! Whether or not the Serializer contains variable-length columns
	bool has_variable_columns;
};

} // namespace duckdb
