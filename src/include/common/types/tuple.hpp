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
	uint64_t size;
	unique_ptr<uint8_t[]> data;
};

class TupleSerializer {
	friend class TupleComparer;

public:
	TupleSerializer();
	TupleSerializer(const vector<TypeId> &types, vector<uint64_t> columns = {});

	//! Initialize the TupleSerializer, should only be called if the empty
	//! constructor is used
	void Initialize(const vector<TypeId> &types, vector<uint64_t> columns = {});

	//! Serialize a DataChunk to a set of tuples. Memory is allocated for the
	//! tuple data.
	void Serialize(DataChunk &chunk, Tuple targets[]);
	//! Serialize a DataChunk to a set of memory locations
	void Serialize(DataChunk &chunk, uint8_t *targets[]);
	//! Serializes a tuple from a set of columns to a single memory location
	void Serialize(vector<char *> &columns, uint64_t offset, uint8_t *target);
	//! Deserialize a DataChunk from a set of memory locations
	void Deserialize(Vector &source, DataChunk &chunk);

	//! Deserialize a tuple from a single memory location to a set of columns
	void Deserialize(vector<char *> &columns, uint64_t offset, uint8_t *target);
	//! Serializes a set of tuples (specified by the indices vector) to a set of
	//! memory location. Targets[] should have enough spaces to hold
	//! indices.count tuples
	void Serialize(vector<char *> &columns, Vector &indices, uint8_t *targets[]);
	//! Serializes a set of tuples with updates. The base tuples are specified
	//! by the index vector, the updated values are specified by update_chunk.
	//! affected_columns signifies
	void SerializeUpdate(vector<char *> &column_data, vector<column_t> &affected_columns, DataChunk &update_chunk,
	                     Vector &index_vector, uint64_t index_offset, Tuple targets[]);

	//! Returns the constant per-tuple size (only if the size is constant)
	inline uint64_t TupleSize() {
		return base_size;
	}

	inline uint64_t TypeSize() {
		return type_sizes.size();
	}

	//! Compares two tuples. Returns 0 if they are equal, or else returns an
	//! ordering of the tuples. Both should have been constructed by this
	//! TupleSerializer.
	int Compare(Tuple &a, Tuple &b);
	//! Compare two tuple locations in memory. Can only be called if either (1)
	//! inline varlength is FALSE OR (2) no variable length columns are there
	int Compare(const uint8_t *a, const uint8_t *b);

	//! Serialize a single column of a chunk
	void SerializeColumn(DataChunk &chunk, uint8_t *targets[], uint64_t column_index, uint64_t &offset);
	//! Deserialize a single column of a chunk
	void DeserializeColumn(Vector &source, uint64_t column_index, Vector &target);

private:
	//! Types of the generated tuples
	vector<TypeId> types;
	//! The type sizes
	vector<uint64_t> type_sizes;
	//! The column indexes of the chunks
	vector<uint64_t> columns;
	//! Base size of tuples
	uint64_t base_size;
	//! Set of variable-length columns included in the set
	vector<bool> is_variable;
	//! Whether or not the Serializer contains variable-length columns
	bool has_variable_columns;
};

//! Compare tuples created through different TupleSerializers
class TupleComparer {
public:
	//! Create a tuple comparer that compares tuples created with the left
	//! serializer to tuples created with the right serializer. The columns of
	//! the left serializer must be a subset of the columns of the right
	//! serializer.
	TupleComparer(TupleSerializer &left, TupleSerializer &right);
	//! Use the TupleComparer to compare two tuples from the {left,right}
	//! serializer with each other
	int Compare(const uint8_t *left, const uint8_t *right);

private:
	//! Left tuple serializer
	TupleSerializer &left;
	//! The left offsets used for comparison
	vector<uint64_t> left_offsets;
	//! The right offsets used for comparison
	vector<uint64_t> right_offsets;
};

struct TupleReference {
	Tuple *tuple;
	TupleSerializer &serializer;

	TupleReference(Tuple *tuple, TupleSerializer &serializer) : tuple(tuple), serializer(serializer) {
		// NULL tuple not allowed
		assert(tuple);
	}

	bool operator<(const TupleReference &rhs) const {
		// comparison needs the same serializer
		assert(&serializer == &rhs.serializer);
		return serializer.Compare(*tuple, *rhs.tuple) < 0;
	}
};

typedef std::set<TupleReference> TupleSet;

} // namespace duckdb
