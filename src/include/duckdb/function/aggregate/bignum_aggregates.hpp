//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/bignum_aggregates.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bignum.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/function/aggregate_state.hpp"

namespace duckdb {

//! The state of an average over BIGNUM values: an exact sum and the count of the summed rows, 0 for an empty state
struct BignumAvgState {
	uint64_t count;
	BignumIntermediate value;
};

//! The BIGNUM accumulators of aggregate states: exported as (trimmed) BIGNUM blobs, imported as copies in the arena
struct BignumAggregates {
	//! Writes the value as a BIGNUM blob through the writer of the result vector
	DUCKDB_API static void WriteBignum(const BignumIntermediate &value, VectorWriter<string_t> &writer);
	//! Copies an imported BIGNUM blob into the arena and returns the intermediate over the copy
	DUCKDB_API static BignumIntermediate ImportBignum(string_t blob, ArenaAllocator &allocator);
	//! Writes an exported average state: the count, and the sum as a BIGNUM blob (NULL for an empty state)
	DUCKDB_API static void WriteAvgState(const BignumAvgState &state, VectorWriter<uint64_t> &count_writer,
	                                     VectorWriter<string_t> &value_writer);
	//! Fills an initialized average state from its exported count and sum; the sum may only be NULL when the count
	//! is 0. value_field names the sum field of the exported state in error messages
	DUCKDB_API static void ImportAvgState(BignumAvgState &state, const VectorIterator<uint64_t>::ValueEntry &count,
	                                      const VectorIterator<string_t>::ValueEntry &value, ArenaAllocator &allocator,
	                                      const char *function_name, const char *value_field);
};

//! Accumulates BIGNUM values into a BignumAvgState; the aggregates derive from it and add their own Finalize
struct BignumAvgOperation {
	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Initialize(STATE &state) {
		state.count = 0;
		state.value = BignumIntermediate();
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		if (state.count == 0) {
			state.value.Initialize(unary_input.input.allocator);
		}
		state.count++;
		const BignumIntermediate rhs(input);
		state.value.AddInPlace(unary_input.input.allocator, rhs);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input) {
		if (source.count == 0) {
			return;
		}
		// the counts of imported states can be anything, the count of rows of a table cannot overflow
		uint64_t count;
		if (!TryAddOperator::Operation(target.count, source.count, count)) {
			throw OutOfRangeException("Overflow in aggregate state combine");
		}
		if (target.count == 0) {
			target.value.Initialize(input.allocator);
		}
		target.count = count;
		target.value.AddInPlace(input.allocator, source.value);
	}
};

} // namespace duckdb
