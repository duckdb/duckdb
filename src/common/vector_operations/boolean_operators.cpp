//===--------------------------------------------------------------------===//
// boolean_operators.cpp
// Description: This file contains the implementation of the boolean
// operations AND OR !
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/boolean_operators.hpp"

#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// AND/OR
//===--------------------------------------------------------------------===//
template <class OP, class NULLOP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
static void templated_boolean_function_loop(bool *__restrict ldata, bool *__restrict rdata,
                                            bool *__restrict result_data, index_t count, sel_t *__restrict sel_vector,
                                            nullmask_t &left_nullmask, nullmask_t &right_nullmask,
                                            nullmask_t &result_nullmask) {
	if (left_nullmask.any() || right_nullmask.any()) {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			index_t left_idx = LEFT_CONSTANT ? 0 : i;
			index_t right_idx = RIGHT_CONSTANT ? 0 : i;
			result_data[i] = OP::Operation(ldata[left_idx], rdata[right_idx]);
			result_nullmask[i] = NULLOP::Operation(ldata[left_idx], rdata[right_idx], left_nullmask[left_idx],
			                                       right_nullmask[right_idx]);
		});
	} else {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			result_data[i] = OP::Operation(ldata[LEFT_CONSTANT ? 0 : i], rdata[RIGHT_CONSTANT ? 0 : i]);
		});
	}
}

template <class OP, class NULLOP> static void templated_boolean_nullmask(Vector &left, Vector &right, Vector &result) {
	assert(left.type == TypeId::BOOL && right.type == TypeId::BOOL && result.type == TypeId::BOOL);
	assert(left.SameCardinality(right));
	result.SetCount(left.size());
	result.SetSelVector(left.sel_vector());

	auto ldata = (bool *)left.GetData();
	auto rdata = (bool *)right.GetData();
	auto result_data = (bool *)result.GetData();

	if (left.vector_type == VectorType::CONSTANT_VECTOR && right.vector_type == VectorType::CONSTANT_VECTOR) {
		// operation on two constants, result is constant vector
		result.vector_type = VectorType::CONSTANT_VECTOR;
		templated_boolean_function_loop<OP, NULLOP, true, false>(ldata, rdata, result_data, 1, nullptr, left.nullmask,
		                                                         right.nullmask, result.nullmask);
	} else if (left.vector_type == VectorType::CONSTANT_VECTOR) {
		// left side is constant, result is regular vector
		result.vector_type = VectorType::FLAT_VECTOR;
		templated_boolean_function_loop<OP, NULLOP, true, false>(
		    ldata, rdata, result_data, result.size(), result.sel_vector(), left.nullmask, right.nullmask, result.nullmask);
	} else if (right.vector_type == VectorType::CONSTANT_VECTOR) {
		// right side is constant, result is regular vector
		result.vector_type = VectorType::FLAT_VECTOR;
		templated_boolean_function_loop<OP, NULLOP, false, true>(
		    ldata, rdata, result_data, result.size(), result.sel_vector(), left.nullmask, right.nullmask, result.nullmask);
	} else {
		// no constant vectors: perform general loop
		result.vector_type = VectorType::FLAT_VECTOR;
		templated_boolean_function_loop<OP, NULLOP, false, false>(
		    ldata, rdata, result_data, result.size(), result.sel_vector(), left.nullmask, right.nullmask, result.nullmask);
	}
}

void VectorOperations::And(Vector &left, Vector &right, Vector &result) {
	templated_boolean_nullmask<duckdb::And, duckdb::AndMask>(left, right, result);
}

void VectorOperations::Or(Vector &left, Vector &right, Vector &result) {
	templated_boolean_nullmask<duckdb::Or, duckdb::OrMask>(left, right, result);
}

struct NotOperator {
	template <class TA, class TR> static inline TR Operation(TA left) {
		return !left;
	}
};

void VectorOperations::Not(Vector &input, Vector &result) {
	assert(input.type == TypeId::BOOL && result.type == TypeId::BOOL);
	UnaryExecutor::Execute<bool, bool, NotOperator>(input, result);
}
