//===--------------------------------------------------------------------===//
// boolean_operators.cpp
// Description: This file contains the implementation of the boolean
// operations AND OR !
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/boolean_operators.hpp"

#include "duckdb/common/vector_operations/binary_loops.hpp"
#include "duckdb/common/vector_operations/unary_loops.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// AND/OR
//===--------------------------------------------------------------------===//
template <class OP, class NULLOP> void templated_boolean_nullmask(Vector &left, Vector &right, Vector &result) {
	if (left.type != TypeId::BOOLEAN || right.type != TypeId::BOOLEAN) {
		throw TypeMismatchException(left.type, right.type, "Conjunction can only be applied on boolean values");
	}

	auto ldata = (bool *)left.data;
	auto rdata = (bool *)right.data;
	auto result_data = (bool *)result.data;

	if (left.IsConstant()) {
		bool left_null = left.nullmask[0];
		bool constant = ldata[0];
		VectorOperations::Exec(right, [&](index_t i, index_t k) {
			result_data[i] = OP::Operation(constant, rdata[i]);
			result.nullmask[i] = NULLOP::Operation(constant, rdata[i], left_null, right.nullmask[i]);
		});
		result.sel_vector = right.sel_vector;
		result.count = right.count;
	} else if (right.IsConstant()) {
		// AND / OR operations are commutative
		templated_boolean_nullmask<OP, NULLOP>(right, left, result);
	} else if (left.count == right.count) {
		assert(left.sel_vector == right.sel_vector);
		VectorOperations::Exec(left, [&](index_t i, index_t k) {
			result_data[i] = OP::Operation(ldata[i], rdata[i]);
			result.nullmask[i] = NULLOP::Operation(ldata[i], rdata[i], left.nullmask[i], right.nullmask[i]);
		});
		result.sel_vector = left.sel_vector;
		result.count = left.count;
	} else {
		throw Exception("Vector lengths don't match");
	}
}

void VectorOperations::And(Vector &left, Vector &right, Vector &result) {
	templated_boolean_nullmask<duckdb::And, duckdb::AndMask>(left, right, result);
}

void VectorOperations::Or(Vector &left, Vector &right, Vector &result) {
	templated_boolean_nullmask<duckdb::Or, duckdb::OrMask>(left, right, result);
}

void VectorOperations::Not(Vector &left, Vector &result) {
	if (left.type != TypeId::BOOLEAN) {
		throw InvalidTypeException(left.type, "NOT() needs a boolean input");
	}
	templated_unary_loop<int8_t, int8_t, duckdb::Not>(left, result);
}
