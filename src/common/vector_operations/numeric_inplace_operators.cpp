//===--------------------------------------------------------------------===//
// numeric_inplace_operators.cpp
// Description: This file contains the implementation of numeric inplace ops
// += *= /= -= %=
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/numeric_inplace_operators.hpp"

#include "duckdb/common/vector_operations/inplace_loops.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// In-Place Addition
//===--------------------------------------------------------------------===//
// left += right
void VectorOperations::AddInPlace(Vector &result, Vector &input) {
	// the inplace loops take the result as the last parameter
	switch (input.type) {
	case TypeId::INT8:
		templated_inplace_loop<int8_t, int8_t, duckdb::AddInPlace>(input, result);
		break;
	case TypeId::INT16:
		templated_inplace_loop<int16_t, int16_t, duckdb::AddInPlace>(input, result);
		break;
	case TypeId::INT32:
		templated_inplace_loop<int32_t, int32_t, duckdb::AddInPlace>(input, result);
		break;
	case TypeId::INT64:
		templated_inplace_loop<int64_t, int64_t, duckdb::AddInPlace>(input, result);
		break;
	case TypeId::HASH:
		templated_inplace_loop<uint64_t, uint64_t, duckdb::AddInPlace>(input, result);
		break;
	case TypeId::POINTER:
		templated_inplace_loop<uintptr_t, uintptr_t, duckdb::AddInPlace>(input, result);
		break;
	case TypeId::FLOAT:
		templated_inplace_loop<float, float, duckdb::AddInPlace>(input, result);
		break;
	case TypeId::DOUBLE:
		templated_inplace_loop<double, double, duckdb::AddInPlace>(input, result);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for addition");
	}
}

void VectorOperations::AddInPlace(Vector &left, int64_t right) {
	Value right_value = Value::Numeric(left.type, right);
	Vector right_vector(left.cardinality(), right_value);
	VectorOperations::AddInPlace(left, right_vector);
}
