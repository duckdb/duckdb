//===--------------------------------------------------------------------===//
// numeric_inplace_operators.cpp
// Description: This file contains the implementation of numeric inplace ops
// += *= /= -= %=
//===--------------------------------------------------------------------===//

#include "common/operator/numeric_functions.hpp"
#include "common/types/vector_operations.hpp"
#include "common/vector_operations/unary_loops.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Abs
//===--------------------------------------------------------------------===//
void VectorOperations::Abs(Vector &input, Vector &result) {
	UNARY_TYPE_CHECK(input, result);
	switch (input.type) {
	case TypeId::TINYINT:
		templated_unary_loop<int8_t, int8_t, operators::Abs>(input, result);
		break;
	case TypeId::SMALLINT:
		templated_unary_loop<int16_t, int16_t, operators::Abs>(input, result);
		break;
	case TypeId::INTEGER:
		templated_unary_loop<int32_t, int32_t, operators::Abs>(input, result);
		break;
	case TypeId::BIGINT:
		templated_unary_loop<int64_t, int64_t, operators::Abs>(input, result);
		break;
	case TypeId::DECIMAL:
		templated_unary_loop<double, double, operators::Abs>(input, result);
		break;
	case TypeId::POINTER:
		// nop, pointer is unsigned
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for abs");
	}
}
