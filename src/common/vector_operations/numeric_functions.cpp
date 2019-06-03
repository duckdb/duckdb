//===--------------------------------------------------------------------===//
// numeric_inplace_operators.cpp
// Description: This file contains the implementation of numeric inplace ops
// += *= /= -= %=
//===--------------------------------------------------------------------===//

#include "common/operator/numeric_functions.hpp"

#include "common/vector_operations/binary_loops.hpp"
#include "common/vector_operations/unary_loops.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Abs
//===--------------------------------------------------------------------===//
void VectorOperations::Abs(Vector &input, Vector &result) {
	UNARY_TYPE_CHECK(input, result);
	switch (input.type) {
	case TypeId::TINYINT:
		templated_unary_loop<int8_t, int8_t, duckdb::Abs>(input, result);
		break;
	case TypeId::SMALLINT:
		templated_unary_loop<int16_t, int16_t, duckdb::Abs>(input, result);
		break;
	case TypeId::INTEGER:
		templated_unary_loop<int32_t, int32_t, duckdb::Abs>(input, result);
		break;
	case TypeId::BIGINT:
		templated_unary_loop<int64_t, int64_t, duckdb::Abs>(input, result);
		break;
	case TypeId::FLOAT:
		templated_unary_loop<float, float, duckdb::Abs>(input, result);
		break;
	case TypeId::DOUBLE:
		templated_unary_loop<double, double, duckdb::Abs>(input, result);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for abs");
	}
}

//===--------------------------------------------------------------------===//
// Round
//===--------------------------------------------------------------------===//
void VectorOperations::Round(Vector &input, Vector &precision, Vector &result) {
	UNARY_TYPE_CHECK(input, result);
	if (!TypeIsInteger(precision.type)) {
		throw InvalidTypeException(precision.type, "Invalid type for rounding precision");
	}

	switch (input.type) {
	case TypeId::TINYINT:
	case TypeId::SMALLINT:
	case TypeId::INTEGER:
	case TypeId::BIGINT:
		VectorOperations::Copy(input, result);
		break;
	case TypeId::FLOAT:
		precision.Cast(TypeId::TINYINT);
		templated_binary_loop<float, int8_t, float, duckdb::Round>(input, precision, result);
		break;
	case TypeId::DOUBLE:
		precision.Cast(TypeId::TINYINT);
		templated_binary_loop<double, int8_t, double, duckdb::Round>(input, precision, result);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for round");
	}
}

// the low-level functions cast to double anyway so makes no sense to have different implementations
void VectorOperations::Sin(Vector &left, Vector &result) {
	if (left.type != TypeId::DOUBLE) {
		throw InvalidTypeException(left.type, "Invalid type for SIN");
	}
	templated_unary_loop<double, double, duckdb::Sin>(left, result);
}


void VectorOperations::Cos(Vector &left, Vector &result) {
	if (left.type != TypeId::DOUBLE) {
			throw InvalidTypeException(left.type, "Invalid type for COS");
		}
		templated_unary_loop<double, double, duckdb::Cos>(left, result);
}


void VectorOperations::Tan(Vector &left, Vector &result) {
	if (left.type != TypeId::DOUBLE) {
			throw InvalidTypeException(left.type, "Invalid type for TAN");
		}
		templated_unary_loop<double, double, duckdb::Tan>(left, result);
}


void VectorOperations::ASin(Vector &left, Vector &result) {
	if (left.type != TypeId::DOUBLE) {
			throw InvalidTypeException(left.type, "Invalid type for ASIN");
		}
		templated_unary_loop<double, double, duckdb::ASin>(left, result);
}


void VectorOperations::ACos(Vector &left, Vector &result) {
	if (left.type != TypeId::DOUBLE) {
			throw InvalidTypeException(left.type, "Invalid type for ACOS");
		}
		templated_unary_loop<double, double, duckdb::ACos>(left, result);
}


void VectorOperations::ATan(Vector &left, Vector &result) {
	if (left.type != TypeId::DOUBLE) {
			throw InvalidTypeException(left.type, "Invalid type for ACOS");
		}
		templated_unary_loop<double, double, duckdb::ATan>(left, result);
}

void VectorOperations::ATan2(Vector &left, Vector &right, Vector &result) {
	if (left.type != TypeId::DOUBLE || right.type != TypeId::DOUBLE) {
		throw InvalidTypeException(left.type, "Invalid type for ATAN2");
	}
	templated_binary_loop<double, double, double, duckdb::ATan2>(left, right, result);
}
