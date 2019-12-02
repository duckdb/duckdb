//===--------------------------------------------------------------------===//
// numeric_inplace_operators.cpp
// Description: This file contains the implementation of numeric inplace ops
// += *= /= -= %=
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/numeric_functions.hpp"

#include "duckdb/common/vector_operations/binary_loops.hpp"
#include "duckdb/common/vector_operations/unary_loops.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "duckdb/common/vector_operations/unary_numeric.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Abs
//===--------------------------------------------------------------------===//
void VectorOperations::Abs(Vector &input, Vector &result) {
	unary_numeric_op<duckdb::Abs>(input, result);
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

void VectorOperations::Ceil(Vector &input, Vector &result) {
	unary_numeric_op<duckdb::Ceil>(input, result);
}

void VectorOperations::Floor(Vector &input, Vector &result) {
	unary_numeric_op<duckdb::Floor>(input, result);
}

void VectorOperations::Sqrt(Vector &input, Vector &result) {
	unary_numeric_op<duckdb::Sqrt>(input, result);
}

void VectorOperations::Ln(Vector &input, Vector &result) {
	unary_numeric_op<duckdb::Ln>(input, result);
}

void VectorOperations::Log10(Vector &input, Vector &result) {
	unary_numeric_op<duckdb::Log10>(input, result);
}

void VectorOperations::Log2(Vector &input, Vector &result) {
	unary_numeric_op<duckdb::Log2>(input, result);
}

void VectorOperations::CbRt(Vector &input, Vector &result) {
	unary_numeric_op_dblret<duckdb::CbRt>(input, result);
}

void VectorOperations::Radians(Vector &input, Vector &result) {
	unary_numeric_op_dblret<duckdb::Radians>(input, result);
}

void VectorOperations::Degrees(Vector &input, Vector &result) {
	unary_numeric_op_dblret<duckdb::Degrees>(input, result);
}

void VectorOperations::Exp(Vector &input, Vector &result) {
	unary_numeric_op<duckdb::Exp>(input, result);
}

// the low-level functions cast to double anyway so makes no sense to have different implementations
void VectorOperations::Sin(Vector &left, Vector &result) {
	if (left.type != TypeId::DOUBLE) {
		throw InvalidTypeException(left.type, "Invalid type for SIN");
	}
	templated_unary_loop<double, double, duckdb::Sin, true>(left, result);
}

void VectorOperations::Cos(Vector &left, Vector &result) {
	if (left.type != TypeId::DOUBLE) {
		throw InvalidTypeException(left.type, "Invalid type for COS");
	}
	templated_unary_loop<double, double, duckdb::Cos, true>(left, result);
}

void VectorOperations::Tan(Vector &left, Vector &result) {
	if (left.type != TypeId::DOUBLE) {
		throw InvalidTypeException(left.type, "Invalid type for TAN");
	}
	templated_unary_loop<double, double, duckdb::Tan, true>(left, result);
}

void VectorOperations::ASin(Vector &left, Vector &result) {
	if (left.type != TypeId::DOUBLE) {
		throw InvalidTypeException(left.type, "Invalid type for ASIN");
	}
	templated_unary_loop<double, double, duckdb::ASin, true>(left, result);
}

void VectorOperations::ACos(Vector &left, Vector &result) {
	if (left.type != TypeId::DOUBLE) {
		throw InvalidTypeException(left.type, "Invalid type for ACOS");
	}
	templated_unary_loop<double, double, duckdb::ACos, true>(left, result);
}

void VectorOperations::ATan(Vector &left, Vector &result) {
	if (left.type != TypeId::DOUBLE) {
		throw InvalidTypeException(left.type, "Invalid type for ACOS");
	}
	templated_unary_loop<double, double, duckdb::ATan, true>(left, result);
}

void VectorOperations::ATan2(Vector &left, Vector &right, Vector &result) {
	if (left.type != TypeId::DOUBLE || right.type != TypeId::DOUBLE) {
		throw InvalidTypeException(left.type, "Invalid type for ATAN2");
	}
	templated_binary_loop<double, double, double, duckdb::ATan2, true>(left, right, result);
}

void VectorOperations::Sign(Vector &input, Vector &result) {
	unary_numeric_op_tintret<duckdb::Sign>(input, result);
}

void VectorOperations::Pow(Vector &base, Vector &exponent, Vector &result) {
	templated_binary_loop<double, double, double, duckdb::Pow, true>(base, exponent, result);
}
