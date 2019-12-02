//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/unary_numeric.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/unary_loops.hpp"

namespace duckdb {

template <class OP> void unary_numeric_op(Vector &input, Vector &result) {

	UNARY_TYPE_CHECK(input, result);
	switch (input.type) {
	case TypeId::TINYINT:
		templated_unary_loop<int8_t, int8_t, OP>(input, result);
		break;
	case TypeId::SMALLINT:
		templated_unary_loop<int16_t, int16_t, OP>(input, result);
		break;
	case TypeId::INTEGER:
		templated_unary_loop<int32_t, int32_t, OP>(input, result);
		break;
	case TypeId::BIGINT:
		templated_unary_loop<int64_t, int64_t, OP>(input, result);
		break;
	case TypeId::FLOAT:
		templated_unary_loop<float, float, OP>(input, result);
		break;
	case TypeId::DOUBLE:
		templated_unary_loop<double, double, OP>(input, result);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for operator");
	}
}

template <class OP> void unary_numeric_op_dblret(Vector &input, Vector &result) {
	if (result.type != TypeId::DOUBLE) {
		throw Exception("Invalid result type");
	}
	switch (input.type) {
	case TypeId::TINYINT:
		templated_unary_loop<int8_t, double, OP>(input, result);
		break;
	case TypeId::SMALLINT:
		templated_unary_loop<int16_t, double, OP>(input, result);
		break;
	case TypeId::INTEGER:
		templated_unary_loop<int32_t, double, OP>(input, result);
		break;
	case TypeId::BIGINT:
		templated_unary_loop<int64_t, double, OP>(input, result);
		break;
	case TypeId::FLOAT:
		templated_unary_loop<float, double, OP>(input, result);
		break;
	case TypeId::DOUBLE:
		templated_unary_loop<double, double, OP>(input, result);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for operator");
	}
}

template <class OP> void unary_numeric_op_tintret(Vector &input, Vector &result) {
	if (result.type != TypeId::TINYINT) {
		throw Exception("Invalid result type");
	}
	switch (input.type) {
	case TypeId::TINYINT:
		templated_unary_loop<int8_t, int8_t, OP>(input, result);
		break;
	case TypeId::SMALLINT:
		templated_unary_loop<int16_t, int8_t, OP>(input, result);
		break;
	case TypeId::INTEGER:
		templated_unary_loop<int32_t, int8_t, OP>(input, result);
		break;
	case TypeId::BIGINT:
		templated_unary_loop<int64_t, int8_t, OP>(input, result);
		break;
	case TypeId::FLOAT:
		templated_unary_loop<float, int8_t, OP>(input, result);
		break;
	case TypeId::DOUBLE:
		templated_unary_loop<double, int8_t, OP>(input, result);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for operator");
	}
}

} // namespace duckdb
