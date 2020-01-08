//===--------------------------------------------------------------------===//
// comparison_operators.cpp
// Description: This file contains the implementation of the comparison
// operations == != >= <= > <
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/comparison_operators.hpp"

#include "duckdb/common/vector_operations/binary_loops.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

inline void COMPARISON_TYPE_CHECK(Vector &left, Vector &right, Vector &result) {
	if (left.type != right.type) {
		throw TypeMismatchException(left.type, right.type, "left and right types must be the same");
	}
	if (result.type != TypeId::BOOLEAN) {
		throw InvalidTypeException(result.type, "result of comparison must be boolean");
	}
	if (!left.IsConstant() && !right.IsConstant() && left.count != right.count) {
		throw Exception("Cardinality exception: left and right cannot have "
		                "different cardinalities");
	}
}

template <class OP> static void templated_boolean_operation(Vector &left, Vector &right, Vector &result) {
	COMPARISON_TYPE_CHECK(left, right, result);
	// the inplace loops take the result as the last parameter
	switch (left.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		templated_binary_loop<int8_t, int8_t, bool, OP>(left, right, result);
		break;
	case TypeId::SMALLINT:
		templated_binary_loop<int16_t, int16_t, bool, OP>(left, right, result);
		break;
	case TypeId::INTEGER:
		templated_binary_loop<int32_t, int32_t, bool, OP>(left, right, result);
		break;
	case TypeId::BIGINT:
		templated_binary_loop<int64_t, int64_t, bool, OP>(left, right, result);
		break;
	case TypeId::POINTER:
		templated_binary_loop<uint64_t, uint64_t, bool, OP>(left, right, result);
		break;
	case TypeId::FLOAT:
		templated_binary_loop<float, float, bool, OP>(left, right, result);
		break;
	case TypeId::DOUBLE:
		templated_binary_loop<double, double, bool, OP>(left, right, result);
		break;
	case TypeId::VARCHAR:
		templated_binary_loop<const char *, const char *, bool, OP, true>(left, right, result);
		break;
	default:
		throw InvalidTypeException(left.type, "Invalid type for addition");
	}
}

void VectorOperations::Equals(Vector &left, Vector &right, Vector &result) {
	templated_boolean_operation<duckdb::Equals>(left, right, result);
}

void VectorOperations::NotEquals(Vector &left, Vector &right, Vector &result) {
	templated_boolean_operation<duckdb::NotEquals>(left, right, result);
}

void VectorOperations::GreaterThanEquals(Vector &left, Vector &right, Vector &result) {
	templated_boolean_operation<duckdb::GreaterThanEquals>(left, right, result);
}

void VectorOperations::LessThanEquals(Vector &left, Vector &right, Vector &result) {
	templated_boolean_operation<duckdb::LessThanEquals>(left, right, result);
}

void VectorOperations::GreaterThan(Vector &left, Vector &right, Vector &result) {
	templated_boolean_operation<duckdb::GreaterThan>(left, right, result);
}

void VectorOperations::LessThan(Vector &left, Vector &right, Vector &result) {
	templated_boolean_operation<duckdb::LessThan>(left, right, result);
}
