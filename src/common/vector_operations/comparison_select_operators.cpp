//===--------------------------------------------------------------------===//
// comparison_select_operators.cpp
// Description: This file contains the implementation of the comparison select
// operations == != >= <= > <
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/comparison_operators.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/binary_select_loops.hpp"

using namespace duckdb;
using namespace std;

template <class OP> static index_t templated_select_operation(Vector &left, Vector &right, sel_t result[]) {
	// the inplace loops take the result as the last parameter
	switch (left.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		return templated_binary_select<int8_t, int8_t, OP>(left, right, result);
	case TypeId::SMALLINT:
		return templated_binary_select<int16_t, int16_t, OP>(left, right, result);
	case TypeId::INTEGER:
		return templated_binary_select<int32_t, int32_t, OP>(left, right, result);
	case TypeId::BIGINT:
		return templated_binary_select<int64_t, int64_t, OP>(left, right, result);
	case TypeId::POINTER:
		return templated_binary_select<uint64_t, uint64_t, OP>(left, right, result);
	case TypeId::FLOAT:
		return templated_binary_select<float, float, OP>(left, right, result);
	case TypeId::DOUBLE:
		return templated_binary_select<double, double, OP>(left, right, result);
	case TypeId::VARCHAR:
		return templated_binary_select<const char *, const char *, OP>(left, right, result);
	default:
		throw InvalidTypeException(left.type, "Invalid type for comparison");
	}
}

index_t VectorOperations::SelectEquals(Vector &left, Vector &right, sel_t result[]) {
	return templated_select_operation<duckdb::Equals>(left, right, result);
}

index_t VectorOperations::SelectNotEquals(Vector &left, Vector &right, sel_t result[]) {
	return templated_select_operation<duckdb::NotEquals>(left, right, result);
}

index_t VectorOperations::SelectGreaterThanEquals(Vector &left, Vector &right, sel_t result[]) {
	return templated_select_operation<duckdb::GreaterThanEquals>(left, right, result);
}

index_t VectorOperations::SelectLessThanEquals(Vector &left, Vector &right, sel_t result[]) {
	return templated_select_operation<duckdb::LessThanEquals>(left, right, result);
}

index_t VectorOperations::SelectGreaterThan(Vector &left, Vector &right, sel_t result[]) {
	return templated_select_operation<duckdb::GreaterThan>(left, right, result);
}

index_t VectorOperations::SelectLessThan(Vector &left, Vector &right, sel_t result[]) {
	return templated_select_operation<duckdb::LessThan>(left, right, result);
}
