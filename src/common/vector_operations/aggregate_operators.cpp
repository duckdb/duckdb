//===--------------------------------------------------------------------===//
// aggregate_operators.cpp
// Description: This file contains the implementation of the different
// aggregates
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/aggregate_operators.hpp"

#include "duckdb/common/operator/constant_operators.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/common/types/constant_vector.hpp"
#include "duckdb/common/types/static_vector.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/fold_loops.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

using namespace duckdb;
using namespace std;

template <class OP> static bool numeric_fold_loop(Vector &input, Value &result) {
	switch (input.type) {
	case TypeId::TINYINT:
		return templated_unary_fold<int8_t, int8_t, OP>(input, &result.value_.tinyint);
	case TypeId::SMALLINT:
		return templated_unary_fold<int16_t, int16_t, OP>(input, &result.value_.smallint);
	case TypeId::INTEGER:
		return templated_unary_fold<int32_t, int32_t, OP>(input, &result.value_.integer);
	case TypeId::BIGINT:
		return templated_unary_fold<int64_t, int64_t, OP>(input, &result.value_.bigint);
	case TypeId::FLOAT:
		return templated_unary_fold<float, float, OP>(input, &result.value_.float_);
	case TypeId::DOUBLE:
		return templated_unary_fold<double, double, OP>(input, &result.value_.double_);
	default:
		throw InvalidTypeException(input.type, "Invalid type for aggregate loop");
	}
}

template <class OP> static bool generic_fold_loop(Vector &input, Value &result) {
	switch (input.type) {
	case TypeId::BOOLEAN: {
		bool res;
		if (!templated_unary_fold<bool, bool, OP>(input, &res)) {
			return false;
		}
		result.value_.boolean = res;
		return true;
	}
	case TypeId::VARCHAR: {
		const char *res = nullptr;
		if (!templated_unary_fold<const char *, const char *, OP>(input, &res)) {
			return false;
		}
		result.str_value = res;
		return true;
	}
	default:
		return numeric_fold_loop<OP>(input, result);
	}
}

Value VectorOperations::Count(Vector &left) {
	int64_t count = 0;
	Value result = Value::BIGINT(0);
	if (left.nullmask.any()) {
		// NULL values, count the amount of NULL entries
		VectorOperations::Exec(left, [&](index_t i, index_t k) {
			if (!left.nullmask[i]) {
				count++;
			}
		});
	} else {
		// no NULL values, return all
		count = left.count;
	}
	return Value::BIGINT(count);
}

Value VectorOperations::Sum(Vector &left) {
	if (left.count == 0) {
		return Value(left.type);
	}
	Value result = Value::Numeric(left.type, 0);
	if (!numeric_fold_loop<duckdb::Add>(left, result)) {
		return Value(left.type);
	}
	return result;
}

Value VectorOperations::Max(Vector &left) {
	if (left.count == 0) {
		return Value();
	}
	Value result(left.type);
	if (!generic_fold_loop<duckdb::Max>(left, result)) {
		return Value(left.type);
	}
	result.is_null = false;
	return result;
}

Value VectorOperations::Min(Vector &left) {
	if (left.count == 0) {
		return Value(left.type);
	}
	Value result(left.type);
	if (!generic_fold_loop<duckdb::Min>(left, result)) {
		return Value(left.type);
	}
	result.is_null = false;
	return result;
}

bool VectorOperations::HasNull(Vector &left) {
	return left.nullmask.any();
}
