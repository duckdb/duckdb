//===--------------------------------------------------------------------===//
// aggregate_operators.cpp
// Description: This file contains the implementation of the different
// aggregates
//===--------------------------------------------------------------------===//

#include "common/operator/aggregate_operators.hpp"
#include "common/operator/constant_operators.hpp"
#include "common/operator/numeric_binary_operators.hpp"
#include "common/value_operations/value_operations.hpp"
#include "common/vector_operations/fold_loops.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class OP>
static void generic_fold_loop(Vector &input, Value &result) {
	switch (input.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		templated_unary_fold<int8_t, int8_t, OP>(input, &result.value_.tinyint);
		break;
	case TypeId::SMALLINT:
		templated_unary_fold<int16_t, int16_t, OP>(input,
		                                           &result.value_.smallint);
		break;
	case TypeId::INTEGER:
		templated_unary_fold<int32_t, int32_t, OP>(input,
		                                           &result.value_.integer);
		break;
	case TypeId::BIGINT:
		templated_unary_fold<int64_t, int64_t, OP>(input,
		                                           &result.value_.bigint);
		break;
	case TypeId::DECIMAL:
		templated_unary_fold<double, double, OP>(input, &result.value_.decimal);
		break;
	case TypeId::POINTER:
		templated_unary_fold<uint64_t, uint64_t, OP>(input,
		                                             &result.value_.pointer);
		break;
	case TypeId::DATE:
		templated_unary_fold<date_t, date_t, OP>(input, &result.value_.date);
		break;
	case TypeId::TIMESTAMP:
		templated_unary_fold<timestamp_t, timestamp_t, OP>(
		    input, &result.value_.timestamp);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for addition");
	}
}

Value VectorOperations::Sum(Vector &left) {
	if (left.count == 0 || !TypeIsNumeric(left.type)) {
		return Value();
	}

	Value result = Value::Numeric(left.type, 0);

	// check if all are NULL, because then the result is NULL and not 0
	Vector is_null;
	is_null.Initialize(TypeId::BOOLEAN);
	VectorOperations::IsNull(left, is_null);

	if (ValueOperations::Equals(VectorOperations::AllTrue(is_null),
	                            Value(true))) {
		result.is_null = true;
	} else {
		generic_fold_loop<operators::Add>(left, result);
	}
	return result;
}

Value VectorOperations::Count(Vector &left) {
	Value result = Value::Numeric(left.type, 0);
	generic_fold_loop<operators::AddOne>(left, result);
	return result;
}

Value VectorOperations::Max(Vector &left) {
	if (left.count == 0 || !TypeIsNumeric(left.type)) {
		return Value();
	}
	Value minimum_value = Value::MinimumValue(left.type);
	Value result = minimum_value;
	generic_fold_loop<operators::Max>(left, result);
	result.is_null = ValueOperations::Equals(
	    result, minimum_value); // check if any tuples qualified
	return result;
}

Value VectorOperations::Min(Vector &left) {
	if (left.count == 0 || !TypeIsNumeric(left.type)) {
		return Value();
	}
	Value maximum_value = Value::MaximumValue(left.type);
	Value result = maximum_value;
	generic_fold_loop<operators::Min>(left, result);
	result.is_null = ValueOperations::Equals(
	    result, maximum_value); // check if any tuples qualified
	return result;
}

Value VectorOperations::AnyTrue(Vector &left) {
	if (left.type != TypeId::BOOLEAN) {
		throw InvalidTypeException(
		    left.type, "AnyTrue can only be computed for boolean columns!");
	}
	if (left.count == 0) {
		return Value(false);
	}

	Value result = Value(false);
	generic_fold_loop<operators::AnyTrue>(left, result);
	return result;
}

Value VectorOperations::AllTrue(Vector &left) {
	if (left.type != TypeId::BOOLEAN) {
		throw InvalidTypeException(
		    left.type, "AllTrue can only be computed for boolean columns!");
	}
	if (left.count == 0) {
		return Value(false);
	}

	Value result = Value(true);
	generic_fold_loop<operators::AllTrue>(left, result);
	return result;
}

bool VectorOperations::Contains(Vector &vector, Value &value) {
	if (vector.count == 0) {
		return false;
	}
	// first perform a comparison using Equals
	// then return TRUE if any of the comparisons are true
	// FIXME: this can be done more efficiently in one loop
	Vector right(value.CastAs(vector.type));
	Vector comparison_result(TypeId::BOOLEAN, true, false);
	VectorOperations::Equals(vector, right, comparison_result);
	auto result = VectorOperations::AnyTrue(comparison_result);
	assert(result.type == TypeId::BOOLEAN);
	return result.value_.boolean;
}

bool VectorOperations::HasNull(Vector &left) {
	return left.nullmask.any();
}

Value VectorOperations::MaximumStringLength(Vector &left) {
	if (left.type != TypeId::VARCHAR) {
		throw InvalidTypeException(
		    left.type,
		    "String length can only be computed for char array columns!");
	}
	auto result = Value::POINTER(0);
	if (left.count == 0) {
		return result;
	}
	templated_unary_fold<const char *, uint64_t,
	                     operators::MaximumStringLength>(
	    left, &result.value_.pointer);
	return result;
}
