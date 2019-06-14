//===--------------------------------------------------------------------===//
// aggregate_operators.cpp
// Description: This file contains the implementation of the different
// aggregates
//===--------------------------------------------------------------------===//

#include "common/operator/aggregate_operators.hpp"

#include "common/operator/constant_operators.hpp"
#include "common/operator/numeric_binary_operators.hpp"
#include "common/types/constant_vector.hpp"
#include "common/types/static_vector.hpp"
#include "common/value_operations/value_operations.hpp"
#include "common/vector_operations/fold_loops.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class OP> static void generic_fold_loop(Vector &input, Value &result) {
	switch (input.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		templated_unary_fold<int8_t, int8_t, OP>(input, &result.value_.tinyint);
		break;
	case TypeId::SMALLINT:
		templated_unary_fold<int16_t, int16_t, OP>(input, &result.value_.smallint);
		break;
	case TypeId::INTEGER:
		templated_unary_fold<int32_t, int32_t, OP>(input, &result.value_.integer);
		break;
	case TypeId::BIGINT:
		templated_unary_fold<int64_t, int64_t, OP>(input, &result.value_.bigint);
		break;
	case TypeId::FLOAT:
		templated_unary_fold<float, float, OP>(input, &result.value_.float_);
		break;
	case TypeId::DOUBLE:
		templated_unary_fold<double, double, OP>(input, &result.value_.double_);
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
	StaticVector<bool> is_null;
	VectorOperations::IsNull(left, is_null);

	if (VectorOperations::AllTrue(is_null)) {
		result.is_null = true;
	} else {
		generic_fold_loop<duckdb::Add>(left, result);
	}
	return result;
}

Value VectorOperations::Count(Vector &left) {
	Value result = Value::Numeric(left.type, 0);
	generic_fold_loop<duckdb::AddOne>(left, result);
	return result;
}

Value VectorOperations::Max(Vector &left) {
	if (left.count == 0 || !TypeIsNumeric(left.type)) {
		return Value();
	}
	Value minimum_value = Value::MinimumValue(left.type);
	Value result = minimum_value;
	generic_fold_loop<duckdb::Max>(left, result);
	result.is_null = result == minimum_value; // check if any tuples qualified
	return result;
}

Value VectorOperations::Min(Vector &left) {
	if (left.count == 0 || !TypeIsNumeric(left.type)) {
		return Value();
	}
	Value maximum_value = Value::MaximumValue(left.type);
	Value result = maximum_value;
	generic_fold_loop<duckdb::Min>(left, result);
	result.is_null = result == maximum_value; // check if any tuples qualified
	return result;
}

bool VectorOperations::AnyTrue(Vector &left) {
	if (left.type != TypeId::BOOLEAN) {
		throw InvalidTypeException(left.type, "AnyTrue can only be computed for boolean columns!");
	}
	bool result = false;
	VectorOperations::ExecType<bool>(left, [&](bool value, index_t i, index_t k) {
		if (!left.nullmask[i]) {
			result = result || value;
		}
	});
	return result;
}

bool VectorOperations::AllTrue(Vector &left) {
	if (left.type != TypeId::BOOLEAN) {
		throw InvalidTypeException(left.type, "AllTrue can only be computed for boolean columns!");
	}
	if (left.count == 0) {
		return false;
	}
	bool result = true;
	VectorOperations::ExecType<bool>(left, [&](bool value, index_t i, index_t k) {
		if (left.nullmask[i]) {
			result = false;
		} else {
			result = result && value;
		}
	});
	return result;
}

bool VectorOperations::Contains(Vector &vector, Value &value) {
	if (vector.count == 0) {
		return false;
	}
	ConstantVector right(value.CastAs(vector.type));
	StaticVector<bool> comparison_result;
	VectorOperations::Equals(vector, right, comparison_result);
	return VectorOperations::AnyTrue(comparison_result);
}

bool VectorOperations::HasNull(Vector &left) {
	return left.nullmask.any();
}

Value VectorOperations::MaximumStringLength(Vector &left) {
	if (left.type != TypeId::VARCHAR) {
		throw InvalidTypeException(left.type, "String length can only be computed for char array columns!");
	}
	auto result = Value::BIGINT(0);
	if (left.count == 0) {
		return result;
	}
	templated_unary_fold<const char *, int64_t, duckdb::MaximumStringLength>(left, &result.value_.bigint);
	return result;
}
