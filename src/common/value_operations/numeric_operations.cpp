#include "common/exception.hpp"
#include "common/limits.hpp"
#include "common/operator/aggregate_operators.hpp"
#include "common/operator/numeric_binary_operators.hpp"
#include "common/value_operations/value_operations.hpp"

using namespace duckdb;
using namespace std;

template <class OP, bool IGNORE_NULL> static Value templated_binary_operation(const Value &left, const Value &right) {
	Value result;
	if (left.is_null || right.is_null) {
		if (IGNORE_NULL) {
			if (!right.is_null) {
				result = right;
			} else {
				result = left;
			}
		} else {
			result.type = max(left.type, right.type);
			result.is_null = true;
		}
		return result;
	}
	result.is_null = false;
	if (TypeIsIntegral(left.type) && TypeIsIntegral(right.type) &&
	    (left.type < TypeId::BIGINT || right.type < TypeId::BIGINT)) {
		// upcast integer types if necessary
		Value left_cast = left.CastAs(TypeId::BIGINT);
		Value right_cast = right.CastAs(TypeId::BIGINT);
		result = templated_binary_operation<OP, IGNORE_NULL>(left_cast, right_cast);
		if (result.is_null) {
			result.type = max(left.type, right.type);
		} else {
			auto type = max(MinimalType(result.GetNumericValue()), max(left.type, right.type));
			result = result.CastAs(type);
		}
		return result;
	}
	if (TypeIsIntegral(left.type) && right.type == TypeId::DECIMAL) {
		Value left_cast = left.CastAs(TypeId::DECIMAL);
		return templated_binary_operation<OP, IGNORE_NULL>(left_cast, right);
	}
	if (left.type == TypeId::DECIMAL && TypeIsIntegral(right.type)) {
		Value right_cast = right.CastAs(TypeId::DECIMAL);
		return templated_binary_operation<OP, IGNORE_NULL>(left, right_cast);
	}
	if (left.type != right.type) {
		throw TypeMismatchException(left.type, right.type, "Cannot perform binary operation on these two types");
	}
	result.type = left.type;
	switch (left.type) {
	case TypeId::BOOLEAN:
		result.value_.boolean = OP::Operation(left.value_.boolean, right.value_.boolean);
		break;
	case TypeId::TINYINT:
		result.value_.tinyint = OP::Operation(left.value_.tinyint, right.value_.tinyint);
		break;
	case TypeId::SMALLINT:
		result.value_.smallint = OP::Operation(left.value_.smallint, right.value_.smallint);
		break;
	case TypeId::INTEGER:
		result.value_.integer = OP::Operation(left.value_.integer, right.value_.integer);
		break;
	case TypeId::BIGINT:
		result.value_.bigint = OP::Operation(left.value_.bigint, right.value_.bigint);
		break;
	case TypeId::DATE:
		result.value_.date = OP::Operation(left.value_.date, right.value_.date);
		break;
	case TypeId::TIMESTAMP:
		result.value_.timestamp = OP::Operation(left.value_.timestamp, right.value_.timestamp);
		break;
	case TypeId::DECIMAL:
		result.value_.decimal = OP::Operation(left.value_.decimal, right.value_.decimal);
		break;
	case TypeId::POINTER:
		result.value_.pointer = OP::Operation(left.value_.pointer, right.value_.pointer);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Numeric Operations
//===--------------------------------------------------------------------===//
Value ValueOperations::Add(const Value &left, const Value &right) {
	return templated_binary_operation<operators::Add, false>(left, right);
}

Value ValueOperations::Subtract(const Value &left, const Value &right) {
	return templated_binary_operation<operators::Subtract, false>(left, right);
}

Value ValueOperations::Multiply(const Value &left, const Value &right) {
	return templated_binary_operation<operators::Multiply, false>(left, right);
}

Value ValueOperations::Modulo(const Value &left, const Value &right) {
	throw NotImplementedException("Value modulo");
}

Value ValueOperations::Divide(const Value &left, const Value &right) {
	Value zero = Value::Numeric(right.type, 0);
	if (right == 0) {
		// special case: divide by zero
		Value result;
		result.type = max(left.type, right.type);
		result.is_null = true;
		return result;
	} else {
		return templated_binary_operation<operators::Divide, false>(left, right);
	}
}

Value ValueOperations::Min(const Value &left, const Value &right) {
	return templated_binary_operation<operators::Min, true>(left, right);
}

Value ValueOperations::Max(const Value &left, const Value &right) {
	return templated_binary_operation<operators::Max, true>(left, right);
}
