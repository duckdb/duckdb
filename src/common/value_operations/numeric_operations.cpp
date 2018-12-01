
#include "common/exception.hpp"
#include "common/limits.hpp"
#include "common/operator/aggregate_operators.hpp"
#include "common/operator/numeric_binary_operators.hpp"
#include "common/value_operations/value_operations.hpp"

using namespace duckdb;
using namespace std;

template <class OP>
static void templated_binary_operation(const Value &left, const Value &right,
                                       Value &result, bool ignore_null) {
	if (left.is_null || right.is_null) {
		if (ignore_null) {
			if (!right.is_null) {
				result = right;
			} else {
				result = left;
			}
		} else {
			result.type = max(left.type, right.type);
			result.is_null = true;
		}
		return;
	}
	result.is_null = false;
	if (TypeIsIntegral(left.type) && TypeIsIntegral(right.type) &&
	    (left.type < TypeId::BIGINT || right.type < TypeId::BIGINT)) {
		// upcast integer types if necessary
		Value left_cast = left.CastAs(TypeId::BIGINT);
		Value right_cast = right.CastAs(TypeId::BIGINT);
		templated_binary_operation<OP>(left_cast, right_cast, result,
		                               ignore_null);
		if (result.is_null) {
			result.type = max(left.type, right.type);
		} else {
			auto type = max(MinimalType(result.GetNumericValue()),
			                max(left.type, right.type));
			result = result.CastAs(type);
		}
		return;
	}
	if (TypeIsIntegral(left.type) && right.type == TypeId::DECIMAL) {
		Value left_cast = left.CastAs(TypeId::DECIMAL);
		templated_binary_operation<OP>(left_cast, right, result, ignore_null);
		return;
	}
	if (left.type == TypeId::DECIMAL && TypeIsIntegral(right.type)) {
		Value right_cast = right.CastAs(TypeId::DECIMAL);
		templated_binary_operation<OP>(left, right_cast, result, ignore_null);
		return;
	}
	if (left.type != right.type) {
		throw TypeMismatchException(
		    left.type, right.type,
		    "Cannot perform binary operation on these two types");
	}
	result.type = left.type;
	switch (left.type) {
	case TypeId::BOOLEAN:
		result.value_.boolean =
		    OP::Operation(left.value_.boolean, right.value_.boolean);
		break;
	case TypeId::TINYINT:
		result.value_.tinyint =
		    OP::Operation(left.value_.tinyint, right.value_.tinyint);
		break;
	case TypeId::SMALLINT:
		result.value_.smallint =
		    OP::Operation(left.value_.smallint, right.value_.smallint);
		break;
	case TypeId::INTEGER:
		result.value_.integer =
		    OP::Operation(left.value_.integer, right.value_.integer);
		break;
	case TypeId::BIGINT:
		result.value_.bigint =
		    OP::Operation(left.value_.bigint, right.value_.bigint);
		break;
	case TypeId::DATE:
		result.value_.date = OP::Operation(left.value_.date, right.value_.date);
		break;
	case TypeId::TIMESTAMP:
		result.value_.timestamp =
		    OP::Operation(left.value_.timestamp, right.value_.timestamp);
		break;
	case TypeId::DECIMAL:
		result.value_.decimal =
		    OP::Operation(left.value_.decimal, right.value_.decimal);
		break;
	case TypeId::POINTER:
		result.value_.pointer =
		    OP::Operation(left.value_.pointer, right.value_.pointer);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

//===--------------------------------------------------------------------===//
// Numeric Operations
//===--------------------------------------------------------------------===//
void ValueOperations::Add(const Value &left, const Value &right,
                          Value &result) {
	templated_binary_operation<operators::Add>(left, right, result, false);
}

void ValueOperations::Subtract(const Value &left, const Value &right,
                               Value &result) {
	templated_binary_operation<operators::Subtract>(left, right, result, false);
}

void ValueOperations::Multiply(const Value &left, const Value &right,
                               Value &result) {
	templated_binary_operation<operators::Multiply>(left, right, result, false);
}

void ValueOperations::Divide(const Value &left, const Value &right,
                             Value &result) {
	Value zero = Value::Numeric(right.type, 0);
	if (ValueOperations::Equals(right, zero)) {
		// special case: divide by zero
		result.type = max(left.type, right.type);
		result.is_null = true;
	} else {
		templated_binary_operation<operators::Divide>(left, right, result,
		                                              false);
	}
}

void ValueOperations::Min(const Value &left, const Value &right,
                          Value &result) {
	templated_binary_operation<operators::Min>(left, right, result, true);
}

void ValueOperations::Max(const Value &left, const Value &right,
                          Value &result) {
	templated_binary_operation<operators::Max>(left, right, result, true);
}
