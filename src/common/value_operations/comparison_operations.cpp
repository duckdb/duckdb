
#include "common/exception.hpp"
#include "common/operator/comparison_operators.hpp"
#include "common/value_operations/value_operations.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//
template <class OP>
static bool templated_boolean_operation(const Value &left, const Value &right) {
	if (left.type != right.type) {
		if (TypeIsNumeric(left.type) && TypeIsNumeric(right.type)) {
			if (left.type < right.type) {
				Value left_cast = left.CastAs(right.type);
				return templated_boolean_operation<OP>(left_cast, right);
			} else {
				Value right_cast = right.CastAs(left.type);
				return templated_boolean_operation<OP>(left, right_cast);
			}
		}
		throw TypeMismatchException(
		    left.type, right.type,
		    "Cannot perform boolean operation on these two types");
	}
	switch (left.type) {
	case TypeId::BOOLEAN:
		return OP::Operation(left.value_.boolean, right.value_.boolean);
	case TypeId::TINYINT:
		return OP::Operation(left.value_.tinyint, right.value_.tinyint);
	case TypeId::SMALLINT:
		return OP::Operation(left.value_.smallint, right.value_.smallint);
	case TypeId::INTEGER:
		return OP::Operation(left.value_.integer, right.value_.integer);
	case TypeId::BIGINT:
		return OP::Operation(left.value_.bigint, right.value_.bigint);
	case TypeId::DECIMAL:
		return OP::Operation(left.value_.decimal, right.value_.decimal);
	case TypeId::POINTER:
		return OP::Operation(left.value_.pointer, right.value_.pointer);
	case TypeId::DATE:
		return OP::Operation(left.value_.date, right.value_.date);
	case TypeId::TIMESTAMP:
		return OP::Operation(left.value_.timestamp, right.value_.timestamp);
	case TypeId::VARCHAR:
		return OP::Operation(left.str_value, right.str_value);
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

bool ValueOperations::Equals(const Value &left, const Value &right) {
	if (left.is_null && right.is_null) {
		return true;
	}
	if (left.is_null != right.is_null) {
		return false;
	}
	return templated_boolean_operation<operators::Equals>(left, right);
}

bool ValueOperations::NotEquals(const Value &left, const Value &right) {
	return !ValueOperations::Equals(left, right);
}

bool ValueOperations::GreaterThan(const Value &left, const Value &right) {
	if (left.is_null && right.is_null) {
		return false;
	} else if (right.is_null) {
		return true;
	} else if (left.is_null) {
		return false;
	}
	return templated_boolean_operation<operators::GreaterThan>(left, right);
}

bool ValueOperations::GreaterThanEquals(const Value &left, const Value &right) {
	if (left.is_null && right.is_null) {
		return true;
	} else if (right.is_null) {
		return true;
	} else if (left.is_null) {
		return false;
	}
	return templated_boolean_operation<operators::GreaterThanEquals>(left,
	                                                                 right);
}

bool ValueOperations::LessThan(const Value &left, const Value &right) {
	return ValueOperations::GreaterThan(right, left);
}

bool ValueOperations::LessThanEquals(const Value &left, const Value &right) {
	return ValueOperations::GreaterThanEquals(right, left);
}
