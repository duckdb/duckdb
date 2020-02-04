#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/operator/aggregate_operators.hpp"

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
	if (left.type != right.type) {
		if (TypeIsIntegral(left.type) && TypeIsIntegral(right.type) &&
		    (left.type < TypeId::INT64 || right.type < TypeId::INT64)) {
			// upcast integer types if necessary
			Value left_cast = left.CastAs(TypeId::INT64);
			Value right_cast = right.CastAs(TypeId::INT64);
			result = templated_binary_operation<OP, IGNORE_NULL>(left_cast, right_cast);
			if (result.is_null) {
				result.type = max(left.type, right.type);
			} else {
				auto type = max(MinimalType(result.GetValue<int64_t>()), max(left.type, right.type));
				result = result.CastAs(type);
			}
			return result;
		}
		if (TypeIsIntegral(left.type) && TypeIsNumeric(right.type)) {
			Value left_cast = left.CastAs(right.type);
			return templated_binary_operation<OP, IGNORE_NULL>(left_cast, right);
		}
		if (TypeIsNumeric(left.type) && TypeIsIntegral(right.type)) {
			Value right_cast = right.CastAs(left.type);
			return templated_binary_operation<OP, IGNORE_NULL>(left, right_cast);
		}
		throw TypeMismatchException(left.type, right.type, "Cannot perform binary operation on these two types");
	}
	result.type = left.type;
	switch (left.type) {
	case TypeId::BOOL:
		result.value_.boolean = OP::Operation(left.value_.boolean, right.value_.boolean);
		break;
	case TypeId::INT8:
		result.value_.tinyint = OP::Operation(left.value_.tinyint, right.value_.tinyint);
		break;
	case TypeId::INT16:
		result.value_.smallint = OP::Operation(left.value_.smallint, right.value_.smallint);
		break;
	case TypeId::INT32:
		result.value_.integer = OP::Operation(left.value_.integer, right.value_.integer);
		break;
	case TypeId::INT64:
		result.value_.bigint = OP::Operation(left.value_.bigint, right.value_.bigint);
		break;
	case TypeId::FLOAT:
		result.value_.float_ = OP::Operation(left.value_.float_, right.value_.float_);
		break;
	case TypeId::DOUBLE:
		result.value_.double_ = OP::Operation(left.value_.double_, right.value_.double_);
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
	return templated_binary_operation<duckdb::Add, false>(left, right);
}

Value ValueOperations::Subtract(const Value &left, const Value &right) {
	return templated_binary_operation<duckdb::Subtract, false>(left, right);
}

Value ValueOperations::Multiply(const Value &left, const Value &right) {
	return templated_binary_operation<duckdb::Multiply, false>(left, right);
}

Value ValueOperations::Modulo(const Value &left, const Value &right) {
	if (!TypeIsIntegral(left.type) || !TypeIsIntegral(right.type)) {
		throw InvalidTypeException(left.type, "Arguments to modulo must be integral");
	}
	if (left.type != right.type) {
		if (left.type < right.type) {
			return Modulo(left.CastAs(right.type), right);
		} else {
			return Modulo(left, right.CastAs(left.type));
		}
	}
	if (left.is_null || right.is_null) {
		return Value(left.type);
	}
	Value result;
	result.is_null = false;
	result.type = left.type;
	switch (left.type) {
	case TypeId::INT8:
		return Value::TINYINT(left.value_.tinyint % right.value_.tinyint);
		break;
	case TypeId::INT16:
		return Value::SMALLINT(left.value_.smallint % right.value_.smallint);
		break;
	case TypeId::INT32:
		return Value::INTEGER(left.value_.integer % right.value_.integer);
		break;
	case TypeId::INT64:
		result.value_.bigint = left.value_.bigint % right.value_.bigint;
		break;
	default:
		throw NotImplementedException("Unimplemented type for modulo");
	}
	return result;
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
		return templated_binary_operation<duckdb::Divide, false>(left, right);
	}
}

Value ValueOperations::Min(const Value &left, const Value &right) {
	return templated_binary_operation<duckdb::Min, true>(left, right);
}

Value ValueOperations::Max(const Value &left, const Value &right) {
	return templated_binary_operation<duckdb::Max, true>(left, right);
}
