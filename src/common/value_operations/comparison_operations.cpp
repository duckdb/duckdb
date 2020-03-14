#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//
template <class OP> static bool templated_boolean_operation(const Value &left, const Value &right) {
	if (left.type != right.type) {
		TypeId left_cast = TypeId::INVALID, right_cast = TypeId::INVALID;
		if (TypeIsNumeric(left.type) && TypeIsNumeric(right.type)) {
			if (left.type < right.type) {
				left_cast = right.type;
			} else {
				right_cast = left.type;
			}
		} else if (left.type == TypeId::BOOL) {
			right_cast = TypeId::BOOL;
		} else if (right.type == TypeId::BOOL) {
			left_cast = TypeId::BOOL;
		}
		if (left_cast != TypeId::INVALID) {
			return templated_boolean_operation<OP>(left.CastAs(left_cast), right);
		} else if (right_cast != TypeId::INVALID) {
			return templated_boolean_operation<OP>(left, right.CastAs(right_cast));
		}
		return false;
	}
	switch (left.type) {
	case TypeId::BOOL:
		return OP::Operation(left.value_.boolean, right.value_.boolean);
	case TypeId::INT8:
		return OP::Operation(left.value_.tinyint, right.value_.tinyint);
	case TypeId::INT16:
		return OP::Operation(left.value_.smallint, right.value_.smallint);
	case TypeId::INT32:
		return OP::Operation(left.value_.integer, right.value_.integer);
	case TypeId::INT64:
		return OP::Operation(left.value_.bigint, right.value_.bigint);
	case TypeId::POINTER:
		return OP::Operation(left.value_.pointer, right.value_.pointer);
	case TypeId::HASH:
		return OP::Operation(left.value_.hash, right.value_.hash);
	case TypeId::FLOAT:
		return OP::Operation(left.value_.float_, right.value_.float_);
	case TypeId::DOUBLE:
		return OP::Operation(left.value_.double_, right.value_.double_);
	case TypeId::VARCHAR:
		return OP::Operation(left.str_value, right.str_value);
	case TypeId::STRUCT: {
		for (idx_t i = 0; i < left.struct_value.size(); i++) {
			if (i >= right.struct_value.size() || left.struct_value[i].first != right.struct_value[i].first ||
			    left.struct_value[i].second != left.struct_value[i].second) {
				return false;
			}
		}
		return true;
	}
	case TypeId::LIST: {
		return left.list_value == right.list_value;
	}
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
	return templated_boolean_operation<duckdb::Equals>(left, right);
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
	return templated_boolean_operation<duckdb::GreaterThan>(left, right);
}

bool ValueOperations::GreaterThanEquals(const Value &left, const Value &right) {
	if (left.is_null && right.is_null) {
		return true;
	} else if (right.is_null) {
		return true;
	} else if (left.is_null) {
		return false;
	}
	return templated_boolean_operation<duckdb::GreaterThanEquals>(left, right);
}

bool ValueOperations::LessThan(const Value &left, const Value &right) {
	return ValueOperations::GreaterThan(right, left);
}

bool ValueOperations::LessThanEquals(const Value &left, const Value &right) {
	return ValueOperations::GreaterThanEquals(right, left);
}
