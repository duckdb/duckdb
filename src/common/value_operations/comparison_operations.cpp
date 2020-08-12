#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"

namespace duckdb {
using namespace std;

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//
template <class OP> static bool templated_boolean_operation(const Value &left, const Value &right) {
	if (left.type != right.type) {
		PhysicalType left_cast = PhysicalType::INVALID, right_cast = PhysicalType::INVALID;
		if (TypeIsNumeric(left.type) && TypeIsNumeric(right.type)) {
			if (NumericTypeOrder(left.type) < NumericTypeOrder(right.type)) {
				left_cast = right.type;
			} else {
				right_cast = left.type;
			}
		} else if (left.type == PhysicalType::BOOL) {
			right_cast = PhysicalType::BOOL;
		} else if (right.type == PhysicalType::BOOL) {
			left_cast = PhysicalType::BOOL;
		}
		if (left_cast != PhysicalType::INVALID) {
			return templated_boolean_operation<OP>(left.CastAs(left_cast), right);
		} else if (right_cast != PhysicalType::INVALID) {
			return templated_boolean_operation<OP>(left, right.CastAs(right_cast));
		}
		return false;
	}
	switch (left.type) {
	case PhysicalType::BOOL:
		return OP::Operation(left.value_.boolean, right.value_.boolean);
	case PhysicalType::INT8:
		return OP::Operation(left.value_.tinyint, right.value_.tinyint);
	case PhysicalType::INT16:
		return OP::Operation(left.value_.smallint, right.value_.smallint);
	case PhysicalType::INT32:
		return OP::Operation(left.value_.integer, right.value_.integer);
	case PhysicalType::INT64:
		return OP::Operation(left.value_.bigint, right.value_.bigint);
	case PhysicalType::INT128:
		return OP::Operation(left.value_.hugeint, right.value_.hugeint);
	case PhysicalType::POINTER:
		return OP::Operation(left.value_.pointer, right.value_.pointer);
	case PhysicalType::HASH:
		return OP::Operation(left.value_.hash, right.value_.hash);
	case PhysicalType::FLOAT:
		return OP::Operation(left.value_.float_, right.value_.float_);
	case PhysicalType::DOUBLE:
		return OP::Operation(left.value_.double_, right.value_.double_);
	case PhysicalType::INTERVAL:
		return OP::Operation(left.value_.interval, right.value_.interval);
	case PhysicalType::VARCHAR:
		return OP::Operation(left.str_value, right.str_value);
	case PhysicalType::STRUCT: {
		for (idx_t i = 0; i < left.struct_value.size(); i++) {
			if (i >= right.struct_value.size() || left.struct_value[i].first != right.struct_value[i].first ||
			    left.struct_value[i].second != left.struct_value[i].second) {
				return false;
			}
		}
		return true;
	}
	case PhysicalType::LIST: {
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

} // namespace duckdb
