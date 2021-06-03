#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//
template <class OP>
static bool TemplatedBooleanOperation(const Value &left, const Value &right) {
	const auto &left_type = left.type();
	const auto &right_type = right.type();
	if (left_type != right_type) {
		try {
			LogicalType comparison_type = BoundComparisonExpression::BindComparison(left_type, right_type);
			return TemplatedBooleanOperation<OP>(left.CastAs(comparison_type), right.CastAs(comparison_type));
		} catch (...) {
			return false;
		}
	}
	switch (left_type.InternalType()) {
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
	case PhysicalType::UINT8:
		return OP::Operation(left.value_.utinyint, right.value_.utinyint);
	case PhysicalType::UINT16:
		return OP::Operation(left.value_.usmallint, right.value_.usmallint);
	case PhysicalType::UINT32:
		return OP::Operation(left.value_.uinteger, right.value_.uinteger);
	case PhysicalType::UINT64:
		return OP::Operation(left.value_.ubigint, right.value_.ubigint);
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
		// this should be enforced by the type
		D_ASSERT(left.struct_value.size() == right.struct_value.size());
		for (idx_t i = 0; i < left.struct_value.size(); i++) {
			if (left.struct_value[i] != right.struct_value[i]) {
				return false;
			}
		}
		return true;
	}
	case PhysicalType::LIST: {
		return left.list_value == right.list_value;
	}
	default:
		throw InternalException("Unimplemented type for value comparison");
	}
}

bool ValueOperations::Equals(const Value &left, const Value &right) {
	if (left.is_null && right.is_null) {
		return true;
	}
	if (left.is_null != right.is_null) {
		return false;
	}
	return TemplatedBooleanOperation<duckdb::Equals>(left, right);
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
	return TemplatedBooleanOperation<duckdb::GreaterThan>(left, right);
}

bool ValueOperations::GreaterThanEquals(const Value &left, const Value &right) {
	if (left.is_null && right.is_null) {
		return true;
	} else if (right.is_null) {
		return true;
	} else if (left.is_null) {
		return false;
	}
	return TemplatedBooleanOperation<duckdb::GreaterThanEquals>(left, right);
}

bool ValueOperations::LessThan(const Value &left, const Value &right) {
	return ValueOperations::GreaterThan(right, left);
}

bool ValueOperations::LessThanEquals(const Value &left, const Value &right) {
	return ValueOperations::GreaterThanEquals(right, left);
}

} // namespace duckdb
