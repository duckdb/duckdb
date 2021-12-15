#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//

struct ValuePositionComparator {
	// Return true if the positional Values definitely match.
	// Default to the same as the final value
	template <typename OP>
	static inline bool Definite(const Value &lhs, const Value &rhs) {
		return Final<OP>(lhs, rhs);
	}

	// Select the positional Values that need further testing.
	// Usually this means Is Not Distinct, as those are the semantics used by Postges
	template <typename OP>
	static inline bool Possible(const Value &lhs, const Value &rhs) {
		return ValueOperations::NotDistinctFrom(lhs, rhs);
	}

	// Return true if the positional Values definitely match in the final position
	// This needs to be specialised.
	template <typename OP>
	static inline bool Final(const Value &lhs, const Value &rhs) {
		return false;
	}

	// Tie-break based on length when one of the sides has been exhausted, returning true if the LHS matches.
	// This essentially means that the existing positions compare equal.
	// Default to the same semantics as the OP for idx_t. This works in most cases.
	template <typename OP>
	static inline bool TieBreak(const idx_t lpos, const idx_t rpos) {
		return OP::Operation(lpos, rpos);
	}
};

// Equals must always check every column
template <>
inline bool ValuePositionComparator::Definite<duckdb::Equals>(const Value &lhs, const Value &rhs) {
	return false;
}

template <>
inline bool ValuePositionComparator::Final<duckdb::Equals>(const Value &lhs, const Value &rhs) {
	return ValueOperations::NotDistinctFrom(lhs, rhs);
}

// NotEquals must check everything that matched
template <>
inline bool ValuePositionComparator::Possible<duckdb::NotEquals>(const Value &lhs, const Value &rhs) {
	return true;
}

template <>
inline bool ValuePositionComparator::Final<duckdb::NotEquals>(const Value &lhs, const Value &rhs) {
	return ValueOperations::NotDistinctFrom(lhs, rhs);
}

// Non-strict inequalities must use strict comparisons for Definite
template <>
bool ValuePositionComparator::Definite<duckdb::LessThanEquals>(const Value &lhs, const Value &rhs) {
	return ValueOperations::DistinctLessThan(lhs, rhs);
}

template <>
bool ValuePositionComparator::Final<duckdb::LessThanEquals>(const Value &lhs, const Value &rhs) {
	return ValueOperations::DistinctLessThanEquals(lhs, rhs);
}

template <>
bool ValuePositionComparator::Definite<duckdb::GreaterThanEquals>(const Value &lhs, const Value &rhs) {
	return ValueOperations::DistinctGreaterThan(lhs, rhs);
}

template <>
bool ValuePositionComparator::Final<duckdb::GreaterThanEquals>(const Value &lhs, const Value &rhs) {
	return ValueOperations::DistinctGreaterThanEquals(lhs, rhs);
}

// Strict inequalities just use strict for both Definite and Final
template <>
bool ValuePositionComparator::Final<duckdb::LessThan>(const Value &lhs, const Value &rhs) {
	return ValueOperations::DistinctLessThan(lhs, rhs);
}

template <>
bool ValuePositionComparator::Final<duckdb::GreaterThan>(const Value &lhs, const Value &rhs) {
	return ValueOperations::DistinctGreaterThan(lhs, rhs);
}

template <class OP>
static bool TemplatedBooleanOperation(const Value &left, const Value &right) {
	const auto &left_type = left.type();
	const auto &right_type = right.type();
	if (left_type != right_type) {
		Value left_copy = left;
		Value right_copy = right;

		LogicalType comparison_type = BoundComparisonExpression::BindComparison(left_type, right_type);
		if (!left_copy.TryCastAs(comparison_type) || !right_copy.TryCastAs(comparison_type)) {
			return false;
		}
		D_ASSERT(left_copy.type() == right_copy.type());
		return TemplatedBooleanOperation<OP>(left_copy, right_copy);
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
		idx_t i = 0;
		for (; i < left.struct_value.size() - 1; ++i) {
			if (ValuePositionComparator::Definite<OP>(left.struct_value[i], right.struct_value[i])) {
				return true;
			}
			if (!ValuePositionComparator::Possible<OP>(left.struct_value[i], right.struct_value[i])) {
				return false;
			}
		}
		return ValuePositionComparator::Final<OP>(left.struct_value[i], right.struct_value[i]);
	}
	case PhysicalType::LIST: {
		for (idx_t pos = 0;; ++pos) {
			if (pos == left.list_value.size() || pos == right.list_value.size()) {
				return ValuePositionComparator::TieBreak<OP>(left.list_value.size(), right.list_value.size());
			}
			if (ValuePositionComparator::Definite<OP>(left.list_value[pos], right.list_value[pos])) {
				return true;
			}
			if (!ValuePositionComparator::Possible<OP>(left.list_value[pos], right.list_value[pos])) {
				return false;
			}
		}
		return false;
	}
	default:
		throw InternalException("Unimplemented type for value comparison");
	}
}

bool ValueOperations::Equals(const Value &left, const Value &right) {
	if (left.is_null || right.is_null) {
		throw InternalException("Comparison on NULL values");
	}
	return TemplatedBooleanOperation<duckdb::Equals>(left, right);
}

bool ValueOperations::NotEquals(const Value &left, const Value &right) {
	return !ValueOperations::Equals(left, right);
}

bool ValueOperations::GreaterThan(const Value &left, const Value &right) {
	if (left.is_null || right.is_null) {
		throw InternalException("Comparison on NULL values");
	}
	return TemplatedBooleanOperation<duckdb::GreaterThan>(left, right);
}

bool ValueOperations::GreaterThanEquals(const Value &left, const Value &right) {
	if (left.is_null || right.is_null) {
		throw InternalException("Comparison on NULL values");
	}
	return TemplatedBooleanOperation<duckdb::GreaterThanEquals>(left, right);
}

bool ValueOperations::LessThan(const Value &left, const Value &right) {
	return ValueOperations::GreaterThan(right, left);
}

bool ValueOperations::LessThanEquals(const Value &left, const Value &right) {
	return ValueOperations::GreaterThanEquals(right, left);
}

bool ValueOperations::NotDistinctFrom(const Value &left, const Value &right) {
	if (left.is_null && right.is_null) {
		return true;
	}
	if (left.is_null != right.is_null) {
		return false;
	}
	return TemplatedBooleanOperation<duckdb::Equals>(left, right);
}

bool ValueOperations::DistinctFrom(const Value &left, const Value &right) {
	return !ValueOperations::NotDistinctFrom(left, right);
}

bool ValueOperations::DistinctGreaterThan(const Value &left, const Value &right) {
	if (left.is_null && right.is_null) {
		return false;
	} else if (right.is_null) {
		return false;
	} else if (left.is_null) {
		return true;
	}
	return TemplatedBooleanOperation<duckdb::GreaterThan>(left, right);
}

bool ValueOperations::DistinctGreaterThanEquals(const Value &left, const Value &right) {
	if (left.is_null) {
		return true;
	} else if (right.is_null) {
		return false;
	}
	return TemplatedBooleanOperation<duckdb::GreaterThanEquals>(left, right);
}

bool ValueOperations::DistinctLessThan(const Value &left, const Value &right) {
	return ValueOperations::DistinctGreaterThan(right, left);
}

bool ValueOperations::DistinctLessThanEquals(const Value &left, const Value &right) {
	return ValueOperations::DistinctGreaterThanEquals(right, left);
}

} // namespace duckdb
