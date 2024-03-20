#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/uhugeint.hpp"
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
	return !ValuePositionComparator::Definite<duckdb::GreaterThan>(lhs, rhs);
}

template <>
bool ValuePositionComparator::Final<duckdb::GreaterThan>(const Value &lhs, const Value &rhs) {
	return ValueOperations::DistinctGreaterThan(lhs, rhs);
}

template <>
bool ValuePositionComparator::Final<duckdb::LessThanEquals>(const Value &lhs, const Value &rhs) {
	return !ValuePositionComparator::Final<duckdb::GreaterThan>(lhs, rhs);
}

template <>
bool ValuePositionComparator::Definite<duckdb::GreaterThanEquals>(const Value &lhs, const Value &rhs) {
	return !ValuePositionComparator::Definite<duckdb::GreaterThan>(rhs, lhs);
}

template <>
bool ValuePositionComparator::Final<duckdb::GreaterThanEquals>(const Value &lhs, const Value &rhs) {
	return !ValuePositionComparator::Final<duckdb::GreaterThan>(rhs, lhs);
}

// Strict inequalities just use strict for both Definite and Final
template <>
bool ValuePositionComparator::Final<duckdb::LessThan>(const Value &lhs, const Value &rhs) {
	return ValuePositionComparator::Final<duckdb::GreaterThan>(rhs, lhs);
}

template <class OP>
static bool TemplatedBooleanOperation(const Value &left, const Value &right) {
	const auto &left_type = left.type();
	const auto &right_type = right.type();
	if (left_type != right_type) {
		Value left_copy = left;
		Value right_copy = right;

		auto comparison_type = LogicalType::ForceMaxLogicalType(left_type, right_type);
		if (!left_copy.DefaultTryCastAs(comparison_type) || !right_copy.DefaultTryCastAs(comparison_type)) {
			return false;
		}
		D_ASSERT(left_copy.type() == right_copy.type());
		return TemplatedBooleanOperation<OP>(left_copy, right_copy);
	}
	switch (left_type.InternalType()) {
	case PhysicalType::BOOL:
		return OP::Operation(left.GetValueUnsafe<bool>(), right.GetValueUnsafe<bool>());
	case PhysicalType::INT8:
		return OP::Operation(left.GetValueUnsafe<int8_t>(), right.GetValueUnsafe<int8_t>());
	case PhysicalType::INT16:
		return OP::Operation(left.GetValueUnsafe<int16_t>(), right.GetValueUnsafe<int16_t>());
	case PhysicalType::INT32:
		return OP::Operation(left.GetValueUnsafe<int32_t>(), right.GetValueUnsafe<int32_t>());
	case PhysicalType::INT64:
		return OP::Operation(left.GetValueUnsafe<int64_t>(), right.GetValueUnsafe<int64_t>());
	case PhysicalType::UINT8:
		return OP::Operation(left.GetValueUnsafe<uint8_t>(), right.GetValueUnsafe<uint8_t>());
	case PhysicalType::UINT16:
		return OP::Operation(left.GetValueUnsafe<uint16_t>(), right.GetValueUnsafe<uint16_t>());
	case PhysicalType::UINT32:
		return OP::Operation(left.GetValueUnsafe<uint32_t>(), right.GetValueUnsafe<uint32_t>());
	case PhysicalType::UINT64:
		return OP::Operation(left.GetValueUnsafe<uint64_t>(), right.GetValueUnsafe<uint64_t>());
	case PhysicalType::UINT128:
		return OP::Operation(left.GetValueUnsafe<uhugeint_t>(), right.GetValueUnsafe<uhugeint_t>());
	case PhysicalType::INT128:
		return OP::Operation(left.GetValueUnsafe<hugeint_t>(), right.GetValueUnsafe<hugeint_t>());
	case PhysicalType::FLOAT:
		return OP::Operation(left.GetValueUnsafe<float>(), right.GetValueUnsafe<float>());
	case PhysicalType::DOUBLE:
		return OP::Operation(left.GetValueUnsafe<double>(), right.GetValueUnsafe<double>());
	case PhysicalType::INTERVAL:
		return OP::Operation(left.GetValueUnsafe<interval_t>(), right.GetValueUnsafe<interval_t>());
	case PhysicalType::VARCHAR:
		return OP::Operation(StringValue::Get(left), StringValue::Get(right));
	case PhysicalType::STRUCT: {
		auto &left_children = StructValue::GetChildren(left);
		auto &right_children = StructValue::GetChildren(right);
		// this should be enforced by the type
		D_ASSERT(left_children.size() == right_children.size());
		idx_t i = 0;
		for (; i < left_children.size() - 1; ++i) {
			if (ValuePositionComparator::Definite<OP>(left_children[i], right_children[i])) {
				return true;
			}
			if (!ValuePositionComparator::Possible<OP>(left_children[i], right_children[i])) {
				return false;
			}
		}
		return ValuePositionComparator::Final<OP>(left_children[i], right_children[i]);
	}
	case PhysicalType::LIST: {
		auto &left_children = ListValue::GetChildren(left);
		auto &right_children = ListValue::GetChildren(right);
		for (idx_t pos = 0;; ++pos) {
			if (pos == left_children.size() || pos == right_children.size()) {
				return ValuePositionComparator::TieBreak<OP>(left_children.size(), right_children.size());
			}
			if (ValuePositionComparator::Definite<OP>(left_children[pos], right_children[pos])) {
				return true;
			}
			if (!ValuePositionComparator::Possible<OP>(left_children[pos], right_children[pos])) {
				return false;
			}
		}
		return false;
	}
	case PhysicalType::ARRAY: {
		auto &left_children = ArrayValue::GetChildren(left);
		auto &right_children = ArrayValue::GetChildren(right);

		// Should be enforced by the type
		D_ASSERT(left_children.size() == right_children.size());

		for (idx_t i = 0; i < left_children.size(); i++) {
			if (ValuePositionComparator::Definite<OP>(left_children[i], right_children[i])) {
				return true;
			}
			if (!ValuePositionComparator::Possible<OP>(left_children[i], right_children[i])) {
				return false;
			}
		}
		return true;
	}
	default:
		throw InternalException("Unimplemented type for value comparison");
	}
}

bool ValueOperations::Equals(const Value &left, const Value &right) {
	if (left.IsNull() || right.IsNull()) {
		throw InternalException("Comparison on NULL values");
	}
	return TemplatedBooleanOperation<duckdb::Equals>(left, right);
}

bool ValueOperations::NotEquals(const Value &left, const Value &right) {
	return !ValueOperations::Equals(left, right);
}

bool ValueOperations::GreaterThan(const Value &left, const Value &right) {
	if (left.IsNull() || right.IsNull()) {
		throw InternalException("Comparison on NULL values");
	}
	return TemplatedBooleanOperation<duckdb::GreaterThan>(left, right);
}

bool ValueOperations::GreaterThanEquals(const Value &left, const Value &right) {
	return !ValueOperations::GreaterThan(right, left);
}

bool ValueOperations::LessThan(const Value &left, const Value &right) {
	return ValueOperations::GreaterThan(right, left);
}

bool ValueOperations::LessThanEquals(const Value &left, const Value &right) {
	return !ValueOperations::GreaterThan(left, right);
}

bool ValueOperations::NotDistinctFrom(const Value &left, const Value &right) {
	if (left.IsNull() && right.IsNull()) {
		return true;
	}
	if (left.IsNull() != right.IsNull()) {
		return false;
	}
	return TemplatedBooleanOperation<duckdb::Equals>(left, right);
}

bool ValueOperations::DistinctFrom(const Value &left, const Value &right) {
	return !ValueOperations::NotDistinctFrom(left, right);
}

bool ValueOperations::DistinctGreaterThan(const Value &left, const Value &right) {
	if (left.IsNull() && right.IsNull()) {
		return false;
	} else if (right.IsNull()) {
		return false;
	} else if (left.IsNull()) {
		return true;
	}
	return TemplatedBooleanOperation<duckdb::GreaterThan>(left, right);
}

bool ValueOperations::DistinctGreaterThanEquals(const Value &left, const Value &right) {
	return !ValueOperations::DistinctGreaterThan(right, left);
}

bool ValueOperations::DistinctLessThan(const Value &left, const Value &right) {
	return ValueOperations::DistinctGreaterThan(right, left);
}

bool ValueOperations::DistinctLessThanEquals(const Value &left, const Value &right) {
	return !ValueOperations::DistinctGreaterThan(left, right);
}

} // namespace duckdb
