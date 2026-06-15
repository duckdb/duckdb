#include "duckdb/common/vector/for_view.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/for_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

namespace {

bool IsSupportedComparison(ExpressionType op) {
	switch (op) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return true;
	default:
		return false;
	}
}

bool IsThinFlatType(PhysicalType pt) {
	switch (pt) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
		return true;
	default:
		return false;
	}
}

bool IsThinForStored(PhysicalType pt) {
	return pt == PhysicalType::UINT8 || pt == PhysicalType::UINT16 || pt == PhysicalType::UINT32;
}

struct IntRange {
	int64_t lo;
	int64_t hi;
};

bool TryGetNarrowRange(PhysicalType pt, IntRange &range) {
	switch (pt) {
	case PhysicalType::INT8:
		range = {int64_t(INT8_MIN), int64_t(INT8_MAX)};
		return true;
	case PhysicalType::INT16:
		range = {int64_t(INT16_MIN), int64_t(INT16_MAX)};
		return true;
	case PhysicalType::INT32:
		range = {int64_t(INT32_MIN), int64_t(INT32_MAX)};
		return true;
	case PhysicalType::UINT8:
		range = {0, int64_t(UINT8_MAX)};
		return true;
	case PhysicalType::UINT16:
		range = {0, int64_t(UINT16_MAX)};
		return true;
	case PhysicalType::UINT32:
		range = {0, int64_t(UINT32_MAX)};
		return true;
	default:
		return false;
	}
}

bool TryCastConstantToInt64(const Value &constant, int64_t &out) {
	if (constant.IsNull()) {
		return false;
	}
	// GetValueUnsafe returns the raw internal value needed for scaled types like DECIMAL.
	// The stored int matches the narrow payload, not the logical integer.
	switch (constant.type().InternalType()) {
	case PhysicalType::BOOL:
		out = constant.GetValueUnsafe<bool>() ? 1 : 0;
		return true;
	case PhysicalType::INT8:
		out = constant.GetValueUnsafe<int8_t>();
		return true;
	case PhysicalType::INT16:
		out = constant.GetValueUnsafe<int16_t>();
		return true;
	case PhysicalType::INT32:
		out = constant.GetValueUnsafe<int32_t>();
		return true;
	case PhysicalType::INT64:
		out = constant.GetValueUnsafe<int64_t>();
		return true;
	case PhysicalType::UINT8:
		out = constant.GetValueUnsafe<uint8_t>();
		return true;
	case PhysicalType::UINT16:
		out = constant.GetValueUnsafe<uint16_t>();
		return true;
	case PhysicalType::UINT32:
		out = constant.GetValueUnsafe<uint32_t>();
		return true;
	case PhysicalType::UINT64: {
		auto u = constant.GetValueUnsafe<uint64_t>();
		if (u > uint64_t(INT64_MAX)) {
			return false;
		}
		out = int64_t(u);
		return true;
	}
	default:
		return false;
	}
}

// For a comparison `narrow_value <op> constant`, given that the narrow data lives in
// [lo, hi], decide whether the comparison short-circuits (always false / always true)
// and, if not, that the constant is representable in the narrow domain.
void ApplyRangeShortCircuit(ExpressionType op, int64_t constant, const IntRange &range, ForView &out) {
	if (constant < range.lo) {
		// All narrow values > constant
		switch (op) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			out.always_false = true;
			return;
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			out.always_true = true;
			return;
		default:
			break;
		}
	} else if (constant > range.hi) {
		// All narrow values < constant
		switch (op) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			out.always_false = true;
			return;
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			out.always_true = true;
			return;
		default:
			break;
		}
	}
}

} // namespace

bool TryResolveForView(const Vector &col, ExpressionType op, const Value &constant, ForView &out) {
	out = ForView();
	if (!IsSupportedComparison(op)) {
		return false;
	}
	int64_t constant_i64;
	if (!TryCastConstantToInt64(constant, constant_i64)) {
		return false;
	}

	auto vt = col.GetVectorType();
	if (vt == VectorType::FLAT_VECTOR) {
		auto pt = col.GetType().InternalType();
		if (!IsThinFlatType(pt)) {
			return false;
		}
		IntRange range;
		if (!TryGetNarrowRange(pt, range)) {
			return false;
		}
		ApplyRangeShortCircuit(op, constant_i64, range, out);
		out.kind = ForView::Kind::FLAT;
		out.narrow_type = pt;
		out.data = const_data_ptr_cast(FlatVector::GetData(col));
		out.rewritten_constant = constant_i64;
		auto &validity = FlatVector::Validity(col);
		out.original_validity = validity.CanHaveNull() ? &validity : nullptr;
		return true;
	}

	if (vt == VectorType::FOR_VECTOR) {
		auto stored = FORVector::GetStoredType(col);
		if (!IsThinForStored(stored)) {
			return false;
		}
		IntRange range;
		TryGetNarrowRange(stored, range);
		// FOR vectors are non-negative with logical max `for_max_value`; range is [0, max].
		// Use the tighter logical bound when it's smaller than the narrow-type upper bound.
		int64_t for_max;
		switch (stored) {
		case PhysicalType::UINT8:
			for_max = int64_t(FORVector::GetMax<uint8_t>(col));
			break;
		case PhysicalType::UINT16:
			for_max = int64_t(FORVector::GetMax<uint16_t>(col));
			break;
		case PhysicalType::UINT32:
			for_max = int64_t(FORVector::GetMax<uint32_t>(col));
			break;
		default:
			return false;
		}
		IntRange logical_range = {0, for_max};
		ApplyRangeShortCircuit(op, constant_i64, logical_range, out);
		out.kind = ForView::Kind::FOR;
		out.narrow_type = stored;
		out.data = const_data_ptr_cast(FORVector::GetData(col));
		out.rewritten_constant = constant_i64;
		auto &validity = FORVector::Validity(col);
		out.original_validity = validity.CanHaveNull() ? &validity : nullptr;
		return true;
	}

	return false;
}

namespace {

LogicalType NarrowLogicalType(PhysicalType pt) {
	switch (pt) {
	case PhysicalType::INT8:
		return LogicalType::TINYINT;
	case PhysicalType::INT16:
		return LogicalType::SMALLINT;
	case PhysicalType::INT32:
		return LogicalType::INTEGER;
	case PhysicalType::UINT8:
		return LogicalType::UTINYINT;
	case PhysicalType::UINT16:
		return LogicalType::USMALLINT;
	case PhysicalType::UINT32:
		return LogicalType::UINTEGER;
	default:
		throw InternalException("Unsupported narrow physical type in ForView");
	}
}

Value NarrowConstant(PhysicalType pt, int64_t constant) {
	switch (pt) {
	case PhysicalType::INT8:
		return Value::TINYINT(static_cast<int8_t>(constant));
	case PhysicalType::INT16:
		return Value::SMALLINT(static_cast<int16_t>(constant));
	case PhysicalType::INT32:
		return Value::INTEGER(static_cast<int32_t>(constant));
	case PhysicalType::UINT8:
		return Value::UTINYINT(static_cast<uint8_t>(constant));
	case PhysicalType::UINT16:
		return Value::USMALLINT(static_cast<uint16_t>(constant));
	case PhysicalType::UINT32:
		return Value::UINTEGER(static_cast<uint32_t>(constant));
	default:
		throw InternalException("Unsupported narrow physical type in ForView");
	}
}

void WriteConstantBool(Vector &bool_result, bool value, idx_t count) {
	bool_result.SetVectorType(VectorType::FLAT_VECTOR);
	auto data = FlatVector::GetDataMutable<bool>(bool_result);
	for (idx_t i = 0; i < count; i++) {
		data[i] = value;
	}
	FlatVector::ValidityMutable(bool_result).SetAllValid(count);
}

} // namespace

void EvaluateForComparison(const ForView &view, ExpressionType op, idx_t count, Vector &bool_result) {
	D_ASSERT(view.kind != ForView::Kind::NONE);
	if (view.always_false) {
		WriteConstantBool(bool_result, false, count);
		return;
	}
	if (view.always_true) {
		WriteConstantBool(bool_result, true, count);
		return;
	}

	// Build a FLAT all-valid view over the narrow buffer and a CONSTANT vector for the rewritten constant.
	auto narrow_logical = NarrowLogicalType(view.narrow_type);
	Vector narrow_view(narrow_logical, const_cast<data_ptr_t>(view.data), count);
	FlatVector::ValidityMutable(narrow_view).SetAllValid(count);
	// Constant vector sized to `count` so BinaryExecutor::CheckExecuteCount matches the narrow view;
	// it remains a CONSTANT_VECTOR, so the value is still broadcast (no per-row materialization).
	Vector narrow_const(NarrowConstant(view.narrow_type, view.rewritten_constant), count_t(count));
	bool_result.SetVectorType(VectorType::FLAT_VECTOR);

	switch (op) {
	case ExpressionType::COMPARE_EQUAL:
		VectorOperations::Equals(narrow_view, narrow_const, bool_result);
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		VectorOperations::NotEquals(narrow_view, narrow_const, bool_result);
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		VectorOperations::LessThan(narrow_view, narrow_const, bool_result);
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		VectorOperations::LessThanEquals(narrow_view, narrow_const, bool_result);
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		VectorOperations::GreaterThan(narrow_view, narrow_const, bool_result);
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		VectorOperations::GreaterThanEquals(narrow_view, narrow_const, bool_result);
		break;
	default:
		throw InternalException("Unsupported comparison op in EvaluateForComparison");
	}
}

bool TryResolveForViewFromExpr(const Expression &expr, DataChunk &input_chunk, ForView &out_view,
                               ExpressionType &out_op) {
	if (!BoundComparisonExpression::IsComparison(expr)) {
		return false;
	}
	auto &cmp = expr.Cast<BoundFunctionExpression>();
	auto &left = BoundComparisonExpression::Left(cmp);
	auto &right = BoundComparisonExpression::Right(cmp);

	const Expression *ref_expr = nullptr;
	const Expression *const_expr = nullptr;
	auto op = cmp.GetExpressionType();
	if (left.GetExpressionClass() == ExpressionClass::BOUND_REF &&
	    right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		ref_expr = &left;
		const_expr = &right;
	} else if (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
	           right.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		ref_expr = &right;
		const_expr = &left;
		op = FlipComparisonExpression(op);
	} else {
		return false;
	}

	auto ref_idx = ref_expr->Cast<BoundReferenceExpression>().Index();
	if (ref_idx >= input_chunk.ColumnCount()) {
		return false;
	}
	auto &col = input_chunk.data[ref_idx];
	auto &const_value = const_expr->Cast<BoundConstantExpression>().GetValue();
	if (const_value.IsNull()) {
		return false;
	}
	if (!TryResolveForView(col, op, const_value, out_view)) {
		return false;
	}
	out_op = op;
	return true;
}

} // namespace duckdb
