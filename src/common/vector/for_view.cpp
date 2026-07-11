#include "duckdb/common/vector/for_view.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/for_vector.hpp"
#include "duckdb/common/vector_operations/comparison_bitmap.hpp"

namespace duckdb {

namespace {

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
	if (!BitmapCmpOpSupported(op)) {
		return false;
	}
	int64_t constant_i64;
	if (!TryCastConstantToInt64(constant, constant_i64)) {
		return false;
	}

	auto vt = col.GetVectorType();
	if (vt == VectorType::FLAT_VECTOR) {
		auto pt = col.GetType().InternalType();
		if (!BitmapThinIntegralTypeSupported(pt)) {
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
		if (!FORVector::IsThinStoredType(stored)) {
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

} // namespace duckdb
