#include "duckdb/common/vector/for_view.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/for_vector.hpp"
#include "duckdb/common/vector_operations/comparison_bitmap.hpp"

namespace duckdb {

namespace {

bool TryCastConstantToInt64(const Value &constant, int64_t &out) {
	if (constant.IsNull()) {
		return false;
	}
	// Use raw internal values so DECIMAL constants match the narrow payload.
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
	case PhysicalType::INT128: {
		auto h = constant.GetValueUnsafe<hugeint_t>();
		if (h < Hugeint::Convert(INT64_MIN) || h > Hugeint::Convert(INT64_MAX)) {
			return false;
		}
		out = Hugeint::Cast<int64_t>(h);
		return true;
	}
	case PhysicalType::UINT128: {
		auto u = constant.GetValueUnsafe<uhugeint_t>();
		if (u.upper != 0 || u.lower > uint64_t(INT64_MAX)) {
			return false;
		}
		out = int64_t(u.lower);
		return true;
	}
	default:
		return false;
	}
}

void ApplyRangeShortCircuit(ExpressionType op, int64_t constant, int64_t lo, int64_t hi, ForView &out) {
	if (constant >= lo && constant <= hi) {
		return;
	}
	const bool below = constant < lo;
	auto set_result = [&](bool value) {
		out.always_true = value;
		out.always_false = !value;
	};
	switch (op) {
	case ExpressionType::COMPARE_EQUAL:
		return set_result(false);
	case ExpressionType::COMPARE_NOTEQUAL:
		return set_result(true);
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return set_result(!below);
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return set_result(below);
	default:
		return;
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

	if (col.GetVectorType() != VectorType::FOR_VECTOR) {
		return false;
	}
	auto stored = FORVector::GetStoredType(col);
	if (!FORVector::IsThinStoredType(stored)) {
		return false;
	}
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
	case PhysicalType::UINT64: {
		auto max_value = FORVector::GetMax<uint64_t>(col);
		if (max_value > uint64_t(INT64_MAX)) {
			return false;
		}
		for_max = int64_t(max_value);
		break;
	}
	default:
		return false;
	}
	ApplyRangeShortCircuit(op, constant_i64, 0, for_max, out);
	out.narrow_type = stored;
	out.data = const_data_ptr_cast(FORVector::GetData(col));
	out.rewritten_constant = constant_i64;
	auto &validity = FORVector::Validity(col);
	out.original_validity = validity.CanHaveNull() ? &validity : nullptr;
	return true;
}

} // namespace duckdb
