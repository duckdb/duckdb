#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/common/operator/numeric_cast.hpp"
#include "duckdb/common/types/bignum.hpp"

namespace duckdb {

namespace {
//! Same signedness, any int -> float, or unsigned -> strictly-wider signed.
template <class SRC, class DST>
constexpr bool IsOrderPreservingNumericCast() {
	if (NumericLimits<SRC>::IsSigned() == NumericLimits<DST>::IsSigned()) {
		return true;
	}
	if (std::is_floating_point<DST>::value) {
		return true;
	}
	if (!NumericLimits<SRC>::IsSigned() && NumericLimits<DST>::IsSigned()) {
		return sizeof(DST) > sizeof(SRC);
	}
	return false;
}

//! Distinct sources may collapse: DOUBLE->FLOAT, wide-int->float, float->int.
template <class SRC, class DST>
constexpr bool HasFloatPrecisionLoss() {
	if (std::is_same<SRC, double>::value && std::is_same<DST, float>::value) {
		return true;
	}
	if (!std::is_floating_point<SRC>::value && std::is_same<DST, float>::value) {
		return sizeof(SRC) > 2;
	}
	if (!std::is_floating_point<SRC>::value && std::is_same<DST, double>::value) {
		return sizeof(SRC) > 4;
	}
	if (std::is_floating_point<SRC>::value && !std::is_floating_point<DST>::value) {
		return true;
	}
	return false;
}

template <class SRC, class DST, class OP>
BoundCastInfo MakeNumericToNumericCast() {
	BoundCastInfo info(&VectorCastHelpers::TryCastLoop<SRC, DST, OP>);
	if (IsOrderPreservingNumericCast<SRC, DST>()) {
		auto props =
		    HasFloatPrecisionLoss<SRC, DST>() ? ArgProperties().NonDecreasing() : ArgProperties().StrictlyIncreasing();
		info.SetArgProperties(props);
	}
	return info;
}

template <class SRC>
BoundCastInfo MakeNumericToDecimalCast() {
	BoundCastInfo info(&VectorCastHelpers::ToDecimalCast<SRC>);
	auto props =
	    std::is_floating_point<SRC>::value ? ArgProperties().NonDecreasing() : ArgProperties().StrictlyIncreasing();
	info.SetArgProperties(props);
	return info;
}
} // namespace

template <class SRC>
static BoundCastInfo InternalNumericCastSwitch(const LogicalType &source, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::BOOLEAN:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<SRC, bool, duckdb::NumericTryCast>);
	case LogicalTypeId::TINYINT:
		return MakeNumericToNumericCast<SRC, int8_t, duckdb::NumericTryCast>();
	case LogicalTypeId::SMALLINT:
		return MakeNumericToNumericCast<SRC, int16_t, duckdb::NumericTryCast>();
	case LogicalTypeId::INTEGER:
		return MakeNumericToNumericCast<SRC, int32_t, duckdb::NumericTryCast>();
	case LogicalTypeId::BIGINT:
		return MakeNumericToNumericCast<SRC, int64_t, duckdb::NumericTryCast>();
	case LogicalTypeId::UTINYINT:
		return MakeNumericToNumericCast<SRC, uint8_t, duckdb::NumericTryCast>();
	case LogicalTypeId::USMALLINT:
		return MakeNumericToNumericCast<SRC, uint16_t, duckdb::NumericTryCast>();
	case LogicalTypeId::UINTEGER:
		return MakeNumericToNumericCast<SRC, uint32_t, duckdb::NumericTryCast>();
	case LogicalTypeId::UBIGINT:
		return MakeNumericToNumericCast<SRC, uint64_t, duckdb::NumericTryCast>();
	case LogicalTypeId::HUGEINT:
		return MakeNumericToNumericCast<SRC, hugeint_t, duckdb::NumericTryCast>();
	case LogicalTypeId::UHUGEINT:
		return MakeNumericToNumericCast<SRC, uhugeint_t, duckdb::NumericTryCast>();
	case LogicalTypeId::FLOAT:
		return MakeNumericToNumericCast<SRC, float, duckdb::NumericTryCast>();
	case LogicalTypeId::DOUBLE:
		return MakeNumericToNumericCast<SRC, double, duckdb::NumericTryCast>();
	case LogicalTypeId::DECIMAL:
		return MakeNumericToDecimalCast<SRC>();
	case LogicalTypeId::VARCHAR:
		return BoundCastInfo(&VectorCastHelpers::StringCast<SRC, duckdb::StringCast>);
	case LogicalTypeId::BIT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<SRC, duckdb::NumericTryCastToBit>);
	case LogicalTypeId::BIGNUM:
		return Bignum::NumericToBignumCastSwitch(source);
	case LogicalTypeId::UUID:
		if (source.id() == LogicalTypeId::UHUGEINT) {
			return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<SRC, hugeint_t, duckdb::CastFromUHugeintToUUID>);
		}
		return DefaultCasts::TryVectorNullCast;
	default:
		return DefaultCasts::TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::NumericCastSwitch(BindCastInput &input, const LogicalType &source,
                                              const LogicalType &target) {
	switch (source.id()) {
	case LogicalTypeId::BOOLEAN:
		return InternalNumericCastSwitch<bool>(source, target);
	case LogicalTypeId::TINYINT:
		return InternalNumericCastSwitch<int8_t>(source, target);
	case LogicalTypeId::SMALLINT:
		return InternalNumericCastSwitch<int16_t>(source, target);
	case LogicalTypeId::INTEGER:
		return InternalNumericCastSwitch<int32_t>(source, target);
	case LogicalTypeId::BIGINT:
		return InternalNumericCastSwitch<int64_t>(source, target);
	case LogicalTypeId::UTINYINT:
		return InternalNumericCastSwitch<uint8_t>(source, target);
	case LogicalTypeId::USMALLINT:
		return InternalNumericCastSwitch<uint16_t>(source, target);
	case LogicalTypeId::UINTEGER:
		return InternalNumericCastSwitch<uint32_t>(source, target);
	case LogicalTypeId::UBIGINT:
		return InternalNumericCastSwitch<uint64_t>(source, target);
	case LogicalTypeId::HUGEINT:
		return InternalNumericCastSwitch<hugeint_t>(source, target);
	case LogicalTypeId::UHUGEINT:
		return InternalNumericCastSwitch<uhugeint_t>(source, target);
	case LogicalTypeId::FLOAT:
		return InternalNumericCastSwitch<float>(source, target);
	case LogicalTypeId::DOUBLE:
		return InternalNumericCastSwitch<double>(source, target);
	default:
		throw InternalException("NumericCastSwitch called with non-numeric argument");
	}
}

} // namespace duckdb
