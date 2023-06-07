#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

BoundCastInfo DefaultCasts::BitCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	// Numerics
	case LogicalTypeId::BOOLEAN:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<string_t, bool, duckdb::CastFromBitToNumeric>);
	case LogicalTypeId::TINYINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<string_t, int8_t, duckdb::CastFromBitToNumeric>);
	case LogicalTypeId::SMALLINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<string_t, int16_t, duckdb::CastFromBitToNumeric>);
	case LogicalTypeId::INTEGER:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<string_t, int32_t, duckdb::CastFromBitToNumeric>);
	case LogicalTypeId::BIGINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<string_t, int64_t, duckdb::CastFromBitToNumeric>);
	case LogicalTypeId::UTINYINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<string_t, uint8_t, duckdb::CastFromBitToNumeric>);
	case LogicalTypeId::USMALLINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<string_t, uint16_t, duckdb::CastFromBitToNumeric>);
	case LogicalTypeId::UINTEGER:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<string_t, uint32_t, duckdb::CastFromBitToNumeric>);
	case LogicalTypeId::UBIGINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<string_t, uint64_t, duckdb::CastFromBitToNumeric>);
	case LogicalTypeId::HUGEINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<string_t, hugeint_t, duckdb::CastFromBitToNumeric>);
	case LogicalTypeId::FLOAT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<string_t, float, duckdb::CastFromBitToNumeric>);
	case LogicalTypeId::DOUBLE:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<string_t, double, duckdb::CastFromBitToNumeric>);

	case LogicalTypeId::VARCHAR:
		return BoundCastInfo(&VectorCastHelpers::StringCast<string_t, duckdb::CastFromBitToString>);

	default:
		return DefaultCasts::TryVectorNullCast;
	}
}

} // namespace duckdb
