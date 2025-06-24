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
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, bool, CastFromBitToNumeric>);
	case LogicalTypeId::TINYINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, int8_t, CastFromBitToNumeric>);
	case LogicalTypeId::SMALLINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, int16_t, CastFromBitToNumeric>);
	case LogicalTypeId::INTEGER:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, int32_t, CastFromBitToNumeric>);
	case LogicalTypeId::BIGINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, int64_t, CastFromBitToNumeric>);
	case LogicalTypeId::UTINYINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, uint8_t, CastFromBitToNumeric>);
	case LogicalTypeId::USMALLINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, uint16_t, CastFromBitToNumeric>);
	case LogicalTypeId::UINTEGER:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, uint32_t, CastFromBitToNumeric>);
	case LogicalTypeId::UBIGINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, uint64_t, CastFromBitToNumeric>);
	case LogicalTypeId::HUGEINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, hugeint_t, CastFromBitToNumeric>);
	case LogicalTypeId::UHUGEINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, uhugeint_t, CastFromBitToNumeric>);
	case LogicalTypeId::FLOAT:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, float, CastFromBitToNumeric>);
	case LogicalTypeId::DOUBLE:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, double, CastFromBitToNumeric>);

	case LogicalTypeId::BLOB:
		return BoundCastInfo(&VectorCastHelpers::StringCast<string_t, CastFromBitToBlob>);

	case LogicalTypeId::VARCHAR:
		return BoundCastInfo(&VectorCastHelpers::StringCast<string_t, CastFromBitToString>);

	default:
		return DefaultCasts::TryVectorNullCast;
	}
}

} // namespace duckdb
