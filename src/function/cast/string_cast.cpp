#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

static BoundCastInfo VectorStringCastNumericSwitch(BindCastInput &input, const LogicalType &source,
                                                   const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::ENUM: {
		throw InternalException("string -> enum");
		//		switch (target.InternalType()) {
		//		case PhysicalType::UINT8:
		//			return TransformEnum<uint8_t>;
		//		case PhysicalType::UINT16:
		//			return TransformEnum<uint16_t>;
		//		case PhysicalType::UINT32:
		//			return TransformEnum<uint32_t>;
		//		default:
		//			throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
		//		}
	}
	case LogicalTypeId::BOOLEAN:
		return VectorTryCastStrictLoop<string_t, bool, duckdb::TryCast>;
	case LogicalTypeId::TINYINT:
		return VectorTryCastStrictLoop<string_t, int8_t, duckdb::TryCast>;
	case LogicalTypeId::SMALLINT:
		return VectorTryCastStrictLoop<string_t, int16_t, duckdb::TryCast>;
	case LogicalTypeId::INTEGER:
		return VectorTryCastStrictLoop<string_t, int32_t, duckdb::TryCast>;
	case LogicalTypeId::BIGINT:
		return VectorTryCastStrictLoop<string_t, int64_t, duckdb::TryCast>;
	case LogicalTypeId::UTINYINT:
		return VectorTryCastStrictLoop<string_t, uint8_t, duckdb::TryCast>;
	case LogicalTypeId::USMALLINT:
		return VectorTryCastStrictLoop<string_t, uint16_t, duckdb::TryCast>;
	case LogicalTypeId::UINTEGER:
		return VectorTryCastStrictLoop<string_t, uint32_t, duckdb::TryCast>;
	case LogicalTypeId::UBIGINT:
		return VectorTryCastStrictLoop<string_t, uint64_t, duckdb::TryCast>;
	case LogicalTypeId::HUGEINT:
		return VectorTryCastStrictLoop<string_t, hugeint_t, duckdb::TryCast>;
	case LogicalTypeId::FLOAT:
		return VectorTryCastStrictLoop<string_t, float, duckdb::TryCast>;
	case LogicalTypeId::DOUBLE:
		return VectorTryCastStrictLoop<string_t, double, duckdb::TryCast>;
	case LogicalTypeId::INTERVAL:
		return VectorTryCastErrorLoop<string_t, interval_t, duckdb::TryCastErrorMessage>;
	case LogicalTypeId::DECIMAL:
		throw InternalException("string -> decimal");
		//		return ToDecimalCast<string_t>;
	default:
		return DefaultCasts::TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::StringCastSwitch(BindCastInput &input, const LogicalType &source,
                                             const LogicalType &target) {
	// now switch on the target type
	switch (target.id()) {
	case LogicalTypeId::DATE:
		return VectorTryCastErrorLoop<string_t, date_t, duckdb::TryCastErrorMessage>;
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return VectorTryCastErrorLoop<string_t, dtime_t, duckdb::TryCastErrorMessage>;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return VectorTryCastErrorLoop<string_t, timestamp_t, duckdb::TryCastErrorMessage>;
	case LogicalTypeId::TIMESTAMP_NS:
		return VectorTryCastStrictLoop<string_t, timestamp_t, duckdb::TryCastToTimestampNS>;
	case LogicalTypeId::TIMESTAMP_SEC:
		return VectorTryCastStrictLoop<string_t, timestamp_t, duckdb::TryCastToTimestampSec>;
	case LogicalTypeId::TIMESTAMP_MS:
		return VectorTryCastStrictLoop<string_t, timestamp_t, duckdb::TryCastToTimestampMS>;
	case LogicalTypeId::BLOB:
		return VectorTryCastStringLoop<string_t, string_t, duckdb::TryCastToBlob>;
	case LogicalTypeId::UUID:
		return VectorTryCastStringLoop<string_t, hugeint_t, duckdb::TryCastToUUID>;
	case LogicalTypeId::SQLNULL:
		return DefaultCasts::TryVectorNullCast;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		return DefaultCasts::ReinterpretCast;
	default:
		return VectorStringCastNumericSwitch(input, source, target);
	}
}

} // namespace duckdb
