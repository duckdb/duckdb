#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

BindCastInfo::~BindCastInfo() {}

BoundCastData::~BoundCastData() {}

BoundCastInfo::BoundCastInfo(cast_function_t function_p, unique_ptr<BoundCastData> cast_data_p) :
    function(function_p), cast_data(move(cast_data_p)) {}

BoundCastInfo BoundCastInfo::Copy() const {
	return BoundCastInfo(function, cast_data->Copy());
}

bool DefaultCasts::NopCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	result.Reference(source);
}

// NULL cast only works if all values in source are NULL, otherwise an unimplemented cast exception is thrown
bool DefaultCasts::TryVectorNullCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	bool success = true;
	if (VectorOperations::HasNotNull(source, count)) {
		HandleCastError::AssignError(UnimplementedCastMessage(source.GetType(), result.GetType()), parameters.error_message);
		success = false;
	}
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(result, true);
	return success;
}

static cast_function_t VectorStringCastNumericSwitch(Vector &source, Vector &result, idx_t count, bool strict,
                                          string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::ENUM: {
		switch (result.GetType().InternalType()) {
		case PhysicalType::UINT8:
			return TransformEnum<uint8_t>(source, result, count, error_message);
		case PhysicalType::UINT16:
			return TransformEnum<uint16_t>(source, result, count, error_message);
		case PhysicalType::UINT32:
			return TransformEnum<uint32_t>(source, result, count, error_message);
		default:
			throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
		}
	}
	case LogicalTypeId::BOOLEAN:
		return VectorTryCastStrictLoop<string_t, bool, duckdb::TryCast>(source, result, count, strict, error_message);
	case LogicalTypeId::TINYINT:
		return VectorTryCastStrictLoop<string_t, int8_t, duckdb::TryCast>(source, result, count, strict, error_message);
	case LogicalTypeId::SMALLINT:
		return VectorTryCastStrictLoop<string_t, int16_t, duckdb::TryCast>(source, result, count, strict,
		                                                                   error_message);
	case LogicalTypeId::INTEGER:
		return VectorTryCastStrictLoop<string_t, int32_t, duckdb::TryCast>(source, result, count, strict,
		                                                                   error_message);
	case LogicalTypeId::BIGINT:
		return VectorTryCastStrictLoop<string_t, int64_t, duckdb::TryCast>(source, result, count, strict,
		                                                                   error_message);
	case LogicalTypeId::UTINYINT:
		return VectorTryCastStrictLoop<string_t, uint8_t, duckdb::TryCast>(source, result, count, strict,
		                                                                   error_message);
	case LogicalTypeId::USMALLINT:
		return VectorTryCastStrictLoop<string_t, uint16_t, duckdb::TryCast>(source, result, count, strict,
		                                                                    error_message);
	case LogicalTypeId::UINTEGER:
		return VectorTryCastStrictLoop<string_t, uint32_t, duckdb::TryCast>(source, result, count, strict,
		                                                                    error_message);
	case LogicalTypeId::UBIGINT:
		return VectorTryCastStrictLoop<string_t, uint64_t, duckdb::TryCast>(source, result, count, strict,
		                                                                    error_message);
	case LogicalTypeId::HUGEINT:
		return VectorTryCastStrictLoop<string_t, hugeint_t, duckdb::TryCast>(source, result, count, strict,
		                                                                     error_message);
	case LogicalTypeId::FLOAT:
		return VectorTryCastStrictLoop<string_t, float, duckdb::TryCast>(source, result, count, strict, error_message);
	case LogicalTypeId::DOUBLE:
		return VectorTryCastStrictLoop<string_t, double, duckdb::TryCast>(source, result, count, strict, error_message);
	case LogicalTypeId::INTERVAL:
		return VectorTryCastErrorLoop<string_t, interval_t, duckdb::TryCastErrorMessage>(source, result, count, strict,
		                                                                                 error_message);
	case LogicalTypeId::DECIMAL:
		return ToDecimalCast<string_t>(source, result, count, error_message);
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static cast_function_t StringCastSwitch(Vector &source, Vector &result, idx_t count, bool strict, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::DATE:
		return VectorTryCastErrorLoop<string_t, date_t, duckdb::TryCastErrorMessage>(source, result, count, strict,
		                                                                             error_message);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return VectorTryCastErrorLoop<string_t, dtime_t, duckdb::TryCastErrorMessage>(source, result, count, strict,
		                                                                              error_message);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return VectorTryCastErrorLoop<string_t, timestamp_t, duckdb::TryCastErrorMessage>(source, result, count, strict,
		                                                                                  error_message);
	case LogicalTypeId::TIMESTAMP_NS:
		return VectorTryCastStrictLoop<string_t, timestamp_t, duckdb::TryCastToTimestampNS>(source, result, count,
		                                                                                    strict, error_message);
	case LogicalTypeId::TIMESTAMP_SEC:
		return VectorTryCastStrictLoop<string_t, timestamp_t, duckdb::TryCastToTimestampSec>(source, result, count,
		                                                                                     strict, error_message);
	case LogicalTypeId::TIMESTAMP_MS:
		return VectorTryCastStrictLoop<string_t, timestamp_t, duckdb::TryCastToTimestampMS>(source, result, count,
		                                                                                    strict, error_message);
	case LogicalTypeId::BLOB:
		return VectorTryCastStringLoop<string_t, string_t, duckdb::TryCastToBlob>(source, result, count, strict,
		                                                                          error_message);
	case LogicalTypeId::UUID:
		return VectorTryCastStringLoop<string_t, hugeint_t, duckdb::TryCastToUUID>(source, result, count, strict,
		                                                                           error_message);
	case LogicalTypeId::SQLNULL:
		return TryVectorNullCast(source, result, count, error_message);
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		result.Reinterpret(source);
		return true;
	default:
		return VectorStringCastNumericSwitch(source, result, count, strict, error_message);
	}
}

static cast_function_t BlobCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// blob to varchar
		VectorStringCast<string_t, duckdb::CastFromBlob>(source, result, count);
		break;
	case LogicalTypeId::AGGREGATE_STATE:
		result.Reinterpret(source);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static bool AggregateStateToBlobCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	if (result.GetType().id() != LogicalTypeId::BLOB) {
		throw TypeMismatchException(source.GetType(), result.GetType(),
		                            "Cannot cast AGGREGATE_STATE to anything but BLOB");
	}
	result.Reinterpret(source);
	return true;
}

static bool NullTypeCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	// cast a NULL to another type, just copy the properties and change the type
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(result, true);
	return true;
}

BoundCastInfo DefaultCasts::GetDefaultCastFunction(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	D_ASSERT(source != target);
	// first switch on source type
	switch (source.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return NumericCastSwitch(input, source, target);
	case LogicalTypeId::UUID:
		return UUIDCastSwitch(input, source, target);
	case LogicalTypeId::DECIMAL:
		return DecimalCastSwitch(source, result, count, error_message);
	case LogicalTypeId::DATE:
		return DateCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIME:
		return TimeCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIME_TZ:
		return TimeTzCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP:
		return TimestampCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_TZ:
		return TimestampTzCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_NS:
		return TimestampNsCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_MS:
		return TimestampMsCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_SEC:
		return TimestampSecCastSwitch(source, result, count, error_message);
	case LogicalTypeId::INTERVAL:
		return IntervalCastSwitch(source, result, count, error_message);
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR:
		return StringCastSwitch(source, result, count, strict, error_message);
	case LogicalTypeId::BLOB:
		return BlobCastSwitch(source, result, count, error_message);
	case LogicalTypeId::SQLNULL:
		return NullTypeCast;
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT:
		return StructCastSwitch(source, result, count, error_message);
	case LogicalTypeId::LIST:
		return ListCastSwitch(source, result, count, error_message);
	case LogicalTypeId::ENUM:
		return EnumCastSwitch(source, result, count, error_message, strict);
	case LogicalTypeId::AGGREGATE_STATE:
		return AggregateStateToBlobCast(source, result, count, error_message, strict);
	default:
		return nullptr;
	}
}

}
