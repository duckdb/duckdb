#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/common/operator/string_cast.hpp"
namespace duckdb {

BoundCastInfo DefaultCasts::DateCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// date to varchar
		return VectorStringCast<date_t, duckdb::StringCast>;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		// date to timestamp
		return VectorTryCastLoop<date_t, timestamp_t, duckdb::TryCast>;
	case LogicalTypeId::TIMESTAMP_NS:
		return VectorTryCastLoop<date_t, timestamp_t, duckdb::TryCastToTimestampNS>;
	case LogicalTypeId::TIMESTAMP_SEC:
		return VectorTryCastLoop<date_t, timestamp_t, duckdb::TryCastToTimestampSec>;
	case LogicalTypeId::TIMESTAMP_MS:
		return VectorTryCastLoop<date_t, timestamp_t, duckdb::TryCastToTimestampMS>;
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimeCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// time to varchar
		return VectorStringCast<dtime_t, duckdb::StringCast>;
	case LogicalTypeId::TIME_TZ:
		// time to time with time zone
		return ReinterpretCast;
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimeTzCastSwitch(BindCastInput &input, const LogicalType &source,
                                             const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// time with time zone to varchar
		return VectorStringCast<dtime_t, duckdb::StringCastTZ>;
	case LogicalTypeId::TIME:
		// time with time zone to time
		return ReinterpretCast;
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimestampCastSwitch(BindCastInput &input, const LogicalType &source,
                                                const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// timestamp to varchar
		return VectorStringCast<timestamp_t, duckdb::StringCast>;
	case LogicalTypeId::DATE:
		// timestamp to date
		return TemplatedVectorCastLoop<timestamp_t, date_t, duckdb::Cast>;
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		// timestamp to time
		return TemplatedVectorCastLoop<timestamp_t, dtime_t, duckdb::Cast>;
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp (us) to timestamp with time zone
		return TemplatedVectorCastLoop<timestamp_t, timestamp_t, duckdb::Cast>;
	case LogicalTypeId::TIMESTAMP_NS:
		// timestamp (us) to timestamp (ns)
		return TemplatedVectorCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampUsToNs>;
	case LogicalTypeId::TIMESTAMP_MS:
		// timestamp (us) to timestamp (ms)
		return TemplatedVectorCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampUsToMs>;
	case LogicalTypeId::TIMESTAMP_SEC:
		// timestamp (us) to timestamp (s)
		return TemplatedVectorCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampUsToSec>;
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimestampTzCastSwitch(BindCastInput &input, const LogicalType &source,
                                                  const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// timestamp with time zone to varchar
		return VectorStringCast<timestamp_t, duckdb::StringCastTZ>;
	case LogicalTypeId::TIME_TZ:
		// timestamp with time zone to time with time zone.
		// TODO: set the offset to +00
		return TemplatedVectorCastLoop<timestamp_t, dtime_t, duckdb::Cast>;
	case LogicalTypeId::TIMESTAMP:
		// timestamp with time zone to timestamp (us)
		return TemplatedVectorCastLoop<timestamp_t, timestamp_t, duckdb::Cast>;
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimestampNsCastSwitch(BindCastInput &input, const LogicalType &source,
                                                  const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// timestamp (ns) to varchar
		return VectorStringCast<timestamp_t, duckdb::CastFromTimestampNS>;
	case LogicalTypeId::TIMESTAMP:
		// timestamp (ns) to timestamp (us)
		return TemplatedVectorCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampNsToUs>;
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimestampMsCastSwitch(BindCastInput &input, const LogicalType &source,
                                                  const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// timestamp (ms) to varchar
		return VectorStringCast<timestamp_t, duckdb::CastFromTimestampMS>;
	case LogicalTypeId::TIMESTAMP:
		// timestamp (ms) to timestamp (us)
		return TemplatedVectorCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampMsToUs>;
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimestampSecCastSwitch(BindCastInput &input, const LogicalType &source,
                                                   const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// timestamp (sec) to varchar
		return VectorStringCast<timestamp_t, duckdb::CastFromTimestampSec>;
	case LogicalTypeId::TIMESTAMP:
		// timestamp (s) to timestamp (us)
		return TemplatedVectorCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampSecToUs>;
	default:
		return TryVectorNullCast;
	}
}
BoundCastInfo DefaultCasts::IntervalCastSwitch(BindCastInput &input, const LogicalType &source,
                                               const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// time to varchar
		return VectorStringCast<interval_t, duckdb::StringCast>;
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
