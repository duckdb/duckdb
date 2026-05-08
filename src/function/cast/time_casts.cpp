#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/common/operator/string_cast.hpp"
namespace duckdb {

BoundCastInfo DefaultCasts::DateCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// date to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<date_t, duckdb::StringCast>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		// date to timestamp
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<date_t, timestamp_t, duckdb::TryCast>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<date_t, timestamp_ns_t, duckdb::TryCastToTimestampNS>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_SEC:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<date_t, timestamp_t, duckdb::TryCastToTimestampSec>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_MS:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<date_t, timestamp_t, duckdb::TryCastToTimestampMS>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimeCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// time to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<dtime_t, duckdb::StringCast>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIME_TZ:
		// default packs offset 0; ICU's override is unannotated and the propagator bails (see #22259)
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<dtime_t, dtime_tz_t, duckdb::Cast>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimeNsCastSwitch(BindCastInput &input, const LogicalType &src, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// time (ns) to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<dtime_ns_t, duckdb::StringCast>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIME:
		// time (ns) to time (µs)
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<dtime_ns_t, dtime_t, duckdb::Cast>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimeTzCastSwitch(BindCastInput &input, const LogicalType &source,
                                             const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// time with time zone to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<dtime_tz_t, duckdb::StringCastTZ>);
	case LogicalTypeId::TIME:
		// time with time zone to time
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<dtime_tz_t, dtime_t, duckdb::Cast>);
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimestampCastSwitch(BindCastInput &input, const LogicalType &source,
                                                const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// timestamp to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<timestamp_t, duckdb::StringCast>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::DATE:
		// timestamp to date
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, date_t, duckdb::Cast>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	case LogicalTypeId::TIME:
		// timestamp to time — UNKNOWN: drops the date
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_t, duckdb::Cast>);
	case LogicalTypeId::TIME_TZ:
		// timestamp to time_tz — UNKNOWN: drops the date
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_tz_t, duckdb::Cast>);
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp (us) to timestamp with time zone
		return BoundCastInfo(ReinterpretCast).SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		// timestamp (us) to timestamp [with time zone] (ns)
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampUsToNs>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_MS:
		// timestamp (us) to timestamp (ms)
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampUsToMs>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	case LogicalTypeId::TIMESTAMP_SEC:
		// timestamp (us) to timestamp (s)
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampUsToSec>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimestampTzCastSwitch(BindCastInput &input, const LogicalType &source,
                                                  const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// timestamp with time zone to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<timestamp_t, duckdb::StringCastTZ>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIME_TZ:
		// timestamp with time zone to time with time zone — UNKNOWN: drops the date
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_tz_t, duckdb::Cast>);
	case LogicalTypeId::TIMESTAMP:
		// timestamp with time zone to timestamp (us)
		return BoundCastInfo(ReinterpretCast).SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_NS:
		// timestamptz (us) to timestamp (ns)
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampUsToNs>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_MS:
		// timestamptz (us) to timestamp (ms)
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampUsToMs>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	case LogicalTypeId::TIMESTAMP_SEC:
		// timestamptz (us) to timestamp (s)
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampUsToSec>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimestampTzNsCastSwitch(BindCastInput &input, const LogicalType &source,
                                                    const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// timestamp(ns) with time zone to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<timestamp_ns_t, duckdb::StringCastTZ>);
	case LogicalTypeId::TIME_TZ:
		// timestamp with time zone to time with time zone.
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_ns_t, dtime_tz_t, duckdb::Cast>);
	case LogicalTypeId::TIMESTAMP_NS:
		// timestamp with time zone to timestamp (us)
		return ReinterpretCast;
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp with time zone (ns) to timestamp with time zone (us)
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampNsToUs>);
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimestampNsCastSwitch(BindCastInput &input, const LogicalType &source,
                                                  const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		return BoundCastInfo(&VectorCastHelpers::StringCast<timestamp_ns_t, duckdb::StringCast>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::DATE:
		// timestamp (ns) to date
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, date_t, duckdb::CastTimestampNsToDate>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	case LogicalTypeId::TIME:
		// timestamp (ns) to time — UNKNOWN: drops the date
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_t, duckdb::CastTimestampNsToTime>);
	case LogicalTypeId::TIME_NS:
		// timestamp (ns) to time (ns) — UNKNOWN: drops the date
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_ns_t, dtime_ns_t, duckdb::CastTimestampNsToTimeNs>);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp (ns) to timestamp [with time zone] (us)
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampNsToUs>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimestampMsCastSwitch(BindCastInput &input, const LogicalType &source,
                                                  const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		return BoundCastInfo(&VectorCastHelpers::StringCast<timestamp_t, duckdb::CastFromTimestampMS>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::DATE:
		// timestamp (ms) to date
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, date_t, duckdb::CastTimestampMsToDate>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	case LogicalTypeId::TIME:
		// timestamp (ms) to time — UNKNOWN: drops the date
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_t, duckdb::CastTimestampMsToTime>);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp (ms) to timestamp [with time zone] (us)
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampMsToUs>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		// timestamp (ms) to timestamp [with time zone] (ns)
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampMsToNs>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::TimestampSecCastSwitch(BindCastInput &input, const LogicalType &source,
                                                   const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		return BoundCastInfo(&VectorCastHelpers::StringCast<timestamp_t, duckdb::CastFromTimestampSec>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::DATE:
		// timestamp (s) to date
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, date_t, duckdb::CastTimestampSecToDate>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	case LogicalTypeId::TIME:
		// timestamp (s) to time — UNKNOWN: drops the date
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_t, duckdb::CastTimestampSecToTime>);
	case LogicalTypeId::TIMESTAMP_MS:
		// timestamp (s) to timestamp (ms)
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampSecToMs>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp (s) to timestamp [with time zone] (us)
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampSecToUs>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		// timestamp (s) to timestamp [with time zone] (ns)
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampSecToNs>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	default:
		return TryVectorNullCast;
	}
}
BoundCastInfo DefaultCasts::IntervalCastSwitch(BindCastInput &input, const LogicalType &source,
                                               const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		return BoundCastInfo(&VectorCastHelpers::StringCast<interval_t, duckdb::StringCast>);
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
