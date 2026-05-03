#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/common/operator/string_cast.hpp"
namespace duckdb {

BoundCastInfo DefaultCasts::DateCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// date to varchar: ISO 8601 zero-padded; lexicographic order matches date ordering.
		return BoundCastInfo(&VectorCastHelpers::StringCast<date_t, duckdb::StringCast>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		// date to timestamp: widening, distinct dates -> distinct timestamps at midnight.
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<date_t, timestamp_t, duckdb::TryCast>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_NS:
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
		// time to varchar: HH:MM:SS[.uuuuuu] zero-padded; lex order matches micros order.
		return BoundCastInfo(&VectorCastHelpers::StringCast<dtime_t, duckdb::StringCast>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIME_TZ:
		// time to time with time zone: depends on session zone semantics — leave UNKNOWN.
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<dtime_t, dtime_tz_t, duckdb::Cast>);
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
		// time (ns) to time (µs): truncation collapses 1000 ns values per us.
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
		// timestamp to date: truncation; many timestamps in same day collapse.
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, date_t, duckdb::Cast>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	case LogicalTypeId::TIME:
		// timestamp to time: drops the date — distinct timestamps on different days share times.
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_t, duckdb::Cast>);
	case LogicalTypeId::TIME_TZ:
		// timestamp to time_tz: drops the date.
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_tz_t, duckdb::Cast>);
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp (us) to timestamp with time zone: same physical representation.
		return BoundCastInfo(ReinterpretCast).SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_NS:
		// timestamp (us) to timestamp (ns): widening.
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampUsToNs>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_MS:
		// timestamp (us) to timestamp (ms): truncation.
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampUsToMs>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	case LogicalTypeId::TIMESTAMP_SEC:
		// timestamp (us) to timestamp (s): truncation.
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
		// timestamp with time zone to varchar: rendered in session zone, lex order matches instant.
		return BoundCastInfo(&VectorCastHelpers::StringCast<timestamp_t, duckdb::StringCastTZ>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIME_TZ:
		// timestamp with time zone to time with time zone: drops the date.
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_tz_t, duckdb::Cast>);
	case LogicalTypeId::TIMESTAMP:
		// timestamp with time zone to timestamp (us): identity reinterpret.
		return BoundCastInfo(ReinterpretCast).SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_NS:
		// timestamptz (us) to timestamp (ns): widening.
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampUsToNs>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_MS:
		// timestamptz (us) to timestamp (ms): truncation.
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampUsToMs>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	case LogicalTypeId::TIMESTAMP_SEC:
		// timestamptz (us) to timestamp (s): truncation.
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampUsToSec>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
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
		// timestamp (ns) to date: truncation.
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, date_t, duckdb::CastTimestampNsToDate>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	case LogicalTypeId::TIME:
		// timestamp (ns) to time: drops the date.
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_t, duckdb::CastTimestampNsToTime>);
	case LogicalTypeId::TIME_NS:
		// timestamp (ns) to time (ns): drops the date.
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_ns_t, dtime_ns_t, duckdb::CastTimestampNsToTimeNs>);
	case LogicalTypeId::TIMESTAMP:
		// timestamp (ns) to timestamp (us): truncation.
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampNsToUs>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp (ns) to timestamp with time zone (us): truncation.
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
		// timestamp (ms) to date: truncation.
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, date_t, duckdb::CastTimestampMsToDate>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	case LogicalTypeId::TIME:
		// timestamp (ms) to time: drops the date.
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_t, duckdb::CastTimestampMsToTime>);
	case LogicalTypeId::TIMESTAMP:
		// timestamp (ms) to timestamp (us): widening.
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampMsToUs>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_NS:
		// timestamp (ms) to timestamp (ns): widening.
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampMsToNs>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp (ms) to timestamp with timezone (us): widening.
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampMsToUs>)
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
		// timestamp (s) to date: truncation.
		return BoundCastInfo(&VectorCastHelpers::TemplatedCastLoop<timestamp_t, date_t, duckdb::CastTimestampSecToDate>)
		    .SetArgProperties(ArgProperties().NonDecreasing());
	case LogicalTypeId::TIME:
		// timestamp (s) to time: drops the date.
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<timestamp_t, dtime_t, duckdb::CastTimestampSecToTime>);
	case LogicalTypeId::TIMESTAMP_MS:
		// timestamp (s) to timestamp (ms): widening.
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampSecToMs>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP:
		// timestamp (s) to timestamp (us): widening.
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampSecToUs>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp (s) to timestamp with timezone (us): widening.
		return BoundCastInfo(
		           &VectorCastHelpers::TemplatedCastLoop<timestamp_t, timestamp_t, duckdb::CastTimestampSecToUs>)
		    .SetArgProperties(ArgProperties().StrictlyIncreasing());
	case LogicalTypeId::TIMESTAMP_NS:
		// timestamp (s) to timestamp (ns): widening.
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
