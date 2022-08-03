#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

static cast_function_t DateCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// date to varchar
		VectorStringCast<date_t, duckdb::StringCast>(source, result, count);
		return true;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		// date to timestamp
		return VectorTryCastLoop<date_t, timestamp_t, duckdb::TryCast>(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_NS:
		return VectorTryCastLoop<date_t, timestamp_t, duckdb::TryCastToTimestampNS>(source, result, count,
		                                                                            error_message);
	case LogicalTypeId::TIMESTAMP_SEC:
		return VectorTryCastLoop<date_t, timestamp_t, duckdb::TryCastToTimestampSec>(source, result, count,
		                                                                             error_message);
	case LogicalTypeId::TIMESTAMP_MS:
		return VectorTryCastLoop<date_t, timestamp_t, duckdb::TryCastToTimestampMS>(source, result, count,
		                                                                            error_message);
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static cast_function_t TimeCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// time to varchar
		VectorStringCast<dtime_t, duckdb::StringCast>(source, result, count);
		return true;
	case LogicalTypeId::TIME_TZ:
		// time to time with time zone
		UnaryExecutor::Execute<dtime_t, dtime_t, duckdb::Cast>(source, result, count);
		return true;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static cast_function_t TimeTzCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// time with time zone to varchar
		VectorStringCast<dtime_t, duckdb::StringCastTZ>(source, result, count);
		return true;
	case LogicalTypeId::TIME:
		// time with time zone to time
		UnaryExecutor::Execute<dtime_t, dtime_t, duckdb::Cast>(source, result, count);
		return true;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static cast_function_t TimestampCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// timestamp to varchar
		VectorStringCast<timestamp_t, duckdb::StringCast>(source, result, count);
		break;
	case LogicalTypeId::DATE:
		// timestamp to date
		UnaryExecutor::Execute<timestamp_t, date_t, duckdb::Cast>(source, result, count);
		break;
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		// timestamp to time
		UnaryExecutor::Execute<timestamp_t, dtime_t, duckdb::Cast>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp (us) to timestamp with time zone
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::Cast>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP_NS:
		// timestamp (us) to timestamp (ns)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampUsToNs>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP_MS:
		// timestamp (us) to timestamp (ms)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampUsToMs>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP_SEC:
		// timestamp (us) to timestamp (s)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampUsToSec>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static cast_function_t TimestampTzCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// timestamp with time zone to varchar
		VectorStringCast<timestamp_t, duckdb::StringCastTZ>(source, result, count);
		break;
	case LogicalTypeId::TIME_TZ:
		// timestamp with time zone to time with time zone.
		// TODO: set the offset to +00
		UnaryExecutor::Execute<timestamp_t, dtime_t, duckdb::Cast>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP:
		// timestamp with time zone to timestamp (us)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::Cast>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static cast_function_t TimestampNsCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// timestamp (ns) to varchar
		VectorStringCast<timestamp_t, duckdb::CastFromTimestampNS>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP:
		// timestamp (ns) to timestamp (us)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampNsToUs>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static cast_function_t TimestampMsCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// timestamp (ms) to varchar
		VectorStringCast<timestamp_t, duckdb::CastFromTimestampMS>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP:
		// timestamp (ms) to timestamp (us)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampMsToUs>(source, result, count);
		break;
	default:
		return nullptr;
	}
}

static cast_function_t TimestampSecCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// timestamp (sec) to varchar
		VectorStringCast<timestamp_t, duckdb::CastFromTimestampSec>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP:
		// timestamp (s) to timestamp (us)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampSecToUs>(source, result, count);
		break;
	default:
		return nullptr;
	}
}

static cast_function_t IntervalCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// time to varchar
		VectorStringCast<interval_t, duckdb::StringCast>(source, result, count);
		break;
	default:
		return nullptr;
	}
}

}
