//===--------------------------------------------------------------------===//
// cast_operators.cpp
// Description: This file contains the implementation of the different casts
//===--------------------------------------------------------------------===//
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {
using namespace std;

template <class SRC, class OP> static void string_cast(Vector &source, Vector &result, idx_t count) {
	assert(result.type == PhysicalType::VARCHAR);
	UnaryExecutor::Execute<SRC, string_t, true>(source, result, count,
	                                            [&](SRC input) { return OP::template Operation<SRC>(input, result); });
}

static NotImplementedException UnimplementedCast(LogicalType source_type, LogicalType target_type) {
	return NotImplementedException("Unimplemented type for cast (%s -> %s)", LogicalTypeToString(source_type).c_str(),
	                               LogicalTypeToString(target_type).c_str());
}

// NULL cast only works if all values in source are NULL, otherwise an unimplemented cast exception is thrown
static void null_cast(Vector &source, Vector &result, LogicalType source_type, LogicalType target_type, idx_t count) {
	if (VectorOperations::HasNotNull(source, count)) {
		throw UnimplementedCast(source_type, target_type);
	}
	if (source.vector_type == VectorType::CONSTANT_VECTOR) {
		result.vector_type = VectorType::CONSTANT_VECTOR;
		ConstantVector::SetNull(result, true);
	} else {
		result.vector_type = VectorType::FLAT_VECTOR;
		FlatVector::Nullmask(result).set();
	}
}

template <class SRC>
static void numeric_cast_switch(Vector &source, Vector &result, LogicalType source_type, LogicalType target_type, idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case LogicalTypeId::BOOLEAN:
		assert(result.type == PhysicalType::BOOL);
		UnaryExecutor::Execute<SRC, bool, duckdb::Cast, true>(source, result, count);
		break;
	case LogicalTypeId::TINYINT:
		assert(result.type == PhysicalType::INT8);
		UnaryExecutor::Execute<SRC, int8_t, duckdb::Cast, true>(source, result, count);
		break;
	case LogicalTypeId::SMALLINT:
		assert(result.type == PhysicalType::INT16);
		UnaryExecutor::Execute<SRC, int16_t, duckdb::Cast, true>(source, result, count);
		break;
	case LogicalTypeId::INTEGER:
		assert(result.type == PhysicalType::INT32);
		UnaryExecutor::Execute<SRC, int32_t, duckdb::Cast, true>(source, result, count);
		break;
	case LogicalTypeId::BIGINT:
		assert(result.type == PhysicalType::INT64);
		UnaryExecutor::Execute<SRC, int64_t, duckdb::Cast, true>(source, result, count);
		break;
	case LogicalTypeId::HUGEINT:
		assert(result.type == PhysicalType::INT128);
		UnaryExecutor::Execute<SRC, hugeint_t, duckdb::Cast, true>(source, result, count);
		break;
	case LogicalTypeId::FLOAT:
		assert(result.type == PhysicalType::FLOAT);
		UnaryExecutor::Execute<SRC, float, duckdb::Cast, true>(source, result, count);
		break;
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DOUBLE:
		assert(result.type == PhysicalType::DOUBLE);
		UnaryExecutor::Execute<SRC, double, duckdb::Cast, true>(source, result, count);
		break;
	case LogicalTypeId::VARCHAR: {
		string_cast<SRC, duckdb::StringCast>(source, result, count);
		break;
	}
	case LogicalTypeId::LIST: {
		assert(result.type == PhysicalType::LIST);
		auto list_child = make_unique<ChunkCollection>();
		ListVector::SetEntry(result, move(list_child));
		null_cast(source, result, source_type, target_type, count);
		break;
	}
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

template <class OP>
static void string_cast_numeric_switch(Vector &source, Vector &result, LogicalType source_type, LogicalType target_type,
                                       idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case LogicalTypeId::BOOLEAN:
		assert(result.type == PhysicalType::BOOL);
		UnaryExecutor::Execute<string_t, bool, OP, true>(source, result, count);
		break;
	case LogicalTypeId::TINYINT:
		assert(result.type == PhysicalType::INT8);
		UnaryExecutor::Execute<string_t, int8_t, OP, true>(source, result, count);
		break;
	case LogicalTypeId::SMALLINT:
		assert(result.type == PhysicalType::INT16);
		UnaryExecutor::Execute<string_t, int16_t, OP, true>(source, result, count);
		break;
	case LogicalTypeId::INTEGER:
		assert(result.type == PhysicalType::INT32);
		UnaryExecutor::Execute<string_t, int32_t, OP, true>(source, result, count);
		break;
	case LogicalTypeId::BIGINT:
		assert(result.type == PhysicalType::INT64);
		UnaryExecutor::Execute<string_t, int64_t, OP, true>(source, result, count);
		break;
	case LogicalTypeId::HUGEINT:
		assert(result.type == PhysicalType::INT128);
		UnaryExecutor::Execute<string_t, hugeint_t, OP, true>(source, result, count);
		break;
	case LogicalTypeId::FLOAT:
		assert(result.type == PhysicalType::FLOAT);
		UnaryExecutor::Execute<string_t, float, OP, true>(source, result, count);
		break;
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DOUBLE:
		assert(result.type == PhysicalType::DOUBLE);
		UnaryExecutor::Execute<string_t, double, OP, true>(source, result, count);
		break;
	case LogicalTypeId::INTERVAL:
		assert(result.type == PhysicalType::INTERVAL);
		UnaryExecutor::Execute<string_t, interval_t, OP, true>(source, result, count);
		break;
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

static void string_cast_switch(Vector &source, Vector &result, LogicalType source_type, LogicalType target_type, idx_t count,
                               bool strict = false) {
	// now switch on the result type
	switch (target_type.id) {
	case LogicalTypeId::DATE:
		assert(result.type == PhysicalType::INT32);
		if (strict) {
			UnaryExecutor::Execute<string_t, date_t, duckdb::StrictCastToDate, true>(source, result, count);
		} else {
			UnaryExecutor::Execute<string_t, date_t, duckdb::CastToDate, true>(source, result, count);
		}
		break;
	case LogicalTypeId::TIME:
		assert(result.type == PhysicalType::INT32);
		if (strict) {
			UnaryExecutor::Execute<string_t, dtime_t, duckdb::StrictCastToTime, true>(source, result, count);
		} else {
			UnaryExecutor::Execute<string_t, dtime_t, duckdb::CastToTime, true>(source, result, count);
		}
		break;
	case LogicalTypeId::TIMESTAMP:
		assert(result.type == PhysicalType::INT64);
		UnaryExecutor::Execute<string_t, timestamp_t, duckdb::CastToTimestamp, true>(source, result, count);
		break;
	case LogicalTypeId::BLOB:
		assert(result.type == PhysicalType::VARCHAR);
		string_cast<string_t, duckdb::CastToBlob>(source, result, count);
		break;
	default:
		if (strict) {
			string_cast_numeric_switch<duckdb::StrictCast>(source, result, source_type, target_type, count);
		} else {
			string_cast_numeric_switch<duckdb::Cast>(source, result, source_type, target_type, count);
		}
		break;
	}
}

static void date_cast_switch(Vector &source, Vector &result, LogicalType source_type, LogicalType target_type, idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case LogicalTypeId::VARCHAR:
		// date to varchar
		string_cast<date_t, duckdb::CastFromDate>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP:
		// date to timestamp
		UnaryExecutor::Execute<date_t, timestamp_t, duckdb::CastDateToTimestamp, true>(source, result, count);
		break;
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

static void time_cast_switch(Vector &source, Vector &result, LogicalType source_type, LogicalType target_type, idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case LogicalTypeId::VARCHAR:
		// time to varchar
		string_cast<dtime_t, duckdb::CastFromTime>(source, result, count);
		break;
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

static void timestamp_cast_switch(Vector &source, Vector &result, LogicalType source_type, LogicalType target_type,
                                  idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case LogicalTypeId::VARCHAR:
		// timestamp to varchar
		string_cast<timestamp_t, duckdb::CastFromTimestamp>(source, result, count);
		break;
	case LogicalTypeId::DATE:
		// timestamp to date
		UnaryExecutor::Execute<timestamp_t, date_t, duckdb::CastTimestampToDate, true>(source, result, count);
		break;
	case LogicalTypeId::TIME:
		// timestamp to time
		UnaryExecutor::Execute<timestamp_t, dtime_t, duckdb::CastTimestampToTime, true>(source, result, count);
		break;
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

static void interval_cast_switch(Vector &source, Vector &result, LogicalType source_type, LogicalType target_type,
                                 idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case LogicalTypeId::VARCHAR:
		// time to varchar
		string_cast<interval_t, duckdb::StringCast>(source, result, count);
		break;
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

static void blob_cast_switch(Vector &source, Vector &result, LogicalType source_type, LogicalType target_type, idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case LogicalTypeId::VARCHAR:
		// blob to varchar
		string_cast<string_t, duckdb::CastFromBlob>(source, result, count);
		break;
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

void VectorOperations::Cast(Vector &source, Vector &result, LogicalType source_type, LogicalType target_type, idx_t count,
                            bool strict) {
	assert(source_type != target_type);
	// first switch on source type
	switch (source_type.id) {
	case LogicalTypeId::BOOLEAN:
		assert(source.type == PhysicalType::BOOL);
		numeric_cast_switch<bool>(source, result, source_type, target_type, count);
		break;
	case LogicalTypeId::TINYINT:
		assert(source.type == PhysicalType::INT8);
		numeric_cast_switch<int8_t>(source, result, source_type, target_type, count);
		break;
	case LogicalTypeId::SMALLINT:
		assert(source.type == PhysicalType::INT16);
		numeric_cast_switch<int16_t>(source, result, source_type, target_type, count);
		break;
	case LogicalTypeId::INTEGER:
		assert(source.type == PhysicalType::INT32);
		numeric_cast_switch<int32_t>(source, result, source_type, target_type, count);
		break;
	case LogicalTypeId::BIGINT:
		assert(source.type == PhysicalType::INT64);
		numeric_cast_switch<int64_t>(source, result, source_type, target_type, count);
		break;
	case LogicalTypeId::HUGEINT:
		assert(source.type == PhysicalType::INT128);
		numeric_cast_switch<hugeint_t>(source, result, source_type, target_type, count);
		break;
	case LogicalTypeId::FLOAT:
		assert(source.type == PhysicalType::FLOAT);
		numeric_cast_switch<float>(source, result, source_type, target_type, count);
		break;
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DOUBLE:
		assert(source.type == PhysicalType::DOUBLE);
		numeric_cast_switch<double>(source, result, source_type, target_type, count);
		break;
	case LogicalTypeId::DATE:
		assert(source.type == PhysicalType::INT32);
		date_cast_switch(source, result, source_type, target_type, count);
		break;
	case LogicalTypeId::TIME:
		assert(source.type == PhysicalType::INT32);
		time_cast_switch(source, result, source_type, target_type, count);
		break;
	case LogicalTypeId::TIMESTAMP:
		assert(source.type == PhysicalType::INT64);
		timestamp_cast_switch(source, result, source_type, target_type, count);
		break;
	case LogicalTypeId::INTERVAL:
		assert(source.type == PhysicalType::INTERVAL);
		interval_cast_switch(source, result, source_type, target_type, count);
		break;
	case LogicalTypeId::VARCHAR:
		assert(source.type == PhysicalType::VARCHAR);
		string_cast_switch(source, result, source_type, target_type, count, strict);
		break;
	case LogicalTypeId::BLOB:
		assert(source.type == PhysicalType::VARCHAR);
		blob_cast_switch(source, result, source_type, target_type, count);
		break;
	case LogicalTypeId::SQLNULL: {
		// cast a NULL to another type, just copy the properties and change the type
		result.vector_type = VectorType::CONSTANT_VECTOR;
		ConstantVector::SetNull(result, true);
		break;
	}
	default:
		throw UnimplementedCast(source_type, target_type);
	}
}

void VectorOperations::Cast(Vector &source, Vector &result, idx_t count, bool strict) {
	return VectorOperations::Cast(source, result, LogicalTypeFromInternalType(source.type),
	                              LogicalTypeFromInternalType(result.type), count, strict);
}

} // namespace duckdb
