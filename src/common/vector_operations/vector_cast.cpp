//===--------------------------------------------------------------------===//
// cast_operators.cpp
// Description: This file contains the implementation of the different casts
//===--------------------------------------------------------------------===//
#include "duckdb/common/operator/cast_operators.hpp"

#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

using namespace duckdb;
using namespace std;

template <class SRC, class OP> static void string_cast(Vector &source, Vector &result, idx_t count) {
	assert(result.type == TypeId::VARCHAR);
	UnaryExecutor::Execute<SRC, string_t, true>(source, result, count,
	                                            [&](SRC input) { return OP::template Operation<SRC>(input, result); });
}

static NotImplementedException UnimplementedCast(SQLType source_type, SQLType target_type) {
	return NotImplementedException("Unimplemented type for cast (%s -> %s)", SQLTypeToString(source_type).c_str(),
	                               SQLTypeToString(target_type).c_str());
}

// NULL cast only works if all values in source are NULL, otherwise an unimplemented cast exception is thrown
static void null_cast(Vector &source, Vector &result, SQLType source_type, SQLType target_type, idx_t count) {
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
static void numeric_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type, idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::BOOLEAN:
		assert(result.type == TypeId::BOOL);
		UnaryExecutor::Execute<SRC, bool, duckdb::Cast, true>(source, result, count);
		break;
	case SQLTypeId::TINYINT:
		assert(result.type == TypeId::INT8);
		UnaryExecutor::Execute<SRC, int8_t, duckdb::Cast, true>(source, result, count);
		break;
	case SQLTypeId::SMALLINT:
		assert(result.type == TypeId::INT16);
		UnaryExecutor::Execute<SRC, int16_t, duckdb::Cast, true>(source, result, count);
		break;
	case SQLTypeId::INTEGER:
		assert(result.type == TypeId::INT32);
		UnaryExecutor::Execute<SRC, int32_t, duckdb::Cast, true>(source, result, count);
		break;
	case SQLTypeId::BIGINT:
		assert(result.type == TypeId::INT64);
		UnaryExecutor::Execute<SRC, int64_t, duckdb::Cast, true>(source, result, count);
		break;
	case SQLTypeId::FLOAT:
		assert(result.type == TypeId::FLOAT);
		UnaryExecutor::Execute<SRC, float, duckdb::Cast, true>(source, result, count);
		break;
	case SQLTypeId::DECIMAL:
	case SQLTypeId::DOUBLE:
		assert(result.type == TypeId::DOUBLE);
		UnaryExecutor::Execute<SRC, double, duckdb::Cast, true>(source, result, count);
		break;
	case SQLTypeId::VARCHAR: {
		string_cast<SRC, duckdb::StringCast>(source, result, count);
		break;
	}
	case SQLTypeId::LIST: {
		assert(result.type == TypeId::LIST);
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
static void string_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type, idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::BOOLEAN:
		assert(result.type == TypeId::BOOL);
		UnaryExecutor::Execute<string_t, bool, OP, true>(source, result, count);
		break;
	case SQLTypeId::TINYINT:
		assert(result.type == TypeId::INT8);
		UnaryExecutor::Execute<string_t, int8_t, OP, true>(source, result, count);
		break;
	case SQLTypeId::SMALLINT:
		assert(result.type == TypeId::INT16);
		UnaryExecutor::Execute<string_t, int16_t, OP, true>(source, result, count);
		break;
	case SQLTypeId::INTEGER:
		assert(result.type == TypeId::INT32);
		UnaryExecutor::Execute<string_t, int32_t, OP, true>(source, result, count);
		break;
	case SQLTypeId::BIGINT:
		assert(result.type == TypeId::INT64);
		UnaryExecutor::Execute<string_t, int64_t, OP, true>(source, result, count);
		break;
	case SQLTypeId::FLOAT:
		assert(result.type == TypeId::FLOAT);
		UnaryExecutor::Execute<string_t, float, OP, true>(source, result, count);
		break;
	case SQLTypeId::DECIMAL:
	case SQLTypeId::DOUBLE:
		assert(result.type == TypeId::DOUBLE);
		UnaryExecutor::Execute<string_t, double, OP, true>(source, result, count);
		break;
	case SQLTypeId::DATE:
		assert(result.type == TypeId::INT32);
		UnaryExecutor::Execute<string_t, date_t, duckdb::CastToDate, true>(source, result, count);
		break;
	case SQLTypeId::TIME:
		assert(result.type == TypeId::INT32);
		UnaryExecutor::Execute<string_t, dtime_t, duckdb::CastToTime, true>(source, result, count);
		break;
	case SQLTypeId::TIMESTAMP:
		assert(result.type == TypeId::INT64);
		UnaryExecutor::Execute<string_t, timestamp_t, duckdb::CastToTimestamp, true>(source, result, count);
		break;
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

static void date_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type, idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::VARCHAR:
		// date to varchar
		string_cast<date_t, duckdb::CastFromDate>(source, result, count);
		break;
	case SQLTypeId::TIMESTAMP:
		// date to timestamp
		UnaryExecutor::Execute<date_t, timestamp_t, duckdb::CastDateToTimestamp, true>(source, result, count);
		break;
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

static void time_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type, idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::VARCHAR:
		// time to varchar
		string_cast<dtime_t, duckdb::CastFromTime>(source, result, count);
		break;
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

static void timestamp_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type,
                                  idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::VARCHAR:
		// timestamp to varchar
		string_cast<timestamp_t, duckdb::CastFromTimestamp>(source, result, count);
		break;
	case SQLTypeId::DATE:
		// timestamp to date
		UnaryExecutor::Execute<timestamp_t, date_t, duckdb::CastTimestampToDate, true>(source, result, count);
		break;
	case SQLTypeId::TIME:
		// timestamp to time
		UnaryExecutor::Execute<timestamp_t, dtime_t, duckdb::CastTimestampToTime, true>(source, result, count);
		break;
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

void VectorOperations::Cast(Vector &source, Vector &result, SQLType source_type, SQLType target_type, idx_t count,
                            bool strict) {
	assert(source_type != target_type);
	// first switch on source type
	switch (source_type.id) {
	case SQLTypeId::BOOLEAN:
		assert(source.type == TypeId::BOOL);
		numeric_cast_switch<bool>(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::TINYINT:
		assert(source.type == TypeId::INT8);
		numeric_cast_switch<int8_t>(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::SMALLINT:
		assert(source.type == TypeId::INT16);
		numeric_cast_switch<int16_t>(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::INTEGER:
		assert(source.type == TypeId::INT32);
		numeric_cast_switch<int32_t>(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::BIGINT:
		assert(source.type == TypeId::INT64);
		numeric_cast_switch<int64_t>(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::FLOAT:
		assert(source.type == TypeId::FLOAT);
		numeric_cast_switch<float>(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::DECIMAL:
	case SQLTypeId::DOUBLE:
		assert(source.type == TypeId::DOUBLE);
		numeric_cast_switch<double>(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::DATE:
		assert(source.type == TypeId::INT32);
		date_cast_switch(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::TIME:
		assert(source.type == TypeId::INT32);
		time_cast_switch(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::TIMESTAMP:
		assert(source.type == TypeId::INT64);
		timestamp_cast_switch(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::VARCHAR:
		assert(source.type == TypeId::VARCHAR);
		if (strict) {
			string_cast_switch<duckdb::StrictCast>(source, result, source_type, target_type, count);
		} else {
			string_cast_switch<duckdb::Cast>(source, result, source_type, target_type, count);
		}
		break;
	case SQLTypeId::SQLNULL: {
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
	return VectorOperations::Cast(source, result, SQLTypeFromInternalType(source.type),
	                              SQLTypeFromInternalType(result.type), count, strict);
}
