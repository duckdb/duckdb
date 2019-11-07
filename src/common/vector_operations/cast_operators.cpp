//===--------------------------------------------------------------------===//
// cast_operators.cpp
// Description: This file contains the implementation of the different casts
//===--------------------------------------------------------------------===//
#include "duckdb/common/operator/cast_operators.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class SRC_TYPE, class DST_TYPE, class OP, bool IGNORE_NULL>
void templated_cast_loop(Vector &source, Vector &result) {
	auto ldata = (SRC_TYPE *)source.data;
	auto result_data = (DST_TYPE *)result.data;
	if (!IGNORE_NULL || !result.nullmask.any()) {
		VectorOperations::Exec(source, [&](index_t i, index_t k) {
			result_data[i] = OP::template Operation<SRC_TYPE, DST_TYPE>(ldata[i]);
		});
	} else {
		VectorOperations::Exec(source, [&](index_t i, index_t k) {
			if (!result.nullmask[i]) {
				result_data[i] = OP::template Operation<SRC_TYPE, DST_TYPE>(ldata[i]);
			}
		});
	}
}

template <class SRC, class OP, bool IGNORE_NULL>
static void result_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::BOOLEAN:
		assert(result.type == TypeId::BOOLEAN);
		templated_cast_loop<SRC, bool, OP, IGNORE_NULL>(source, result);
		break;
	case SQLTypeId::TINYINT:
		assert(result.type == TypeId::TINYINT);
		templated_cast_loop<SRC, int8_t, OP, IGNORE_NULL>(source, result);
		break;
	case SQLTypeId::SMALLINT:
		assert(result.type == TypeId::SMALLINT);
		templated_cast_loop<SRC, int16_t, OP, IGNORE_NULL>(source, result);
		break;
	case SQLTypeId::INTEGER:
		assert(result.type == TypeId::INTEGER);
		templated_cast_loop<SRC, int32_t, OP, IGNORE_NULL>(source, result);
		break;
	case SQLTypeId::BIGINT:
		assert(result.type == TypeId::BIGINT);
		templated_cast_loop<SRC, int64_t, OP, IGNORE_NULL>(source, result);
		break;
	case SQLTypeId::FLOAT:
		assert(result.type == TypeId::FLOAT);
		templated_cast_loop<SRC, float, OP, IGNORE_NULL>(source, result);
		break;
	case SQLTypeId::DECIMAL:
	case SQLTypeId::DOUBLE:
		assert(result.type == TypeId::DOUBLE);
		templated_cast_loop<SRC, double, OP, IGNORE_NULL>(source, result);
		break;
	case SQLTypeId::DATE:
		assert(result.type == TypeId::INTEGER);
		templated_cast_loop<SRC, int32_t, duckdb::CastToDate, true>(source, result);
		break;
	case SQLTypeId::TIME:
		assert(result.type == TypeId::INTEGER);
		templated_cast_loop<SRC, int32_t, duckdb::CastToTime, true>(source, result);
		break;
	case SQLTypeId::TIMESTAMP:
		assert(result.type == TypeId::BIGINT);
		templated_cast_loop<SRC, int64_t, duckdb::CastToTimestamp, true>(source, result);
		break;
	case SQLTypeId::VARCHAR: {
		assert(result.type == TypeId::VARCHAR);
		// result is VARCHAR
		// we have to place the resulting strings in the string heap
		auto ldata = (SRC *)source.data;
		auto result_data = (const char **)result.data;
		VectorOperations::Exec(source, [&](index_t i, index_t k) {
			if (source.nullmask[i]) {
				result_data[i] = nullptr;
			} else {
				auto str = OP::template Operation<SRC, string>(ldata[i]);
				result_data[i] = result.string_heap.AddString(str);
			}
		});
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for cast");
	}
}
void VectorOperations::Cast(Vector &source, Vector &result, SQLType source_type, SQLType target_type) {
	if (source_type == target_type) {
		throw NotImplementedException("Cast between equal types");
	}

	result.nullmask = source.nullmask;
	result.sel_vector = source.sel_vector;
	result.count = source.count;
	// first switch on source type
	switch (source_type.id) {
	case SQLTypeId::BOOLEAN:
		assert(source.type == TypeId::BOOLEAN);
		result_cast_switch<bool, duckdb::Cast, true>(source, result, source_type, target_type);
		break;
	case SQLTypeId::TINYINT:
		assert(source.type == TypeId::TINYINT);
		result_cast_switch<int8_t, duckdb::Cast, true>(source, result, source_type, target_type);
		break;
	case SQLTypeId::SMALLINT:
		assert(source.type == TypeId::SMALLINT);
		result_cast_switch<int16_t, duckdb::Cast, true>(source, result, source_type, target_type);
		break;
	case SQLTypeId::INTEGER:
		assert(source.type == TypeId::INTEGER);
		result_cast_switch<int32_t, duckdb::Cast, true>(source, result, source_type, target_type);
		break;
	case SQLTypeId::BIGINT:
		assert(source.type == TypeId::BIGINT);
		result_cast_switch<int64_t, duckdb::Cast, true>(source, result, source_type, target_type);
		break;
	case SQLTypeId::FLOAT:
		assert(source.type == TypeId::FLOAT);
		result_cast_switch<float, duckdb::Cast, true>(source, result, source_type, target_type);
		break;
	case SQLTypeId::DECIMAL:
	case SQLTypeId::DOUBLE:
		assert(source.type == TypeId::DOUBLE);
		result_cast_switch<double, duckdb::Cast, true>(source, result, source_type, target_type);
		break;
	case SQLTypeId::DATE:
		assert(source.type == TypeId::INTEGER);
		result_cast_switch<int32_t, duckdb::CastFromDate, true>(source, result, source_type, target_type);
		break;
	case SQLTypeId::TIME:
		assert(source.type == TypeId::INTEGER);
		result_cast_switch<int32_t, duckdb::CastFromTime, true>(source, result, source_type, target_type);
		break;
	case SQLTypeId::TIMESTAMP:
		assert(source.type == TypeId::BIGINT);
		result_cast_switch<int64_t, duckdb::CastFromTimestamp, true>(source, result, source_type, target_type);
		break;
	case SQLTypeId::VARCHAR:
		assert(source.type == TypeId::VARCHAR);
		result_cast_switch<const char *, duckdb::Cast, true>(source, result, source_type, target_type);
		break;
	case SQLTypeId::SQLNULL:
		break;
	default:
		throw NotImplementedException("Unimplemented type for cast");
	}
}

void VectorOperations::Cast(Vector &source, Vector &result) {
	return VectorOperations::Cast(source, result, SQLTypeFromInternalType(source.type),
	                              SQLTypeFromInternalType(result.type));
}
