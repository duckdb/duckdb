//===--------------------------------------------------------------------===//
// cast_operators.cpp
// Description: This file contains the implementation of the different casts
//===--------------------------------------------------------------------===//

#include "common/operator/cast_operators.hpp"

#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class SRC_TYPE, class DST_TYPE, class OP, bool IGNORE_NULL>
void templated_cast_loop(Vector &source, Vector &result) {
	auto ldata = (SRC_TYPE *)source.data;
	auto result_data = (DST_TYPE *)result.data;
	if (!IGNORE_NULL || !result.nullmask.any()) {
		VectorOperations::Exec(
		    source, [&](size_t i, size_t k) { result_data[i] = OP::template Operation<SRC_TYPE, DST_TYPE>(ldata[i]); });
	} else {
		VectorOperations::Exec(source, [&](size_t i, size_t k) {
			if (!result.nullmask[i]) {
				result_data[i] = OP::template Operation<SRC_TYPE, DST_TYPE>(ldata[i]);
			}
		});
	}
	result.sel_vector = source.sel_vector;
	result.count = source.count;
}

template <class SRC, class OP, bool IGNORE_NULL> static void result_cast_switch(Vector &source, Vector &result) {
	// now switch on the result type
	switch (result.type) {
	case TypeId::BOOLEAN:
		templated_cast_loop<SRC, bool, OP, IGNORE_NULL>(source, result);
		break;
	case TypeId::TINYINT:
		templated_cast_loop<SRC, int8_t, OP, IGNORE_NULL>(source, result);
		break;
	case TypeId::SMALLINT:
		templated_cast_loop<SRC, int16_t, OP, IGNORE_NULL>(source, result);
		break;
	case TypeId::INTEGER:
		templated_cast_loop<SRC, int32_t, OP, IGNORE_NULL>(source, result);
		break;
	case TypeId::BIGINT:
		templated_cast_loop<SRC, int64_t, OP, IGNORE_NULL>(source, result);
		break;
	case TypeId::DECIMAL:
		templated_cast_loop<SRC, double, OP, IGNORE_NULL>(source, result);
		break;
	case TypeId::POINTER:
		templated_cast_loop<SRC, uint64_t, OP, IGNORE_NULL>(source, result);
		break;
	case TypeId::VARCHAR: {
		// result is VARCHAR
		// we have to place the resulting strings in the string heap
		auto ldata = (SRC *)source.data;
		auto result_data = (const char **)result.data;
		VectorOperations::Exec(source, [&](size_t i, size_t k) {
			if (source.nullmask[i]) {
				result_data[i] = nullptr;
			} else {
				auto str = OP::template Operation<SRC, string>(ldata[i]);
				result_data[i] = result.string_heap.AddString(str);
			}
		});
		result.sel_vector = source.sel_vector;
		result.count = source.count;
		break;
	}
	case TypeId::DATE:
		templated_cast_loop<SRC, date_t, operators::CastToDate, true>(source, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type for cast");
	}
}
void VectorOperations::Cast(Vector &source, Vector &result) {
	if (source.type == result.type) {
		throw NotImplementedException("Cast between equal types");
	}

	result.nullmask = source.nullmask;
	// first switch on source type
	switch (source.type) {
	case TypeId::BOOLEAN:
		result_cast_switch<bool, operators::Cast, true>(source, result);
		break;
	case TypeId::TINYINT:
		result_cast_switch<int8_t, operators::Cast, true>(source, result);
		break;
	case TypeId::SMALLINT:
		result_cast_switch<int16_t, operators::Cast, true>(source, result);
		break;
	case TypeId::INTEGER:
		result_cast_switch<int32_t, operators::Cast, true>(source, result);
		break;
	case TypeId::BIGINT:
		result_cast_switch<int64_t, operators::Cast, true>(source, result);
		break;
	case TypeId::DECIMAL:
		result_cast_switch<double, operators::Cast, true>(source, result);
		break;
	case TypeId::POINTER:
		result_cast_switch<uint64_t, operators::Cast, true>(source, result);
		break;
	case TypeId::VARCHAR:
		result_cast_switch<const char *, operators::Cast, true>(source, result);
		break;
	case TypeId::DATE:
		result_cast_switch<date_t, operators::CastFromDate, true>(source, result);
		break;
	case TypeId::TIMESTAMP:
		if (result.type == TypeId::VARCHAR) {
			throw NotImplementedException("Cannot cast type from timestamp!");
		} else {
			throw NotImplementedException("Cannot cast type from timestamp!");
		}
		break;
	default:
		throw NotImplementedException("Unimplemented type for cast");
	}
}
