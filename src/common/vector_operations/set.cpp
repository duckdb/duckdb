//===--------------------------------------------------------------------===//
// set.cpp
// Description: This file contains the implementation of VectorOperations::Set
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/constant_operators.hpp"
#include "duckdb/common/vector_operations/unary_loops.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"

using namespace duckdb;
using namespace std;

template <class T>
static inline void set_loop(T *__restrict result_data, T value, index_t count, sel_t *__restrict sel_vector) {
	VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) { result_data[i] = value; });
}

template <class T> void templated_set_loop(Vector &result, T value) {
	auto result_data = (T *)result.data;

	set_loop<T>(result_data, value, result.count, result.sel_vector);
}

//===--------------------------------------------------------------------===//
// Set all elements of a vector to the constant value
//===--------------------------------------------------------------------===//
void VectorOperations::Set(Vector &result, Value value) {
	if (value.type != result.type) {
		value = value.CastAs(result.type);
	}

	if (value.is_null) {
		// initialize the NULL mask with all 1
		result.nullmask.set();
	} else {
		// set all values in the nullmask to 0
		result.nullmask.reset();
		switch (result.type) {
		case TypeId::BOOLEAN:
		case TypeId::TINYINT:
			templated_set_loop<int8_t>(result, value.value_.tinyint);
			break;
		case TypeId::SMALLINT:
			templated_set_loop<int16_t>(result, value.value_.smallint);
			break;
		case TypeId::INTEGER:
			templated_set_loop<int32_t>(result, value.value_.integer);
			break;
		case TypeId::BIGINT:
			templated_set_loop<int64_t>(result, value.value_.bigint);
			break;
		case TypeId::FLOAT:
			templated_set_loop<float>(result, value.value_.float_);
			break;
		case TypeId::DOUBLE:
			templated_set_loop<double>(result, value.value_.double_);
			break;
		case TypeId::POINTER:
			templated_set_loop<uintptr_t>(result, value.value_.pointer);
			break;
		case TypeId::VARCHAR: {
			auto str = result.string_heap.AddString(value.str_value);
			auto dataptr = (const char **)result.data;
			VectorOperations::Exec(result.sel_vector, result.count, [&](index_t i, index_t k) { dataptr[i] = str; });
			break;
		}
		default:
			throw NotImplementedException("Unimplemented type for Set");
		}
	}
}

//===--------------------------------------------------------------------===//
// Set all elements of a vector to the constant value
//===--------------------------------------------------------------------===//
template <class T> void templated_fill_nullmask(Vector &v) {
	auto data = (T *)v.data;
	VectorOperations::Exec(v, [&](index_t i, index_t k) {
		if (v.nullmask[i]) {
			data[i] = NullValue<T>();
		}
	});
	v.nullmask.reset();
}

void VectorOperations::FillNullMask(Vector &v) {
	if (!v.nullmask.any()) {
		// no NULL values, skip
		return;
	}
	switch (v.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		templated_fill_nullmask<int8_t>(v);
		break;
	case TypeId::SMALLINT:
		templated_fill_nullmask<int16_t>(v);
		break;
	case TypeId::INTEGER:
		templated_fill_nullmask<int32_t>(v);
		break;
	case TypeId::BIGINT:
		templated_fill_nullmask<int64_t>(v);
		break;
	case TypeId::FLOAT:
		templated_fill_nullmask<float>(v);
		break;
	case TypeId::DOUBLE:
		templated_fill_nullmask<double>(v);
		break;
	case TypeId::VARCHAR:
		templated_fill_nullmask<const char *>(v);
		break;
	default:
		throw NotImplementedException("Type not implemented for null mask");
	}
}
