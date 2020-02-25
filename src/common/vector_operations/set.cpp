//===--------------------------------------------------------------------===//
// set.cpp
// Description: This file contains the implementation of VectorOperations::Set
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/constant_operators.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"

using namespace duckdb;
using namespace std;

template <class T>
static inline void set_loop(T *__restrict result_data, T value, idx_t count, sel_t *__restrict sel_vector) {
	VectorOperations::Exec(sel_vector, count, [&](idx_t i, idx_t k) { result_data[i] = value; });
}

template <class T> void templated_set_loop(Vector &result, T value) {
	auto result_data = (T *)result.GetData();

	set_loop<T>(result_data, value, result.size(), result.sel_vector());
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
		case TypeId::BOOL:
		case TypeId::INT8:
			templated_set_loop<int8_t>(result, value.value_.tinyint);
			break;
		case TypeId::INT16:
			templated_set_loop<int16_t>(result, value.value_.smallint);
			break;
		case TypeId::INT32:
			templated_set_loop<int32_t>(result, value.value_.integer);
			break;
		case TypeId::INT64:
			templated_set_loop<int64_t>(result, value.value_.bigint);
			break;
		case TypeId::FLOAT:
			templated_set_loop<float>(result, value.value_.float_);
			break;
		case TypeId::DOUBLE:
			templated_set_loop<double>(result, value.value_.double_);
			break;
		case TypeId::HASH:
			templated_set_loop<uint64_t>(result, value.value_.hash);
			break;
		case TypeId::POINTER:
			templated_set_loop<uintptr_t>(result, value.value_.pointer);
			break;
		case TypeId::VARCHAR: {
			auto str = result.AddString(value.str_value);
			auto dataptr = (string_t *)result.GetData();
			VectorOperations::Exec(result, [&](idx_t i, idx_t k) { dataptr[i] = str; });
			break;
		}
		default:
			throw NotImplementedException("Unimplemented type for Set");
		}
	}
}

//===--------------------------------------------------------------------===//
// Fill the null mask of a value, setting every NULL value to NullValue<T>()
//===--------------------------------------------------------------------===//
template <class T> void templated_fill_nullmask(Vector &v) {
	auto data = (T *)v.GetData();
	if (v.vector_type == VectorType::CONSTANT_VECTOR) {
		if (v.nullmask[0]) {
			data[0] = NullValue<T>();
			v.nullmask[0] = false;
		}
	} else {
		if (!v.nullmask.any()) {
			// no NULL values, skip
			return;
		}
		VectorOperations::Exec(v, [&](idx_t i, idx_t k) {
			if (v.nullmask[i]) {
				data[i] = NullValue<T>();
			}
		});
		v.nullmask.reset();
	}
}

void VectorOperations::FillNullMask(Vector &v) {
	switch (v.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		templated_fill_nullmask<int8_t>(v);
		break;
	case TypeId::INT16:
		templated_fill_nullmask<int16_t>(v);
		break;
	case TypeId::INT32:
		templated_fill_nullmask<int32_t>(v);
		break;
	case TypeId::INT64:
		templated_fill_nullmask<int64_t>(v);
		break;
	case TypeId::FLOAT:
		templated_fill_nullmask<float>(v);
		break;
	case TypeId::DOUBLE:
		templated_fill_nullmask<double>(v);
		break;
	case TypeId::VARCHAR:
		templated_fill_nullmask<string_t>(v);
		break;
	default:
		throw NotImplementedException("Type not implemented for null mask");
	}
}
