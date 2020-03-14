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
	case TypeId::LIST:
		templated_fill_nullmask<list_entry_t>(v);
		break;
	case TypeId::STRUCT:
		break;
	default:
		throw NotImplementedException("Type not implemented for null mask");
	}
}
