//===--------------------------------------------------------------------===//
// append.cpp
// Description: This file contains the implementation of the append function
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class T, bool HAS_NULL>
static void vector_append_function(T *__restrict source, T *__restrict target, index_t count,
                                   sel_t *__restrict sel_vector, nullmask_t &nullmask, index_t right_offset) {
	target += right_offset;
	VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
		target[k] = source[i];
		if (HAS_NULL && IsNullValue<T>(target[k])) {
			nullmask[right_offset + k] = true;
		}
	});
}

template <class T> static void vector_append_loop(Vector &left, Vector &right, bool has_null) {
	auto ldata = (T *)left.GetData();
	auto rdata = (T *)right.GetData();
	if (has_null) {
		vector_append_function<T, true>(ldata, rdata, left.size(), left.sel_vector(), right.nullmask, right.size());
	} else {
		vector_append_function<T, false>(ldata, rdata, left.size(), left.sel_vector(), right.nullmask, right.size());
	}
	right.SetCount(right.size() + left.size());
}

void VectorOperations::AppendFromStorage(Vector &source, Vector &target, bool has_null) {
	if (source.size() == 0)
		return;

	if (source.size() + target.size() > STANDARD_VECTOR_SIZE) {
		throw Exception("Trying to append past STANDARD_VECTOR_SIZE!");
	}

	switch (source.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		vector_append_loop<int8_t>(source, target, has_null);
		break;
	case TypeId::INT16:
		vector_append_loop<int16_t>(source, target, has_null);
		break;
	case TypeId::INT32:
		vector_append_loop<int32_t>(source, target, has_null);
		break;
	case TypeId::INT64:
		vector_append_loop<int64_t>(source, target, has_null);
		break;
	case TypeId::FLOAT:
		vector_append_loop<float>(source, target, has_null);
		break;
	case TypeId::DOUBLE:
		vector_append_loop<double>(source, target, has_null);
		break;
	case TypeId::POINTER:
		vector_append_loop<uint64_t>(source, target, has_null);
		break;
	case TypeId::VARCHAR:
		vector_append_loop<const char *>(source, target, has_null);
		break;
	default:
		throw NotImplementedException("Unimplemented type for copy");
	}
}
