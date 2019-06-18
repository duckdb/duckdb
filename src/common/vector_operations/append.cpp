//===--------------------------------------------------------------------===//
// append.cpp
// Description: This file contains the implementation of the append function
//===--------------------------------------------------------------------===//

#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class T>
static void append_function(T *__restrict source, T *__restrict target, index_t count, sel_t *__restrict sel_vector,
                            nullmask_t &nullmask, index_t right_offset) {
	target += right_offset;
	VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
		target[k] = source[i];
		if (IsNullValue<T>(target[k])) {
			nullmask[right_offset + k] = true;
		}
	});
}

template <class T> static void append_loop(Vector &left, Vector &right) {
	auto ldata = (T *)left.data;
	auto rdata = (T *)right.data;
	append_function(ldata, rdata, left.count, left.sel_vector, right.nullmask, right.count);
	right.count += left.count;
}

void VectorOperations::AppendFromStorage(Vector &source, Vector &target) {
	if (source.count == 0)
		return;

	if (source.count + target.count > STANDARD_VECTOR_SIZE) {
		throw Exception("Trying to append past STANDARD_VECTOR_SIZE!");
	}

	switch (source.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		append_loop<int8_t>(source, target);
		break;
	case TypeId::SMALLINT:
		append_loop<int16_t>(source, target);
		break;
	case TypeId::INTEGER:
		append_loop<int32_t>(source, target);
		break;
	case TypeId::BIGINT:
		append_loop<int64_t>(source, target);
		break;
	case TypeId::FLOAT:
		append_loop<float>(source, target);
		break;
	case TypeId::DOUBLE:
		append_loop<double>(source, target);
		break;
	case TypeId::POINTER:
		append_loop<uint64_t>(source, target);
		break;
	case TypeId::VARCHAR:
		append_loop<const char *>(source, target);
		break;
	default:
		throw NotImplementedException("Unimplemented type for copy");
	}
}
