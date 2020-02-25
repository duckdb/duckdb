//===--------------------------------------------------------------------===//
// append.cpp
// Description: This file contains the implementation of the append function
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class T> static void storage_read_loop(Vector &input, Vector &target) {
	auto source_data = (T *)input.GetData();
	auto target_data = (T *)target.GetData();
	VectorOperations::Exec(input, [&](index_t i, index_t k) {
		target_data[k] = source_data[i];
		if (IsNullValue<T>(target_data[k])) {
			target.nullmask[k] = true;
		}
	});
}

void VectorOperations::ReadFromStorage(Vector &source, Vector &target) {
	assert(source.SameCardinality(target));
	if (source.size() == 0) {
		return;
	}

	switch (source.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		storage_read_loop<int8_t>(source, target);
		break;
	case TypeId::INT16:
		storage_read_loop<int16_t>(source, target);
		break;
	case TypeId::INT32:
		storage_read_loop<int32_t>(source, target);
		break;
	case TypeId::INT64:
		storage_read_loop<int64_t>(source, target);
		break;
	case TypeId::FLOAT:
		storage_read_loop<float>(source, target);
		break;
	case TypeId::DOUBLE:
		storage_read_loop<double>(source, target);
		break;
	case TypeId::POINTER:
		storage_read_loop<uint64_t>(source, target);
		break;
	case TypeId::VARCHAR:
		storage_read_loop<string_t>(source, target);
		break;
	default:
		throw NotImplementedException("Unimplemented type for copy");
	}
}
