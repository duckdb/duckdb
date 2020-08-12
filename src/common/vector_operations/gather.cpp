//===--------------------------------------------------------------------===//
// gather.cpp
// Description: This file contains the implementation of the gather operators
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/constant_operators.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {
using namespace std;

template <class T> static void templated_gather_loop(Vector &source, Vector &dest, idx_t count) {
	auto addresses = FlatVector::GetData<uintptr_t>(source);
	auto data = FlatVector::GetData<T>(dest);
	auto &nullmask = FlatVector::Nullmask(dest);

	for (idx_t i = 0; i < count; i++) {
		auto dataptr = (T *)addresses[i];
		if (IsNullValue<T>(*dataptr)) {
			nullmask[i] = true;
		} else {
			data[i] = *dataptr;
		}
		addresses[i] += sizeof(T);
	}
}

void VectorOperations::Gather::Set(Vector &source, Vector &dest, idx_t count) {
	assert(source.vector_type == VectorType::FLAT_VECTOR);
	assert(source.type == PhysicalType::POINTER); // "Cannot gather from non-pointer type!"

	dest.vector_type = VectorType::FLAT_VECTOR;
	switch (dest.type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		templated_gather_loop<int8_t>(source, dest, count);
		break;
	case PhysicalType::INT16:
		templated_gather_loop<int16_t>(source, dest, count);
		break;
	case PhysicalType::INT32:
		templated_gather_loop<int32_t>(source, dest, count);
		break;
	case PhysicalType::INT64:
		templated_gather_loop<int64_t>(source, dest, count);
		break;
	case PhysicalType::INT128:
		templated_gather_loop<hugeint_t>(source, dest, count);
		break;
	case PhysicalType::FLOAT:
		templated_gather_loop<float>(source, dest, count);
		break;
	case PhysicalType::DOUBLE:
		templated_gather_loop<double>(source, dest, count);
		break;
	case PhysicalType::POINTER:
		templated_gather_loop<uintptr_t>(source, dest, count);
		break;
	case PhysicalType::INTERVAL:
		templated_gather_loop<interval_t>(source, dest, count);
		break;
	case PhysicalType::VARCHAR:
		templated_gather_loop<string_t>(source, dest, count);
		break;
	default:
		throw NotImplementedException("Unimplemented type for gather");
	}
}

} // namespace duckdb
