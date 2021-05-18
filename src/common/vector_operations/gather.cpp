//===--------------------------------------------------------------------===//
// gather.cpp
// Description: This file contains the implementation of the gather operators
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/constant_operators.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

template <class T>
static void TemplatedGatherLoop(Vector &source, Vector &dest, idx_t count, idx_t col_offset, idx_t col_idx) {
	auto addresses = FlatVector::GetData<uintptr_t>(source);
	auto data = FlatVector::GetData<T>(dest);
	auto &mask = FlatVector::Validity(dest);

	for (idx_t i = 0; i < count; i++) {
		auto val = Load<T>((const_data_ptr_t)addresses[i] + col_offset);
		if (IsNullValue<T>(val)) {
			mask.SetInvalid(i);
		} else {
			data[i] = val;
		}
	}
}

void VectorOperations::Gather::Set(Vector &source, Vector &dest, idx_t count, idx_t col_offset, idx_t col_idx) {
	D_ASSERT(source.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(source.GetType().id() == LogicalTypeId::POINTER); // "Cannot gather from non-pointer type!"

	dest.SetVectorType(VectorType::FLAT_VECTOR);
	switch (dest.GetType().InternalType()) {
	case PhysicalType::UINT8:
		TemplatedGatherLoop<uint8_t>(source, dest, count, col_offset, col_idx);
		break;
	case PhysicalType::UINT16:
		TemplatedGatherLoop<uint16_t>(source, dest, count, col_offset, col_idx);
		break;
	case PhysicalType::UINT32:
		TemplatedGatherLoop<uint32_t>(source, dest, count, col_offset, col_idx);
		break;
	case PhysicalType::UINT64:
		TemplatedGatherLoop<uint64_t>(source, dest, count, col_offset, col_idx);
		break;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedGatherLoop<int8_t>(source, dest, count, col_offset, col_idx);
		break;
	case PhysicalType::INT16:
		TemplatedGatherLoop<int16_t>(source, dest, count, col_offset, col_idx);
		break;
	case PhysicalType::INT32:
		TemplatedGatherLoop<int32_t>(source, dest, count, col_offset, col_idx);
		break;
	case PhysicalType::INT64:
		TemplatedGatherLoop<int64_t>(source, dest, count, col_offset, col_idx);
		break;
	case PhysicalType::INT128:
		TemplatedGatherLoop<hugeint_t>(source, dest, count, col_offset, col_idx);
		break;
	case PhysicalType::FLOAT:
		TemplatedGatherLoop<float>(source, dest, count, col_offset, col_idx);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGatherLoop<double>(source, dest, count, col_offset, col_idx);
		break;
	case PhysicalType::POINTER:
		TemplatedGatherLoop<uintptr_t>(source, dest, count, col_offset, col_idx);
		break;
	case PhysicalType::INTERVAL:
		TemplatedGatherLoop<interval_t>(source, dest, count, col_offset, col_idx);
		break;
	case PhysicalType::VARCHAR:
		TemplatedGatherLoop<string_t>(source, dest, count, col_offset, col_idx);
		break;
	default:
		throw NotImplementedException("Unimplemented type for gather");
	}
}

} // namespace duckdb
