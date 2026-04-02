#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

namespace {
template <class T>

void CopyToStorageLoop(Vector &source, idx_t count, data_ptr_t target) {
	auto result_data = (T *)target;
	for (auto entry : source.Values<T>(count)) {
		if (!entry.IsValid()) {
			result_data[entry.index] = NullValue<T>();
		} else {
			result_data[entry.index] = entry.value;
		}
	}
}

template <class T>
void ReadFromStorageLoop(data_ptr_t source, idx_t count, Vector &result) {
	auto ldata = (T *)source;
	auto result_data = FlatVector::GetData<T>(result);
	for (idx_t i = 0; i < count; i++) {
		result_data[i] = ldata[i];
	}
}

} // namespace

void VectorOperations::WriteToStorage(Vector &source, idx_t count, data_ptr_t target) {
	if (count == 0) {
		return;
	}

	switch (source.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		CopyToStorageLoop<int8_t>(source, count, target);
		break;
	case PhysicalType::INT16:
		CopyToStorageLoop<int16_t>(source, count, target);
		break;
	case PhysicalType::INT32:
		CopyToStorageLoop<int32_t>(source, count, target);
		break;
	case PhysicalType::INT64:
		CopyToStorageLoop<int64_t>(source, count, target);
		break;
	case PhysicalType::UINT8:
		CopyToStorageLoop<uint8_t>(source, count, target);
		break;
	case PhysicalType::UINT16:
		CopyToStorageLoop<uint16_t>(source, count, target);
		break;
	case PhysicalType::UINT32:
		CopyToStorageLoop<uint32_t>(source, count, target);
		break;
	case PhysicalType::UINT64:
		CopyToStorageLoop<uint64_t>(source, count, target);
		break;
	case PhysicalType::INT128:
		CopyToStorageLoop<hugeint_t>(source, count, target);
		break;
	case PhysicalType::UINT128:
		CopyToStorageLoop<uhugeint_t>(source, count, target);
		break;
	case PhysicalType::FLOAT:
		CopyToStorageLoop<float>(source, count, target);
		break;
	case PhysicalType::DOUBLE:
		CopyToStorageLoop<double>(source, count, target);
		break;
	case PhysicalType::INTERVAL:
		CopyToStorageLoop<interval_t>(source, count, target);
		break;
	default:
		throw NotImplementedException("Unimplemented type for WriteToStorage");
	}
}

void VectorOperations::ReadFromStorage(data_ptr_t source, idx_t count, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	switch (result.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		ReadFromStorageLoop<int8_t>(source, count, result);
		break;
	case PhysicalType::INT16:
		ReadFromStorageLoop<int16_t>(source, count, result);
		break;
	case PhysicalType::INT32:
		ReadFromStorageLoop<int32_t>(source, count, result);
		break;
	case PhysicalType::INT64:
		ReadFromStorageLoop<int64_t>(source, count, result);
		break;
	case PhysicalType::UINT8:
		ReadFromStorageLoop<uint8_t>(source, count, result);
		break;
	case PhysicalType::UINT16:
		ReadFromStorageLoop<uint16_t>(source, count, result);
		break;
	case PhysicalType::UINT32:
		ReadFromStorageLoop<uint32_t>(source, count, result);
		break;
	case PhysicalType::UINT64:
		ReadFromStorageLoop<uint64_t>(source, count, result);
		break;
	case PhysicalType::INT128:
		ReadFromStorageLoop<hugeint_t>(source, count, result);
		break;
	case PhysicalType::UINT128:
		ReadFromStorageLoop<uhugeint_t>(source, count, result);
		break;
	case PhysicalType::FLOAT:
		ReadFromStorageLoop<float>(source, count, result);
		break;
	case PhysicalType::DOUBLE:
		ReadFromStorageLoop<double>(source, count, result);
		break;
	case PhysicalType::INTERVAL:
		ReadFromStorageLoop<interval_t>(source, count, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type for ReadFromStorage");
	}
}

} // namespace duckdb
