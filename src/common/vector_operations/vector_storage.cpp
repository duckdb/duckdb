#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

template <class T> static void CopyToStorageLoop(VectorData &vdata, idx_t count, data_ptr_t target) {
	auto ldata = (T *)vdata.data;
	auto result_data = (T *)target;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if ((*vdata.nullmask)[idx]) {
			result_data[i] = NullValue<T>();
		} else {
			result_data[i] = ldata[idx];
		}
	}
}

void VectorOperations::WriteToStorage(Vector &source, idx_t count, data_ptr_t target) {
	if (count == 0) {
		return;
	}
	VectorData vdata;
	source.Orrify(count, vdata);

	switch (source.type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		CopyToStorageLoop<int8_t>(vdata, count, target);
		break;
	case PhysicalType::INT16:
		CopyToStorageLoop<int16_t>(vdata, count, target);
		break;
	case PhysicalType::INT32:
		CopyToStorageLoop<int32_t>(vdata, count, target);
		break;
	case PhysicalType::INT64:
		CopyToStorageLoop<int64_t>(vdata, count, target);
		break;
	case PhysicalType::INT128:
		CopyToStorageLoop<hugeint_t>(vdata, count, target);
		break;
	case PhysicalType::HASH:
		CopyToStorageLoop<hash_t>(vdata, count, target);
		break;
	case PhysicalType::POINTER:
		CopyToStorageLoop<uintptr_t>(vdata, count, target);
		break;
	case PhysicalType::FLOAT:
		CopyToStorageLoop<float>(vdata, count, target);
		break;
	case PhysicalType::DOUBLE:
		CopyToStorageLoop<double>(vdata, count, target);
		break;
	case PhysicalType::INTERVAL:
		CopyToStorageLoop<interval_t>(vdata, count, target);
		break;
	default:
		throw NotImplementedException("Unimplemented type for CopyToStorage");
	}
}

template <class T> static void ReadFromStorageLoop(data_ptr_t source, idx_t count, Vector &result) {
	auto ldata = (T *)source;
	auto result_data = FlatVector::GetData<T>(result);
	auto &nullmask = FlatVector::Nullmask(result);
	for (idx_t i = 0; i < count; i++) {
		if (IsNullValue<T>(ldata[i])) {
			nullmask[i] = true;
		} else {
			result_data[i] = ldata[i];
		}
	}
}

void VectorOperations::ReadFromStorage(data_ptr_t source, idx_t count, Vector &result) {
	result.vector_type = VectorType::FLAT_VECTOR;
	switch (result.type.InternalType()) {
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
	case PhysicalType::INT128:
		ReadFromStorageLoop<hugeint_t>(source, count, result);
		break;
	case PhysicalType::HASH:
		ReadFromStorageLoop<hash_t>(source, count, result);
		break;
	case PhysicalType::POINTER:
		ReadFromStorageLoop<uintptr_t>(source, count, result);
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
		throw NotImplementedException("Unimplemented type for CopyToStorage");
	}
}

} // namespace duckdb
