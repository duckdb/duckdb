//===--------------------------------------------------------------------===//
// gather.cpp
// Description: This file contains the implementation of the gather operators
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/constant_operators.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

struct GatherLoopSetNull {
	template <class T, class OP> static void Operation(Vector &src, Vector &result, idx_t offset) {
		auto source = FlatVector::GetData<data_ptr_t>(src);
		auto result_data = FlatVector::GetData<T>(result);
		auto &result_nullmask = FlatVector::Nullmask(result);
		throw NotImplementedException("FIXME gather");
		// auto rsel = result.sel_vector();
		// if (rsel) {
		// 	VectorOperations::Exec(src, [&](idx_t i, idx_t k) {
		// 		data_ptr_t ptr = source[i] + offset;
		// 		T source_value = *((T *)ptr);
		// 		if (IsNullValue<T>(source_value)) {
		// 			result.nullmask.set(rsel[k]);
		// 		} else {
		// 			ldata[rsel[k]] = OP::Operation(source_value, ldata[i]);
		// 		}
		// 	});
		// } else {
			// VectorOperations::Exec(src, [&](idx_t i, idx_t k) {
			// 	data_ptr_t ptr = source[i] + offset;
			// 	T source_value = *((T *)ptr);
			// 	if (IsNullValue<T>(source_value)) {
			// 		result.nullmask.set(k);
			// 	} else {
			// 		ldata[k] = OP::Operation(source_value, ldata[i]);
			// 	}
			// });
		// }
		// for(idx_t i = 0; i < result.size(); i++) {
		// 	data_ptr_t ptr = source[i] + offset;
		// 	T source_value = *((T *)ptr);
		// 	if (IsNullValue<T>(source_value)) {
		// 		result_nullmask[k] = true;
		// 	} else {
		// 		result_data[k] = OP::Operation(source_value, ldata[i]);
		// 	}
		// }
	}
};

struct GatherLoopIgnoreNull {
	template <class T, class OP> static void Operation(Vector &src, Vector &result, idx_t offset) {
		throw NotImplementedException("FIXME gather");
		// auto source = (data_ptr_t *)src.GetData();
		// auto ldata = (T *)result.GetData();
		// auto rsel = result.sel_vector();
		// if (rsel) {
		// 	VectorOperations::Exec(src, [&](idx_t i, idx_t k) {
		// 		data_ptr_t ptr = source[i] + offset;
		// 		T source_value = *((T *)ptr);
		// 		ldata[rsel[k]] = OP::Operation(source_value, ldata[i]);
		// 	});
		// } else {
		// 	VectorOperations::Exec(src, [&](idx_t i, idx_t k) {
		// 		data_ptr_t ptr = source[i] + offset;
		// 		T source_value = *((T *)ptr);
		// 		ldata[k] = OP::Operation(source_value, ldata[i]);
		// 	});
		// }
	}
};

template<class T>
static void templated_gather_loop(Vector &source, Vector &dest, idx_t count) {
	auto addresses = FlatVector::GetData<uint64_t>(source);
	auto data = FlatVector::GetData<T>(dest);
	auto &nullmask = FlatVector::Nullmask(dest);

	for(idx_t i = 0; i < count; i++) {
		auto dataptr = (T*) addresses[i];
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
	assert(source.type == TypeId::POINTER); // "Cannot gather from non-pointer type!"

	dest.vector_type = VectorType::FLAT_VECTOR;
	switch (dest.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		templated_gather_loop<int8_t>(source, dest, count);
		break;
	case TypeId::INT16:
		templated_gather_loop<int16_t>(source, dest, count);
		break;
	case TypeId::INT32:
		templated_gather_loop<int32_t>(source, dest, count);
		break;
	case TypeId::INT64:
		templated_gather_loop<int64_t>(source, dest, count);
		break;
	case TypeId::FLOAT:
		templated_gather_loop<float>(source, dest, count);
		break;
	case TypeId::DOUBLE:
		templated_gather_loop<double>(source, dest, count);
		break;
	case TypeId::POINTER:
		templated_gather_loop<uint64_t>(source, dest, count);
		break;
	case TypeId::VARCHAR:
		templated_gather_loop<string_t>(source, dest, count);
		break;
	default:
		throw NotImplementedException("Unimplemented type for gather");
	}
}
