//===--------------------------------------------------------------------===//
// scatter.cpp
// Description: This file contains the implementation of the scatter operations
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/aggregate_operators.hpp"
#include "duckdb/common/operator/constant_operators.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"

using namespace duckdb;
using namespace std;

template <class T, bool IGNORE_NULL> static void scatter_set_loop(Vector &source, data_ptr_t dest[], idx_t offset) {
	auto data = (T *)source.GetData();
	if (source.vector_type == VectorType::CONSTANT_VECTOR) {
		if (!source.nullmask[0]) {
			VectorOperations::Exec(source, [&](idx_t i, idx_t k) {
				auto destination = (T *)(dest[i] + offset);
				*destination = data[0];
			});
		} else {
			VectorOperations::Exec(source, [&](idx_t i, idx_t k) {
				auto destination = (T *)(dest[i] + offset);
				*destination = NullValue<T>();
			});
		}
	} else {
		assert(source.vector_type == VectorType::FLAT_VECTOR);
		if (IGNORE_NULL || !source.nullmask.any()) {
			VectorOperations::Exec(source, [&](idx_t i, idx_t k) {
				auto destination = (T *)(dest[i] + offset);
				*destination = data[i];
			});
		} else {
			VectorOperations::Exec(source, [&](idx_t i, idx_t k) {
				auto destination = (T *)(dest[i] + offset);
				if (source.nullmask[i]) {
					*destination = NullValue<T>();
				} else {
					*destination = data[i];
				}
			});
		}
	}
}

template <bool IGNORE_NULL = false> static void scatter_set_all_loop(Vector &source, data_ptr_t dest[], idx_t offset) {
	switch (source.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		scatter_set_loop<int8_t, IGNORE_NULL>(source, dest, offset);
		break;
	case TypeId::INT16:
		scatter_set_loop<int16_t, IGNORE_NULL>(source, dest, offset);
		break;
	case TypeId::INT32:
		scatter_set_loop<int32_t, IGNORE_NULL>(source, dest, offset);
		break;
	case TypeId::INT64:
		scatter_set_loop<int64_t, IGNORE_NULL>(source, dest, offset);
		break;
	case TypeId::HASH:
		scatter_set_loop<uint64_t, IGNORE_NULL>(source, dest, offset);
		break;
	case TypeId::FLOAT:
		scatter_set_loop<float, IGNORE_NULL>(source, dest, offset);
		break;
	case TypeId::DOUBLE:
		scatter_set_loop<double, IGNORE_NULL>(source, dest, offset);
		break;
	case TypeId::VARCHAR:
		scatter_set_loop<string_t, IGNORE_NULL>(source, dest, offset);
		break;
	default:
		throw NotImplementedException("Unimplemented type for scatter");
	}
}

void VectorOperations::Scatter::SetAll(Vector &source, Vector &dest, bool set_null, idx_t offset) {
	if (dest.type != TypeId::POINTER) {
		throw InvalidTypeException(dest.type, "Cannot scatter to non-pointer type!");
	}
	auto dest_data = (data_ptr_t *)dest.GetData();
	if (set_null) {
		scatter_set_all_loop<false>(source, dest_data, offset);
	} else {
		scatter_set_all_loop<true>(source, dest_data, offset);
	}
}
