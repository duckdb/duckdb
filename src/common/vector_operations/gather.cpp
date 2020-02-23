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
	template <class T, class OP> static void Operation(Vector &src, Vector &result, index_t offset) {
		auto source = (data_ptr_t *)src.GetData();
		auto ldata = (T *)result.GetData();
		auto rsel = result.sel_vector();
		if (rsel) {
			VectorOperations::Exec(src, [&](index_t i, index_t k) {
				data_ptr_t ptr = source[i] + offset;
				T source_value = *((T *)ptr);
				if (IsNullValue<T>(source_value)) {
					result.nullmask.set(rsel[k]);
				} else {
					ldata[rsel[k]] = OP::Operation(source_value, ldata[i]);
				}
			});
		} else {
			VectorOperations::Exec(src, [&](index_t i, index_t k) {
				data_ptr_t ptr = source[i] + offset;
				T source_value = *((T *)ptr);
				if (IsNullValue<T>(source_value)) {
					result.nullmask.set(k);
				} else {
					ldata[k] = OP::Operation(source_value, ldata[i]);
				}
			});
		}
	}
};

struct GatherLoopIgnoreNull {
	template <class T, class OP> static void Operation(Vector &src, Vector &result, index_t offset) {
		auto source = (data_ptr_t *)src.GetData();
		auto ldata = (T *)result.GetData();
		auto rsel = result.sel_vector();
		if (rsel) {
			VectorOperations::Exec(src, [&](index_t i, index_t k) {
				data_ptr_t ptr = source[i] + offset;
				T source_value = *((T *)ptr);
				ldata[rsel[k]] = OP::Operation(source_value, ldata[i]);
			});
		} else {
			VectorOperations::Exec(src, [&](index_t i, index_t k) {
				data_ptr_t ptr = source[i] + offset;
				T source_value = *((T *)ptr);
				ldata[k] = OP::Operation(source_value, ldata[i]);
			});
		}
	}
};

template <class LOOP, class OP> static void generic_gather_loop(Vector &source, Vector &dest, index_t offset = 0) {
	if (source.type != TypeId::POINTER) {
		throw InvalidTypeException(source.type, "Cannot gather from non-pointer type!");
	}
	dest.vector_type = VectorType::FLAT_VECTOR;
	switch (dest.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		LOOP::template Operation<int8_t, OP>(source, dest, offset);
		break;
	case TypeId::INT16:
		LOOP::template Operation<int16_t, OP>(source, dest, offset);
		break;
	case TypeId::INT32:
		LOOP::template Operation<int32_t, OP>(source, dest, offset);
		break;
	case TypeId::INT64:
		LOOP::template Operation<int64_t, OP>(source, dest, offset);
		break;
	case TypeId::FLOAT:
		LOOP::template Operation<float, OP>(source, dest, offset);
		break;
	case TypeId::DOUBLE:
		LOOP::template Operation<double, OP>(source, dest, offset);
		break;
	case TypeId::POINTER:
		LOOP::template Operation<uint64_t, OP>(source, dest, offset);
		break;
	case TypeId::VARCHAR:
		LOOP::template Operation<string_t, OP>(source, dest, offset);
		break;
	default:
		throw NotImplementedException("Unimplemented type for gather");
	}
}

void VectorOperations::Gather::Set(Vector &source, Vector &dest, bool set_null, index_t offset) {
	assert(source.size() == dest.size());
	if (set_null) {
		generic_gather_loop<GatherLoopSetNull, PickLeft>(source, dest, offset);
	} else {
		generic_gather_loop<GatherLoopIgnoreNull, PickLeft>(source, dest, offset);
	}
}
