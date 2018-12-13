//===--------------------------------------------------------------------===//
// gather.cpp
// Description: This file contains the implementation of the gather operators
//===--------------------------------------------------------------------===//

#include "common/exception.hpp"
#include "common/operator/constant_operators.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class T, class OP> void gather_templated_loop(Vector &src, Vector &result) {
	auto source = (T **)src.data;
	auto ldata = (T *)result.data;
	if (result.sel_vector) {
		VectorOperations::Exec(src, [&](size_t i, size_t k) {
			if (IsNullValue<T>(source[i][0])) {
				result.nullmask.set(result.sel_vector[k]);
			} else {
				ldata[result.sel_vector[k]] = OP::Operation(source[i][0], ldata[i]);
			}
		});
	} else {
		VectorOperations::Exec(src, [&](size_t i, size_t k) {
			if (IsNullValue<T>(source[i][0])) {
				result.nullmask.set(k);
			} else {
				ldata[k] = OP::Operation(source[i][0], ldata[i]);
			}
		});
	}
}

template <class OP> static void generic_gather_loop(Vector &source, Vector &dest) {
	if (source.type != TypeId::POINTER) {
		throw InvalidTypeException(source.type, "Cannot gather from non-pointer type!");
	}
	switch (dest.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		gather_templated_loop<int8_t, OP>(source, dest);
		break;
	case TypeId::SMALLINT:
		gather_templated_loop<int16_t, OP>(source, dest);
		break;
	case TypeId::DATE:
	case TypeId::INTEGER:
		gather_templated_loop<int32_t, OP>(source, dest);
		break;
	case TypeId::TIMESTAMP:
	case TypeId::BIGINT:
		gather_templated_loop<int64_t, OP>(source, dest);
		break;
	case TypeId::DECIMAL:
		gather_templated_loop<double, OP>(source, dest);
		break;
	case TypeId::POINTER:
		gather_templated_loop<uint64_t, OP>(source, dest);
		break;
	case TypeId::VARCHAR:
		gather_templated_loop<char *, OP>(source, dest);
		break;
	default:
		throw NotImplementedException("Unimplemented type for gather");
	}
}

void VectorOperations::Gather::Set(Vector &source, Vector &dest) {
	generic_gather_loop<operators::PickLeft>(source, dest);
}
