
#include "execution/vector/vector_operations.hpp"
#include "common/exception.hpp"
#include "common/types/hash.hpp"
#include "common/types/operators.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Templated Looping Functions
//===--------------------------------------------------------------------===//
template <class T, class RES, class OP>
void _templated_unary_loop_templated_function(Vector &left, Vector &result) {
	T *ldata = (T *)left.data;
	RES *result_data = (RES *)result.data;
	if (left.sel_vector) {
		for (size_t i = 0; i < left.count; i++) {
			result_data[i] = OP::template Operation<T, RES>(ldata[left.sel_vector[i]]);
		}
	} else {
		for (size_t i = 0; i < left.count; i++) {
			result_data[i] = OP::template Operation<T, RES>(ldata[i]);
		}
	}
	result.count = left.count;
}

template<class T>
static void _cast_loop(Vector& source, Vector& result) {
	switch (source.type) {
	case TypeId::TINYINT:
		_templated_unary_loop_templated_function<int8_t, T, operators::Cast>(source, result);
		break;
	case TypeId::SMALLINT:
		_templated_unary_loop_templated_function<int16_t, T, operators::Cast>(source, result);
		break;
	case TypeId::INTEGER:
		_templated_unary_loop_templated_function<int32_t, T, operators::Cast>(source, result);
		break;
	case TypeId::BIGINT:
		_templated_unary_loop_templated_function<int64_t, T, operators::Cast>(source, result);
		break;
	case TypeId::DECIMAL:
		_templated_unary_loop_templated_function<double, T, operators::Cast>(source, result);
		break;
	case TypeId::POINTER:
		_templated_unary_loop_templated_function<uint64_t, T, operators::Cast>(source, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type for copy");
	}
}

template <class T>
static void _copy_loop(Vector &left, void *target) {
	T *ldata = (T *)left.data;
	T *result_data = (T *)target;
	if (left.sel_vector) {
		for (size_t i = 0; i < left.count; i++) {
			result_data[i] = ldata[left.sel_vector[i]];
		}
	} else {
		for (size_t i = 0; i < left.count; i++) {
			result_data[i] = ldata[i];
		}
	}
}

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
// Copy the data from source to target, casting if the types don't match
void VectorOperations::Cast(Vector& source, Vector& result) {
	if (source.type == result.type) {
		throw NotImplementedException("Cast between equal types");
	}

	switch (result.type) {
	case TypeId::TINYINT:
		_cast_loop<int8_t>(source, result);
		break;
	case TypeId::SMALLINT:
		_cast_loop<int16_t>(source, result);
		break;
	case TypeId::INTEGER:
		_cast_loop<int32_t>(source, result);
		break;
	case TypeId::BIGINT:
		_cast_loop<int64_t>(source, result);
		break;
	case TypeId::DECIMAL:
		_cast_loop<double>(source, result);
		break;
	case TypeId::POINTER:
		_cast_loop<uint64_t>(source, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type for copy");
	}
}

void VectorOperations::Copy(Vector &source, void *target) {
	if (source.count == 0)
		return;
	switch (source.type) {
	case TypeId::TINYINT:
		_copy_loop<int8_t>(source, target);
		break;
	case TypeId::SMALLINT:
		_copy_loop<int16_t>(source, target);
		break;
	case TypeId::INTEGER:
		_copy_loop<int32_t>(source, target);
		break;
	case TypeId::BIGINT:
		_copy_loop<int64_t>(source, target);
		break;
	case TypeId::DECIMAL:
		_copy_loop<double>(source, target);
		break;
	case TypeId::POINTER:
		_copy_loop<uint64_t>(source, target);
		break;
	default:
		throw NotImplementedException("Unimplemented type for copy");
	}
}
