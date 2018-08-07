
#include "common/assert.hpp"
#include "common/exception.hpp"
#include "common/types/hash.hpp"
#include "common/types/operators.hpp"
#include "common/types/vector_operations.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Templated Looping Functions
//===--------------------------------------------------------------------===//
struct ExecuteWithNullHandling {
	template <class T, class RES, class OP>
	static inline RES Operation(T left) {
		if (duckdb::IsNullValue<T>(left)) {
			return duckdb::NullValue<RES>();
		}
		return OP::template Operation<T, RES>(left);
	}
};
struct ExecuteWithoutNullHandling {
	template <class T, class RES, class OP>
	static inline RES Operation(T left) {
		return OP::template Operation<T, RES>(left);
	}
};

template <class T, class RES, class OP, class EXEC>
void _templated_unary_loop_templated_function_handling(Vector &left,
                                                       Vector &result) {
	T *ldata = (T *)left.data;
	RES *result_data = (RES *)result.data;
	if (left.sel_vector) {
		for (size_t i = 0; i < left.count; i++) {
			result_data[i] =
			    EXEC::template Operation<T, RES, OP>(ldata[left.sel_vector[i]]);
		}
	} else {
		for (size_t i = 0; i < left.count; i++) {
			result_data[i] = EXEC::template Operation<T, RES, OP>(ldata[i]);
		}
	}
	result.count = left.count;
}

template <class T, class RES, class OP>
void _templated_unary_loop_templated_function(Vector &left, Vector &result,
                                              bool can_have_nulls) {
	if (can_have_nulls) {
		_templated_unary_loop_templated_function_handling<
		    T, RES, OP, ExecuteWithNullHandling>(left, result);
	} else {
		_templated_unary_loop_templated_function_handling<
		    T, RES, OP, ExecuteWithoutNullHandling>(left, result);
	}
}

template <class T>
static void _cast_loop(Vector &source, Vector &result, bool can_have_nulls) {
	switch (source.type) {
	case TypeId::TINYINT:
		_templated_unary_loop_templated_function<int8_t, T, operators::Cast>(
		    source, result, can_have_nulls);
		break;
	case TypeId::SMALLINT:
		_templated_unary_loop_templated_function<int16_t, T, operators::Cast>(
		    source, result, can_have_nulls);
		break;
	case TypeId::INTEGER:
		_templated_unary_loop_templated_function<int32_t, T, operators::Cast>(
		    source, result, can_have_nulls);
		break;
	case TypeId::BIGINT:
		_templated_unary_loop_templated_function<int64_t, T, operators::Cast>(
		    source, result, can_have_nulls);
		break;
	case TypeId::DECIMAL:
		_templated_unary_loop_templated_function<double, T, operators::Cast>(
		    source, result, can_have_nulls);
		break;
	case TypeId::POINTER:
		_templated_unary_loop_templated_function<uint64_t, T, operators::Cast>(
		    source, result, can_have_nulls);
		break;
	case TypeId::VARCHAR:
		_templated_unary_loop_templated_function<const char *, T,
		                                         operators::Cast>(
		    source, result, can_have_nulls);
		break;
	case TypeId::DATE:
		_templated_unary_loop_templated_function<date_t, T,
		                                         operators::CastFromDate>(
		    source, result, can_have_nulls);
		break;
	default:
		throw NotImplementedException("Unimplemented type for cast");
	}
}

template <class T>
static void _copy_loop(Vector &left, void *target, size_t element_count,
                       size_t offset) {
	T *ldata = (T *)left.data;
	T *result_data = (T *)target;
	if (left.sel_vector) {
		for (size_t i = offset; i < offset + element_count; i++) {
			result_data[i] = ldata[left.sel_vector[i]];
		}
	} else {
		for (size_t i = offset; i < offset + element_count; i++) {
			result_data[i] = ldata[i];
		}
	}
}

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
// Copy the data from source to target, casting if the types don't match
void VectorOperations::Cast(Vector &source, Vector &result) {
	if (source.type == result.type) {
		throw NotImplementedException("Cast between equal types");
	}

	bool can_have_nulls = true;

	switch (result.type) {
	case TypeId::TINYINT:
		_cast_loop<int8_t>(source, result, can_have_nulls);
		break;
	case TypeId::SMALLINT:
		_cast_loop<int16_t>(source, result, can_have_nulls);
		break;
	case TypeId::INTEGER:
		_cast_loop<int32_t>(source, result, can_have_nulls);
		break;
	case TypeId::BIGINT:
		_cast_loop<int64_t>(source, result, can_have_nulls);
		break;
	case TypeId::DECIMAL:
		_cast_loop<double>(source, result, can_have_nulls);
		break;
	case TypeId::POINTER:
		_cast_loop<uint64_t>(source, result, can_have_nulls);
		break;
	case TypeId::VARCHAR:
		_cast_loop<const char *>(source, result, can_have_nulls);
		break;
	case TypeId::DATE:
		if (source.type == TypeId::VARCHAR) {
			_templated_unary_loop_templated_function<const char *, date_t,
			                                         operators::CastToDate>(
			    source, result, can_have_nulls);
		} else {
			throw NotImplementedException("Cannot cast type to date!");
		}
		break;
	default:
		throw NotImplementedException("Unimplemented type for cast");
	}
}

void VectorOperations::Copy(Vector &source, void *target, size_t element_count,
                            size_t offset) {
	if (source.count == 0)
		return;
	if (element_count == 0) {
		element_count = source.count;
	}
	assert(offset + element_count <= source.count);

	switch (source.type) {
	case TypeId::TINYINT:
		_copy_loop<int8_t>(source, target, element_count, offset);
		break;
	case TypeId::SMALLINT:
		_copy_loop<int16_t>(source, target, element_count, offset);
		break;
	case TypeId::INTEGER:
		_copy_loop<int32_t>(source, target, element_count, offset);
		break;
	case TypeId::BIGINT:
		_copy_loop<int64_t>(source, target, element_count, offset);
		break;
	case TypeId::DECIMAL:
		_copy_loop<double>(source, target, element_count, offset);
		break;
	case TypeId::POINTER:
		_copy_loop<uint64_t>(source, target, element_count, offset);
		break;
	case TypeId::DATE:
		_copy_loop<date_t>(source, target, element_count, offset);
		break;
	case TypeId::VARCHAR:
		_copy_loop<const char *>(source, target, element_count, offset);
		break;
	default:
		throw NotImplementedException("Unimplemented type for copy");
	}
}

void VectorOperations::Copy(Vector &source, Vector &target, size_t offset) {
	if (source.type != target.type) {
		throw NotImplementedException("Copy types don't match!");
	}
	target.count = std::min(source.count - offset, target.maximum_size);
	VectorOperations::Copy(source, target.data, target.count, offset);
}
