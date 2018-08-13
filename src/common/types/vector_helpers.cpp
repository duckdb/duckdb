
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

template <class T>
static void _case_loop(Vector &check, Vector &res_true, Vector &res_false,
                       Vector &result) {
	bool *cond = (bool *)check.data;
	T *true_data = (T *)res_true.data;
	T *false_data = (T *)res_false.data;
	T *res = (T *)result.data;
	if (check.sel_vector) {
		throw Exception("Selection vector in case check not supported");
	}

	size_t check_mul = 1, res_true_mul = 1, res_false_mul = 1;
	if (check.count == 1)
		check_mul = 0;
	if (res_true.count == 1)
		res_true_mul = 0;
	if (res_false.count == 1)
		res_false_mul = 0;

	if (res_true.sel_vector && res_false.sel_vector) {
		for (size_t i = 0; i < result.count; i++) {
			res[i] = cond[check_mul * i]
			             ? true_data[res_true.sel_vector[res_true_mul * i]]
			             : false_data[res_false.sel_vector[res_false_mul * i]];
		}
	} else if (res_false.sel_vector) {
		for (size_t i = 0; i < result.count; i++) {
			res[i] = cond[check_mul * i]
			             ? true_data[res_true_mul * i]
			             : false_data[res_false.sel_vector[res_false_mul * i]];
		}
	} else if (res_true.sel_vector) {
		for (size_t i = 0; i < result.count; i++) {
			res[i] = cond[check_mul * i]
			             ? true_data[res_true.sel_vector[res_true_mul * i]]
			             : false_data[res_false_mul * i];
		}
	} else {
		for (size_t i = 0; i < result.count; i++) {
			res[i] = cond[check_mul * i] ? true_data[res_true_mul * i]
			                             : false_data[res_false_mul * i];
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

//===--------------------------------------------------------------------===//
// Copy data from vector
//===--------------------------------------------------------------------===//
void VectorOperations::Copy(Vector &source, void *target, size_t element_count,
                            size_t offset) {
	if (source.count == 0)
		return;
	if (element_count == 0) {
		element_count = source.count;
	}
	assert(offset + element_count <= source.count);

	switch (source.type) {
	case TypeId::BOOLEAN:
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

//===--------------------------------------------------------------------===//
// Case statement (if, else, then)
//===--------------------------------------------------------------------===//
void VectorOperations::Case(Vector &check, Vector &res_true, Vector &res_false,
                            Vector &result) {
	if ((check.count != 1 && check.count != result.count) ||
	    (res_true.count != 1 && res_true.count != result.count) ||
	    (res_false.count != 1 && res_false.count != result.count)) {
		throw Exception("Vector lengths don't match in case!");
	}
	if (check.type != TypeId::BOOLEAN) {
		throw Exception("Case check has to be a boolean vector!");
	}

	switch (result.type) {
	case TypeId::TINYINT:
		_case_loop<int8_t>(check, res_true, res_false, result);
		break;
	case TypeId::SMALLINT:
		_case_loop<int16_t>(check, res_true, res_false, result);
		break;
	case TypeId::INTEGER:
		_case_loop<int32_t>(check, res_true, res_false, result);
		break;
	case TypeId::BIGINT:
		_case_loop<int64_t>(check, res_true, res_false, result);
		break;
	case TypeId::DECIMAL:
		_case_loop<double>(check, res_true, res_false, result);
		break;
	case TypeId::POINTER:
		_case_loop<uint64_t>(check, res_true, res_false, result);
		break;
	case TypeId::VARCHAR:
		_case_loop<const char *>(check, res_true, res_false, result);
		break;
	case TypeId::DATE:
		_case_loop<date_t>(check, res_true, res_false, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type for case expression");
	}
	result.count = check.count;
}
