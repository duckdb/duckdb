
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
template <class T, class RES, class OP>
void _templated_unary_loop_templated_function(Vector &left, Vector &result) {
	T *ldata = (T *)left.data;
	RES *result_data = (RES *)result.data;
	if (left.sel_vector) {
		for (size_t i = 0; i < left.count; i++) {
			result_data[left.sel_vector[i]] =
			    OP::template Operation<T, RES>(ldata[left.sel_vector[i]]);
		}
	} else {
		for (size_t i = 0; i < left.count; i++) {
			result_data[i] = OP::template Operation<T, RES>(ldata[i]);
		}
	}
	result.sel_vector = left.sel_vector;
	result.count = left.count;
}

template <class T> static void _cast_loop(Vector &source, Vector &result) {
	switch (source.type) {
	case TypeId::TINYINT:
		_templated_unary_loop_templated_function<int8_t, T, operators::Cast>(
		    source, result);
		break;
	case TypeId::SMALLINT:
		_templated_unary_loop_templated_function<int16_t, T, operators::Cast>(
		    source, result);
		break;
	case TypeId::INTEGER:
		_templated_unary_loop_templated_function<int32_t, T, operators::Cast>(
		    source, result);
		break;
	case TypeId::BIGINT:
		_templated_unary_loop_templated_function<int64_t, T, operators::Cast>(
		    source, result);
		break;
	case TypeId::DECIMAL:
		_templated_unary_loop_templated_function<double, T, operators::Cast>(
		    source, result);
		break;
	case TypeId::POINTER:
		_templated_unary_loop_templated_function<uint64_t, T, operators::Cast>(
		    source, result);
		break;
	case TypeId::VARCHAR:
		_templated_unary_loop_templated_function<const char *, T,
		                                         operators::Cast>(source,
		                                                          result);
		break;
	case TypeId::DATE:
		_templated_unary_loop_templated_function<date_t, T,
		                                         operators::CastFromDate>(
		    source, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type for cast");
	}
}

template <class T>
static void _copy_loop(Vector &left, void *target, size_t offset,
                       size_t element_count) {
	T *ldata = (T *)left.data;
	T *result_data = (T *)target;
	if (left.sel_vector) {
		for (size_t i = 0; i < element_count; i++) {
			result_data[i] = ldata[left.sel_vector[offset + i]];
		}
	} else {
		for (size_t i = 0; i < element_count; i++) {
			result_data[i] = ldata[offset + i];
		}
	}
}

template <class T>
static void _copy_loop_set_null(Vector &left, void *target, size_t offset,
                       size_t element_count) {
	T *ldata = (T *)left.data;
	T *result_data = (T *)target;
	if (left.sel_vector) {
		for (size_t i = 0; i < element_count; i++) {
			if (left.nullmask[left.sel_vector[offset + i]]) {
				result_data[i] = NullValue<T>();
			} else {
				result_data[i] = ldata[left.sel_vector[offset + i]];
			}
		}
	} else {
		for (size_t i = 0; i < element_count; i++) {
			if (left.nullmask[offset + i]) {
				result_data[i] = NullValue<T>();
			} else {
				result_data[i] = ldata[offset + i];
			}
		}
	}
}

template <class T>
static void _copy_loop_check_null(Vector &left, Vector &right) {
	T *ldata = (T *)left.data;
	T *rdata = (T *)right.data;
	if (left.sel_vector) {
		for (size_t i = 0; i < left.count; i++) {
			rdata[i] = ldata[left.sel_vector[i]];
			if (IsNullValue<T>(rdata[i])) {
				right.nullmask[i] = true;
			}
		}
	} else {
		for (size_t i = 0; i < left.count; i++) {
			rdata[i] = ldata[i];
			if (IsNullValue<T>(rdata[i])) {
				right.nullmask[i] = true;
			}
		}
	}
	right.count = left.count;
}
template <class T>
static void _case_loop(Vector &check, Vector &res_true, Vector &res_false,
                       Vector &result) {
	bool *cond = (bool *)check.data;
	T *true_data = (T *)res_true.data;
	T *false_data = (T *)res_false.data;
	T *res = (T *)result.data;
	// it might be the case that not everything has a selection vector
	// as constants do not need a selection vector
	// check if we are using a selection vector
	bool use_sel_vector =
	    check.sel_vector || res_true.sel_vector || res_false.sel_vector;
	if (use_sel_vector) {
		// if we are, set it in the result
		result.sel_vector = check.sel_vector
		                        ? check.sel_vector
		                        : (res_true.sel_vector ? res_true.sel_vector
		                                               : res_false.sel_vector);
	}
	// now check for constants
	// we handle constants by multiplying the index access by 0 to avoid 2^3
	// branches in the code
	size_t check_mul = 1, res_true_mul = 1, res_false_mul = 1;
	if (check.count == 1 && !check.sel_vector) {
		check_mul = 0;
		// set a mock selection vector of [0] if we are using a selection vector
		check.sel_vector = use_sel_vector ? ZERO_VECTOR : nullptr;
	}
	// handle for res_true constant
	if (res_true.count == 1 && !res_true.sel_vector) {
		res_true_mul = 0;
		res_true.sel_vector = use_sel_vector ? ZERO_VECTOR : nullptr;
	}
	// handle res_false constant
	if (res_false.count == 1 && !res_false.sel_vector) {
		res_false_mul = 0;
		res_false.sel_vector = use_sel_vector ? ZERO_VECTOR : nullptr;
	}
	if (use_sel_vector) {
		assert(res_true.sel_vector);
		assert(res_false.sel_vector);
		assert(check.sel_vector);
		for (size_t i = 0; i < result.count; i++) {
			size_t check_index = check.sel_vector[check_mul * i];
			size_t true_index = res_true.sel_vector[res_true_mul * i];
			size_t false_index = res_false.sel_vector[res_false_mul * i];

			bool branch = (cond[check_index] && !check.nullmask[check_index]);
			res[result.sel_vector[i]] =
			    branch ? true_data[true_index] : false_data[false_index];

			result.nullmask[result.sel_vector[i]] =
			    branch ? res_true.nullmask[true_index]
			           : res_false.nullmask[false_index];
		}
	} else {
		for (size_t i = 0; i < result.count; i++) {
			size_t check_index = check_mul * i;
			size_t true_index = res_true_mul * i;
			size_t false_index = res_false_mul * i;

			bool branch = (cond[check_index] && !check.nullmask[check_index]);
			res[i] = branch ? true_data[true_index] : false_data[false_index];

			result.nullmask[i] = branch ? res_true.nullmask[true_index]
			                            : res_false.nullmask[false_index];
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

	result.nullmask = source.nullmask;
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
	case TypeId::VARCHAR:
		_cast_loop<const char *>(source, result);
		break;
	case TypeId::DATE:
		if (source.type == TypeId::VARCHAR) {
			_templated_unary_loop_templated_function<const char *, date_t,
			                                         operators::CastToDate>(
			    source, result);
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
void VectorOperations::Copy(Vector &source, void *target, size_t offset,
                            size_t element_count) {
	if (!TypeIsConstantSize(source.type)) {
		throw Exception(
		    "Cannot copy non-constant size data using this method!");
	}
	if (source.count == 0)
		return;
	if (element_count == 0) {
		element_count = source.count;
	}
	assert(offset + element_count <= source.count);

	switch (source.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		_copy_loop<int8_t>(source, target, offset, element_count);
		break;
	case TypeId::SMALLINT:
		_copy_loop<int16_t>(source, target, offset, element_count);
		break;
	case TypeId::INTEGER:
		_copy_loop<int32_t>(source, target, offset, element_count);
		break;
	case TypeId::BIGINT:
		_copy_loop<int64_t>(source, target, offset, element_count);
		break;
	case TypeId::DECIMAL:
		_copy_loop<double>(source, target, offset, element_count);
		break;
	case TypeId::POINTER:
		_copy_loop<uint64_t>(source, target, offset, element_count);
		break;
	case TypeId::DATE:
		_copy_loop<date_t>(source, target, offset, element_count);
		break;
	default:
		throw NotImplementedException("Unimplemented type for copy");
	}
}

void VectorOperations::CopyNull(Vector &source, void *target, size_t offset,
                 size_t element_count) {
	if (source.count == 0)
		return;
	if (element_count == 0) {
		element_count = source.count;
	}
	assert(offset + element_count <= source.count);

	switch (source.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		_copy_loop_set_null<int8_t>(source, target, offset, element_count);
		break;
	case TypeId::SMALLINT:
		_copy_loop_set_null<int16_t>(source, target, offset, element_count);
		break;
	case TypeId::INTEGER:
		_copy_loop_set_null<int32_t>(source, target, offset, element_count);
		break;
	case TypeId::BIGINT:
		_copy_loop_set_null<int64_t>(source, target, offset, element_count);
		break;
	case TypeId::DECIMAL:
		_copy_loop_set_null<double>(source, target, offset, element_count);
		break;
	case TypeId::POINTER:
		_copy_loop_set_null<uint64_t>(source, target, offset, element_count);
		break;
	case TypeId::DATE:
		_copy_loop_set_null<date_t>(source, target, offset, element_count);
		break;
	case TypeId::VARCHAR:
		_copy_loop_set_null<const char*>(source, target, offset, element_count);
		break;
	default:
		throw NotImplementedException("Unimplemented type for copy");
	}
}

void VectorOperations::CopyNull(Vector &source, Vector &target) {
	if (source.count == 0)
		return;

	switch (source.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		_copy_loop_check_null<int8_t>(source, target);
		break;
	case TypeId::SMALLINT:
		_copy_loop_check_null<int16_t>(source, target);
		break;
	case TypeId::INTEGER:
		_copy_loop_check_null<int32_t>(source, target);
		break;
	case TypeId::BIGINT:
		_copy_loop_check_null<int64_t>(source, target);
		break;
	case TypeId::DECIMAL:
		_copy_loop_check_null<double>(source, target);
		break;
	case TypeId::POINTER:
		_copy_loop_check_null<uint64_t>(source, target);
		break;
	case TypeId::DATE:
		_copy_loop_check_null<date_t>(source, target);
		break;
	case TypeId::VARCHAR:
		_copy_loop_check_null<const char*>(source, target);
		break;
	default:
		throw NotImplementedException("Unimplemented type for copy");
	}
}

void VectorOperations::Copy(Vector &source, Vector &target, size_t offset) {
	if (source.type != target.type) {
		throw NotImplementedException("Copy types don't match!");
	}
	assert(offset < source.count);
	target.count = source.count - offset;
	if (source.sel_vector) {
		for (size_t i = 0; i < target.count; i++) {
			target.nullmask[i] = source.nullmask[source.sel_vector[offset + i]];
		}
	} else {
		target.nullmask = source.nullmask << offset;
	}
	VectorOperations::Copy(source, target.data, offset, target.count);
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
	if (result.type != res_true.type || result.type != res_false.type) {
		throw Exception("Case types have to match!");
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

//===--------------------------------------------------------------------===//
// Apply selection vector
//===--------------------------------------------------------------------===//
template <class T>
void _templated_apply_selection_vector(Vector &left, Vector &result,
                                       sel_t *sel_vector) {
	T *ldata = (T *)left.data;
	T *rdata = (T *)result.data;
	if (left.sel_vector) {
		for (size_t i = 0; i < result.count; i++) {
			rdata[i] = ldata[left.sel_vector[sel_vector[i]]];
		}
	} else {
		for (size_t i = 0; i < result.count; i++) {
			rdata[i] = ldata[sel_vector[i]];
		}
	}
}
void VectorOperations::ApplySelectionVector(Vector &left, Vector &result,
                                            sel_t *sel_vector) {
	if (left.type != result.type) {
		throw Exception("Types of vectors do not match!");
	}
	switch (result.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		_templated_apply_selection_vector<int8_t>(left, result, sel_vector);
		break;
	case TypeId::SMALLINT:
		_templated_apply_selection_vector<int16_t>(left, result, sel_vector);
		break;
	case TypeId::INTEGER:
		_templated_apply_selection_vector<int32_t>(left, result, sel_vector);
		break;
	case TypeId::BIGINT:
		_templated_apply_selection_vector<int64_t>(left, result, sel_vector);
		break;
	case TypeId::DECIMAL:
		_templated_apply_selection_vector<double>(left, result, sel_vector);
		break;
	case TypeId::POINTER:
		_templated_apply_selection_vector<uint64_t>(left, result, sel_vector);
		break;
	case TypeId::VARCHAR:
		_templated_apply_selection_vector<const char *>(left, result,
		                                                sel_vector);
		break;
	case TypeId::DATE:
		_templated_apply_selection_vector<date_t>(left, result, sel_vector);
		break;
	default:
		throw NotImplementedException("Unimplemented type for case expression");
	}
}
