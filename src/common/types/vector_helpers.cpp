
#include "common/assert.hpp"
#include "common/exception.hpp"
#include "common/types/hash.hpp"
#include "common/types/null_value.hpp"
#include "common/types/operators.hpp"
#include "common/types/vector_operations.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Templated Looping Functions
//===--------------------------------------------------------------------===//
template <class SRC, class DST, class OP>
void _templated_cast_loop(Vector &left, Vector &result) {
	SRC *ldata = (SRC *)left.data;
	DST *result_data = (DST *)result.data;
	VectorOperations::Exec(left, [&](size_t i, size_t k) {
		result_data[i] = OP::template Operation<SRC, DST>(ldata[i]);
	});
	result.sel_vector = left.sel_vector;
	result.count = left.count;
}

template <class SRC> static void _cast_loop(Vector &source, Vector &result) {
	switch (result.type) {
	case TypeId::TINYINT:
		_templated_cast_loop<SRC, int8_t, operators::Cast>(source, result);
		break;
	case TypeId::SMALLINT:
		_templated_cast_loop<SRC, int16_t, operators::Cast>(source, result);
		break;
	case TypeId::INTEGER:
		_templated_cast_loop<SRC, int32_t, operators::Cast>(source, result);
		break;
	case TypeId::BIGINT:
		_templated_cast_loop<SRC, int64_t, operators::Cast>(source, result);
		break;
	case TypeId::DECIMAL:
		_templated_cast_loop<SRC, double, operators::Cast>(source, result);
		break;
	case TypeId::POINTER:
		_templated_cast_loop<SRC, uint64_t, operators::Cast>(source, result);
		break;
	case TypeId::VARCHAR: {
		// result is VARCHAR
		// we have to place the resulting strings in the string heap
		auto ldata = (SRC *)source.data;
		auto result_data = (const char **)result.data;
		VectorOperations::Exec(source, [&](size_t i, size_t k) {
			if (source.nullmask[i]) {
				result_data[i] = nullptr;
			} else {
				auto str =
				    operators::Cast::template Operation<SRC, std::string>(
				        ldata[i]);
				result_data[i] = result.string_heap.AddString(str);
			}
		});
		result.sel_vector = source.sel_vector;
		result.count = source.count;
		break;
	}
	case TypeId::DATE:
		_templated_cast_loop<SRC, date_t, operators::CastToDate>(source,
		                                                         result);
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
	VectorOperations::Exec(
	    left, [&](size_t i, size_t k) { result_data[k - offset] = ldata[i]; },
	    offset, element_count);
}

template <class T>
static void _copy_loop_set_null(Vector &left, void *target, size_t offset,
                                size_t element_count) {
	T *ldata = (T *)left.data;
	T *result_data = (T *)target;
	VectorOperations::Exec(left,
	                       [&](size_t i, size_t k) {
		                       if (left.nullmask[i]) {
			                       result_data[k - offset] = NullValue<T>();
		                       } else {
			                       result_data[k - offset] = ldata[i];
		                       }
	                       },
	                       offset, element_count);
}

template <class T>
static void _append_loop_check_null(Vector &left, Vector &right) {
	T *ldata = (T *)left.data;
	T *rdata = (T *)right.data;
	VectorOperations::Exec(left, [&](size_t i, size_t k) {
		rdata[right.count + k] = ldata[i];
		if (IsNullValue<T>(rdata[right.count + k])) {
			right.nullmask[right.count + k] = true;
		}
	});
	right.count += left.count;
}

template <class T, class OP>
static void _case_loop(Vector &check, Vector &res_true, Vector &res_false,
                       Vector &result) {
	auto cond = (bool *)check.data;
	auto true_data = (T *)res_true.data;
	auto false_data = (T *)res_false.data;
	auto res = (T *)result.data;
	// it might be the case that not everything has a selection vector
	// as constants do not need a selection vector
	// check if we are using a selection vector
	if (check.sel_vector) {
		result.sel_vector = check.sel_vector;
	} else if (res_true.sel_vector) {
		result.sel_vector = res_true.sel_vector;
	} else if (res_false.sel_vector) {
		result.sel_vector = res_false.sel_vector;
	} else {
		result.sel_vector = nullptr;
	}

	// now check for constants
	// we handle constants by multiplying the index access by 0 to avoid 2^3
	// branches in the code
	size_t check_mul = check.IsConstant() ? 0 : 1,
	       res_true_mul = res_true.IsConstant() ? 0 : 1,
	       res_false_mul = res_false.IsConstant() ? 0 : 1;

	VectorOperations::Exec(result, [&](size_t i, size_t k) {
		size_t check_index = check.sel_vector ? i : k * check_mul;
		size_t true_index = res_true.sel_vector ? i : k * res_true_mul;
		size_t false_index = res_false.sel_vector ? i : k * res_false_mul;
		bool branch = (cond[check_index] && !check.nullmask[check_index]);
		bool is_null = branch ? res_true.nullmask[true_index]
		                      : res_false.nullmask[false_index];
		result.nullmask[i] = is_null;
		if (!is_null) {
			res[i] = OP::Operation(result, branch, true_data[true_index],
			                       false_data[false_index]);
		}
	});
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
	// first switch on source type
	switch (source.type) {
	case TypeId::BOOLEAN:
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
		if (result.type == TypeId::VARCHAR) {
			_templated_cast_loop<date_t, const char *, operators::CastFromDate>(
			    source, result);
		} else {
			throw NotImplementedException("Cannot cast type from date!");
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
		throw InvalidTypeException(
		    source.type,
		    "Cannot copy non-constant size types using this method!");
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
		_copy_loop_set_null<const char *>(source, target, offset,
		                                  element_count);
		break;
	default:
		throw NotImplementedException("Unimplemented type for copy");
	}
}

void VectorOperations::AppendNull(Vector &source, Vector &target) {
	if (source.count == 0)
		return;

	if (source.count + target.count > STANDARD_VECTOR_SIZE) {
		throw Exception("Trying to append past STANDARD_VECTOR_SIZE!");
	}

	switch (source.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		_append_loop_check_null<int8_t>(source, target);
		break;
	case TypeId::SMALLINT:
		_append_loop_check_null<int16_t>(source, target);
		break;
	case TypeId::INTEGER:
		_append_loop_check_null<int32_t>(source, target);
		break;
	case TypeId::BIGINT:
		_append_loop_check_null<int64_t>(source, target);
		break;
	case TypeId::DECIMAL:
		_append_loop_check_null<double>(source, target);
		break;
	case TypeId::POINTER:
		_append_loop_check_null<uint64_t>(source, target);
		break;
	case TypeId::DATE:
		_append_loop_check_null<date_t>(source, target);
		break;
	case TypeId::VARCHAR:
		_append_loop_check_null<const char *>(source, target);
		break;
	default:
		throw NotImplementedException("Unimplemented type for copy");
	}
}

void VectorOperations::Copy(Vector &source, Vector &target, size_t offset) {
	if (source.type != target.type) {
		throw NotImplementedException("Copy types don't match!");
	}
	assert(offset <= source.count);
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
struct RegularCase {
	template <class T>
	static inline T Operation(Vector &result, bool condition, T left, T right) {
		return condition ? left : right;
	}
};

struct StringCase {
	static inline const char *Operation(Vector &result, bool condition,
	                                    const char *left, const char *right) {
		if (condition) {
			return left ? result.string_heap.AddString(left) : nullptr;
		} else {
			return right ? result.string_heap.AddString(right) : nullptr;
		}
	}
};

void VectorOperations::Case(Vector &check, Vector &res_true, Vector &res_false,
                            Vector &result) {
	result.count = max(max(check.count, res_false.count), res_true.count);
	if ((!check.IsConstant() && check.count != result.count) ||
	    (!res_true.IsConstant() && res_true.count != result.count) ||
	    (!res_false.IsConstant() && res_false.count != result.count)) {
		throw Exception("Vector lengths don't match in case!");
	}
	if (check.type != TypeId::BOOLEAN) {
		throw InvalidTypeException(check.type,
		                           "Case check has to be a boolean vector!");
	}
	if (result.type != res_true.type || result.type != res_false.type) {
		throw TypeMismatchException(
		    result.type,
		    (result.type != res_true.type ? res_true.type : res_false.type),
		    "Case types have to match!");
	}

	switch (result.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		_case_loop<int8_t, RegularCase>(check, res_true, res_false, result);
		break;
	case TypeId::SMALLINT:
		_case_loop<int16_t, RegularCase>(check, res_true, res_false, result);
		break;
	case TypeId::INTEGER:
		_case_loop<int32_t, RegularCase>(check, res_true, res_false, result);
		break;
	case TypeId::BIGINT:
		_case_loop<int64_t, RegularCase>(check, res_true, res_false, result);
		break;
	case TypeId::DECIMAL:
		_case_loop<double, RegularCase>(check, res_true, res_false, result);
		break;
	case TypeId::POINTER:
		_case_loop<uint64_t, RegularCase>(check, res_true, res_false, result);
		break;
	case TypeId::VARCHAR:
		_case_loop<const char *, StringCase>(check, res_true, res_false,
		                                     result);
		break;
	case TypeId::DATE:
		_case_loop<date_t, RegularCase>(check, res_true, res_false, result);
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
	auto ldata = (T *)left.data;
	auto rdata = (T *)result.data;
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
		throw TypeMismatchException(left.type, result.type,
		                            "Types of vectors do not match!");
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
