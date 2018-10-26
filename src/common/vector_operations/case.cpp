//===--------------------------------------------------------------------===//
// case.cpp
// Description: This file contains the implementation of the CASE statement
//===--------------------------------------------------------------------===//

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class T, class OP>
static void case_loop(Vector &check, Vector &res_true, Vector &res_false,
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
		result.nullmask[i] = branch ? res_true.nullmask[true_index]
		                            : res_false.nullmask[false_index];
		res[i] = OP::Operation(result, branch, true_data[true_index],
		                       false_data[false_index], i);
	});
}

//===--------------------------------------------------------------------===//
// Case statement (if, else, then)
//===--------------------------------------------------------------------===//
struct RegularCase {
	template <class T>
	static inline T Operation(Vector &result, bool condition, T left, T right,
	                          size_t i) {
		return condition ? left : right;
	}
};

struct StringCase {
	static inline const char *Operation(Vector &result, bool condition,
	                                    const char *left, const char *right,
	                                    size_t i) {
		if (!result.nullmask[i]) {
			return condition ? result.string_heap.AddString(left)
			                 : result.string_heap.AddString(right);
		} else {
			return nullptr;
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
		case_loop<int8_t, RegularCase>(check, res_true, res_false, result);
		break;
	case TypeId::SMALLINT:
		case_loop<int16_t, RegularCase>(check, res_true, res_false, result);
		break;
	case TypeId::INTEGER:
		case_loop<int32_t, RegularCase>(check, res_true, res_false, result);
		break;
	case TypeId::BIGINT:
		case_loop<int64_t, RegularCase>(check, res_true, res_false, result);
		break;
	case TypeId::DECIMAL:
		case_loop<double, RegularCase>(check, res_true, res_false, result);
		break;
	case TypeId::POINTER:
		case_loop<uint64_t, RegularCase>(check, res_true, res_false, result);
		break;
	case TypeId::VARCHAR:
		case_loop<const char *, StringCase>(check, res_true, res_false, result);
		break;
	case TypeId::DATE:
		case_loop<date_t, RegularCase>(check, res_true, res_false, result);
		break;
	case TypeId::TIMESTAMP:
		case_loop<timestamp_t, RegularCase>(check, res_true, res_false, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type for case expression");
	}
	result.count = check.count;
}
