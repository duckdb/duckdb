//===--------------------------------------------------------------------===//
// case.cpp
// Description: This file contains the implementation of the CASE statement
//===--------------------------------------------------------------------===//

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class T, class OP> static void case_loop(Vector &check, Vector &res_true, Vector &res_false, Vector &result) {
	auto cond = (bool *)check.data;
	auto true_data = (T *)res_true.data;
	auto false_data = (T *)res_false.data;
	auto res = (T *)result.data;
	VectorOperations::TernaryExec(
	    check, res_true, res_false, result,
	    [&](index_t check_index, index_t true_index, index_t false_index, index_t i) {
		    bool branch = (cond[check_index] && !check.nullmask[check_index]);
		    result.nullmask[i] = branch ? res_true.nullmask[true_index] : res_false.nullmask[false_index];
		    res[i] = OP::Operation(result, branch, true_data[true_index], false_data[false_index], i);
	    });
}

//===--------------------------------------------------------------------===//
// Case statement (if, else, then)
//===--------------------------------------------------------------------===//
struct RegularCase {
	template <class T> static inline T Operation(Vector &result, bool condition, T left, T right, index_t i) {
		return condition ? left : right;
	}
};

struct StringCase {
	static inline const char *Operation(Vector &result, bool condition, const char *left, const char *right,
	                                    index_t i) {
		if (!result.nullmask[i]) {
			return condition ? result.string_heap.AddString(left) : result.string_heap.AddString(right);
		} else {
			return nullptr;
		}
	}
};

void VectorOperations::Case(Vector &check, Vector &res_true, Vector &res_false, Vector &result) {
	if (check.type != TypeId::BOOLEAN) {
		throw InvalidTypeException(check.type, "Case check has to be a boolean vector!");
	}
	if (result.type != res_true.type || result.type != res_false.type) {
		throw TypeMismatchException(result.type, (result.type != res_true.type ? res_true.type : res_false.type),
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
	case TypeId::FLOAT:
		case_loop<float, RegularCase>(check, res_true, res_false, result);
		break;
	case TypeId::DOUBLE:
		case_loop<double, RegularCase>(check, res_true, res_false, result);
		break;
	case TypeId::POINTER:
		case_loop<uint64_t, RegularCase>(check, res_true, res_false, result);
		break;
	case TypeId::VARCHAR:
		case_loop<const char *, StringCase>(check, res_true, res_false, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type for case expression");
	}
}
