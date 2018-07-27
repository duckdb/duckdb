
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
void _templated_unary_fold(Vector &left, Vector &result) {
	T *ldata = (T *)left.data;
	RES *result_data = (RES *)result.data;
	result_data[0] = 0;
	if (left.sel_vector) {
		for (size_t i = 0; i < left.count; i++) {
			result_data[0] =
			    OP::Operation(result_data[0], ldata[left.sel_vector[i]]);
		}
	} else {
		for (size_t i = 0; i < left.count; i++) {
			result_data[0] = OP::Operation(result_data[0], ldata[i]);
		}
	}
	result.count = 1;
}

template <class OP>
void _generic_unary_fold_loop(Vector &left, Vector &result) {
	switch (left.type) {
	case TypeId::TINYINT:
		_templated_unary_fold<int8_t, int8_t, OP>(left, result);
		break;
	case TypeId::SMALLINT:
		_templated_unary_fold<int16_t, int16_t, OP>(left, result);
		break;
	case TypeId::INTEGER:
		_templated_unary_fold<int32_t, int32_t, OP>(left, result);
		break;
	case TypeId::BIGINT:
		_templated_unary_fold<int64_t, int64_t, OP>(left, result);
		break;
	case TypeId::DECIMAL:
		_templated_unary_fold<double, double, OP>(left, result);
		break;
	case TypeId::POINTER:
		_templated_unary_fold<uint64_t, uint64_t, OP>(left, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

//===--------------------------------------------------------------------===//
// Aggregates
//===--------------------------------------------------------------------===//
void VectorOperations::Sum(Vector &left, Vector &result) {
	_generic_unary_fold_loop<operators::Addition>(left, result);
}

void VectorOperations::Count(Vector &left, Vector &result) {
	Vector count_vector(Value((int32_t)left.count));
	VectorOperations::Add(result, count_vector, result);
}

void VectorOperations::Average(Vector &left, Vector &result) {
	Vector count_vector(Value((int32_t)left.count));
	VectorOperations::Sum(left, result);
	VectorOperations::Divide(result, count_vector, result);
}

void VectorOperations::Max(Vector &left, Vector &result) {
	_generic_unary_fold_loop<operators::Max>(left, result);
}

void VectorOperations::Min(Vector &left, Vector &result) {
	_generic_unary_fold_loop<operators::Min>(left, result);
}
