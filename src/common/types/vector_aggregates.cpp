
#include "common/exception.hpp"
#include "common/types/hash.hpp"
#include "common/types/operators.hpp"
#include "common/types/vector_operations.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Templated Looping Functions
//===--------------------------------------------------------------------===//
template <class T, class OP>
void _templated_unary_fold(Vector &left, T *result) {
	T *ldata = (T *)left.data;
	if (left.sel_vector) {
		*result = ldata[left.sel_vector[0]];
		for (size_t i = 1; i < left.count; i++) {
			*result = OP::Operation(*result, ldata[left.sel_vector[i]]);
		}
	} else {
		*result = ldata[0];
		for (size_t i = 1; i < left.count; i++) {
			*result = OP::Operation(*result, ldata[i]);
		}
	}
}

template <class OP> Value _generic_unary_fold_loop(Vector &left) {
	Value result;
	result.type = left.type;
	if (left.count == 0) {
		result.is_null = true;
		return result;
	}
	result.is_null = false;
	switch (left.type) {
	case TypeId::TINYINT:
		_templated_unary_fold<int8_t, OP>(left, &result.value_.tinyint);
		break;
	case TypeId::SMALLINT:
		_templated_unary_fold<int16_t, OP>(left, &result.value_.smallint);
		break;
	case TypeId::INTEGER:
		_templated_unary_fold<int32_t, OP>(left, &result.value_.integer);
		break;
	case TypeId::BIGINT:
		_templated_unary_fold<int64_t, OP>(left, &result.value_.bigint);
		break;
	case TypeId::DECIMAL:
		_templated_unary_fold<double, OP>(left, &result.value_.decimal);
		break;
	case TypeId::POINTER:
		_templated_unary_fold<uint64_t, OP>(left, &result.value_.pointer);
		break;
	case TypeId::DATE:
		_templated_unary_fold<date_t, OP>(left, &result.value_.date);
		break;
	case TypeId::VARCHAR:
		return Value();
	default:
		throw NotImplementedException("Unimplemented type");
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Aggregates
//===--------------------------------------------------------------------===//
Value VectorOperations::Sum(Vector &left) {
	return _generic_unary_fold_loop<operators::Addition>(left);
}

Value VectorOperations::Count(Vector &left) {
	return Value((int32_t)left.count);
}

Value VectorOperations::Average(Vector &left) {
	Value result;
	Value sum = VectorOperations::Sum(left);
	Value count = VectorOperations::Count(left);
	Value::Divide(sum, count, result);
	return result;
}

Value VectorOperations::Max(Vector &left) {
	return _generic_unary_fold_loop<operators::Max>(left);
}

Value VectorOperations::Min(Vector &left) {
	return _generic_unary_fold_loop<operators::Min>(left);
}
