
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
void _templated_unary_fold(Vector &left, RES *result) {
	T *ldata = (T *)left.data;
	if (left.sel_vector) {
		for (size_t i = 0; i < left.count; i++) {
			*result = OP::Operation(*result, ldata[left.sel_vector[i]]);
		}
	} else {
		for (size_t i = 0; i < left.count; i++) {
			*result = OP::Operation(*result, ldata[i]);
		}
	}
}

template <class OP> Value _generic_unary_fold_loop(Vector &left, Value &result) {
	switch (left.type) {
	case TypeId::TINYINT:
		_templated_unary_fold<int8_t, int8_t, OP>(left, &result.value_.tinyint);
		break;
	case TypeId::SMALLINT:
		_templated_unary_fold<int16_t, int16_t, OP>(left, &result.value_.smallint);
		break;
	case TypeId::INTEGER:
		_templated_unary_fold<int32_t, int32_t, OP>(left, &result.value_.integer);
		break;
	case TypeId::BIGINT:
		_templated_unary_fold<int64_t, int64_t, OP>(left, &result.value_.bigint);
		break;
	case TypeId::DECIMAL:
		_templated_unary_fold<double, double, OP>(left, &result.value_.decimal);
		break;
	case TypeId::POINTER:
		_templated_unary_fold<uint64_t, uint64_t, OP>(left, &result.value_.pointer);
		break;
	case TypeId::DATE:
		_templated_unary_fold<date_t, date_t, OP>(left, &result.value_.date);
		break;
	case TypeId::VARCHAR:
		return Value();
	default:
		throw NotImplementedException("Unimplemented type");
	}
	return result;
}

template <class RES, class OP> Value _fixed_return_unary_fold_loop(Vector &left, RES *result) {
	switch (left.type) {
	case TypeId::TINYINT:
		_templated_unary_fold<int8_t, RES, OP>(left, result);
		break;
	case TypeId::SMALLINT:
		_templated_unary_fold<int16_t, RES, OP>(left, result);
		break;
	case TypeId::INTEGER:
		_templated_unary_fold<int32_t, RES, OP>(left, result);
		break;
	case TypeId::BIGINT:
		_templated_unary_fold<int64_t, RES, OP>(left, result);
		break;
	case TypeId::DECIMAL:
		_templated_unary_fold<double, RES, OP>(left, result);
		break;
	case TypeId::POINTER:
		_templated_unary_fold<uint64_t, RES, OP>(left, result);
		break;
	case TypeId::DATE:
		_templated_unary_fold<date_t, RES, OP>(left, result);
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
	if (left.count == 0 || !TypeIsNumeric(left.type)) {
		return Value();
	}
	Value result = Value::NumericValue(left.type, 0);
	return _generic_unary_fold_loop<operators::Addition>(left, result);
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
	if (left.count == 0 || !TypeIsNumeric(left.type)) {
		return Value();
	}
	Value result = Value::MinimumValue(left.type);
	return _generic_unary_fold_loop<operators::Max>(left, result);
}

Value VectorOperations::Min(Vector &left) {
	if (left.count == 0 || !TypeIsNumeric(left.type)) {
		return Value();
	}
	Value result = Value::MaximumValue(left.type);
	return _generic_unary_fold_loop<operators::Min>(left, result);
}

bool VectorOperations::HasNull(Vector &left) {
	bool has_null = false;
	_fixed_return_unary_fold_loop<bool, operators::NullCheck>(left, &has_null);
	return has_null;
}

Value VectorOperations::MaximumStringLength(Vector &left) {
	if (left.type != TypeId::VARCHAR) {
		throw Exception("String length can only be computed for char array columns!");
	}
	auto result = Value::NumericValue(TypeId::POINTER, 0);
	if (left.count == 0) {
		return result;
	}
	_templated_unary_fold<const char*, uint64_t, operators::MaximumStringLength>(left, &result.value_.pointer);
	return result;
}
