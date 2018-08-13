
#include "common/exception.hpp"
#include "common/types/hash.hpp"
#include "common/types/operators.hpp"
#include "common/types/vector_operations.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Templated Looping Functions
//===--------------------------------------------------------------------===//
template <class T, class RES, class OP, class EXEC>
void _templated_unary_fold_handling(Vector &left, RES *result) {
	T *ldata = (T *)left.data;
	if (left.sel_vector) {
		for (size_t i = 0; i < left.count; i++) {
			*result = EXEC::template Operation<T, RES, RES, OP>(
			    ldata[left.sel_vector[i]], *result);
		}
	} else {
		for (size_t i = 0; i < left.count; i++) {
			*result =
			    EXEC::template Operation<T, RES, RES, OP>(ldata[i], *result);
		}
	}
}

template <class T, class RES, class OP>
void _templated_unary_fold(Vector &left, RES *result, bool ignore_null) {
	if (ignore_null) {
		_templated_unary_fold_handling<T, RES, OP,
		                               operators::ExecuteIgnoreLeftNull>(
		    left, result);
	} else {
		_templated_unary_fold_handling<T, RES, OP,
		                               operators::ExecuteWithoutNullHandling>(
		    left, result);
	}
}

template <class T, class OP>
void _templated_unary_fold_single_type(Vector &left, T *result,
                                       bool ignore_null) {
	_templated_unary_fold<T, T, OP>(left, result, ignore_null);
}

template <class OP>
void _generic_unary_fold_loop(Vector &left, Value &result, bool ignore_null) {
	switch (left.type) {
	case TypeId::TINYINT:
		_templated_unary_fold_single_type<int8_t, OP>(
		    left, &result.value_.tinyint, ignore_null);
		break;
	case TypeId::SMALLINT:
		_templated_unary_fold_single_type<int16_t, OP>(
		    left, &result.value_.smallint, ignore_null);
		break;
	case TypeId::INTEGER:
		_templated_unary_fold_single_type<int32_t, OP>(
		    left, &result.value_.integer, ignore_null);
		break;
	case TypeId::BIGINT:
		_templated_unary_fold_single_type<int64_t, OP>(
		    left, &result.value_.bigint, ignore_null);
		break;
	case TypeId::DECIMAL:
		_templated_unary_fold_single_type<double, OP>(
		    left, &result.value_.decimal, ignore_null);
		break;
	case TypeId::POINTER:
		_templated_unary_fold_single_type<uint64_t, OP>(
		    left, &result.value_.pointer, ignore_null);
		break;
	case TypeId::DATE:
		_templated_unary_fold_single_type<date_t, OP>(left, &result.value_.date,
		                                              ignore_null);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

template <class RES, class OP>
void _fixed_return_unary_fold_loop(Vector &left, RES *result) {
	switch (left.type) {
	case TypeId::TINYINT:
		_templated_unary_fold<int8_t, RES, OP>(left, result, false);
		break;
	case TypeId::SMALLINT:
		_templated_unary_fold<int16_t, RES, OP>(left, result, false);
		break;
	case TypeId::INTEGER:
		_templated_unary_fold<int32_t, RES, OP>(left, result, false);
		break;
	case TypeId::BIGINT:
		_templated_unary_fold<int64_t, RES, OP>(left, result, false);
		break;
	case TypeId::DECIMAL:
		_templated_unary_fold<double, RES, OP>(left, result, false);
		break;
	case TypeId::POINTER:
		_templated_unary_fold<uint64_t, RES, OP>(left, result, false);
		break;
	case TypeId::DATE:
		_templated_unary_fold<date_t, RES, OP>(left, result, false);
		break;
	case TypeId::VARCHAR:
		_templated_unary_fold<const char *, RES, OP>(left, result, false);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

//===--------------------------------------------------------------------===//
// Aggregates
//===--------------------------------------------------------------------===//
Value VectorOperations::Sum(Vector &left, bool can_have_null) {
	if (left.count == 0 || !TypeIsNumeric(left.type)) {
		return Value();
	}
	Value result = Value::Numeric(left.type, 0);
	_generic_unary_fold_loop<operators::Addition>(left, result, can_have_null);
	// FIXME: null check?
	return result;
}

Value VectorOperations::Count(Vector &left, bool can_have_null) {
	Value result = Value::Numeric(left.type, 0);
	_generic_unary_fold_loop<operators::AddOne>(left, result, can_have_null);
	// FIXME: null check?
	return result;
	// return Value((int32_t)left.count);
}

Value VectorOperations::Average(Vector &left, bool can_have_null) {
	Value result;
	Value sum = VectorOperations::Sum(left, can_have_null);
	Value count = VectorOperations::Count(left, can_have_null);
	Value::Divide(sum, count, result);
	return result;
}

Value VectorOperations::Max(Vector &left, bool can_have_null) {
	if (left.count == 0 || !TypeIsNumeric(left.type)) {
		return Value();
	}
	Value minimum_value = Value::MinimumValue(left.type);
	Value result = minimum_value;
	_generic_unary_fold_loop<operators::Max>(left, result, can_have_null);
	result.is_null =
	    Value::Equals(result, minimum_value); // check if any tuples qualified
	return result;
}

Value VectorOperations::Min(Vector &left, bool can_have_null) {
	if (left.count == 0 || !TypeIsNumeric(left.type)) {
		return Value();
	}
	Value maximum_value = Value::MaximumValue(left.type);
	Value result = maximum_value;
	_generic_unary_fold_loop<operators::Min>(left, result, can_have_null);
	result.is_null =
	    Value::Equals(result, maximum_value); // check if any tuples qualified
	return result;
}

bool VectorOperations::HasNull(Vector &left, bool can_have_null) {
	if (!can_have_null) {
		return false;
	}
	bool has_null = false;
	_fixed_return_unary_fold_loop<bool, operators::NullCheck>(left, &has_null);
	return has_null;
}

Value VectorOperations::MaximumStringLength(Vector &left, bool can_have_null) {
	if (left.type != TypeId::VARCHAR) {
		throw Exception(
		    "String length can only be computed for char array columns!");
	}
	auto result = Value::POINTER(0);
	if (left.count == 0) {
		return result;
	}
	_templated_unary_fold<const char *, uint64_t,
	                      operators::MaximumStringLength>(
	    left, &result.value_.pointer, can_have_null);
	return result;
}
