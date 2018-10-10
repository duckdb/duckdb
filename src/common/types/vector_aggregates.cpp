
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
			if (!left.nullmask[left.sel_vector[i]]) {
				*result = OP::Operation(ldata[left.sel_vector[i]], *result);
			}
		}
	} else {
		for (size_t i = 0; i < left.count; i++) {
			if (!left.nullmask[i]) {
				*result = OP::Operation(ldata[i], *result);
			}
		}
	}
}

template <class T, class OP>
void _templated_unary_fold_single_type(Vector &left, T *result) {
	_templated_unary_fold<T, T, OP>(left, result);
}

template <class OP> void _generic_unary_fold_loop(Vector &left, Value &result) {
	switch (left.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		_templated_unary_fold_single_type<int8_t, OP>(left,
		                                              &result.value_.tinyint);
		break;
	case TypeId::SMALLINT:
		_templated_unary_fold_single_type<int16_t, OP>(left,
		                                               &result.value_.smallint);
		break;
	case TypeId::INTEGER:
		_templated_unary_fold_single_type<int32_t, OP>(left,
		                                               &result.value_.integer);
		break;
	case TypeId::BIGINT:
		_templated_unary_fold_single_type<int64_t, OP>(left,
		                                               &result.value_.bigint);
		break;
	case TypeId::DECIMAL:
		_templated_unary_fold_single_type<double, OP>(left,
		                                              &result.value_.decimal);
		break;
	case TypeId::POINTER:
		_templated_unary_fold_single_type<uint64_t, OP>(left,
		                                                &result.value_.pointer);
		break;
	case TypeId::DATE:
		_templated_unary_fold_single_type<date_t, OP>(left,
		                                              &result.value_.date);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

template <class RES, class OP>
void _fixed_return_unary_fold_loop(Vector &left, RES *result) {
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
		_templated_unary_fold<const char *, RES, OP>(left, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

//===--------------------------------------------------------------------===//
// Aggregates
//===--------------------------------------------------------------------===//
Value VectorOperations::Sum(Vector &left) {
	if (left.count == 0 || !TypeIsNumeric(left.type)) {
		return Value();
	}

	Value result = Value::Numeric(left.type, 0);

	// check if all are NULL, because then the result is NULL and not 0
	Vector is_null;
	is_null.Initialize(TypeId::BOOLEAN);
	VectorOperations::IsNull(left, is_null);

	if (Value::Equals(VectorOperations::AllTrue(is_null), Value(true))) {
		result.is_null = true;
	} else {
		_generic_unary_fold_loop<operators::Addition>(left, result);
	}
	return result;
}

Value VectorOperations::Count(Vector &left) {
	Value result = Value::Numeric(left.type, 0);
	_generic_unary_fold_loop<operators::AddOne>(left, result);
	return result;
}

Value VectorOperations::Max(Vector &left) {
	if (left.count == 0 || !TypeIsNumeric(left.type)) {
		return Value();
	}
	Value minimum_value = Value::MinimumValue(left.type);
	Value result = minimum_value;
	_generic_unary_fold_loop<operators::Max>(left, result);
	result.is_null =
	    Value::Equals(result, minimum_value); // check if any tuples qualified
	return result;
}

Value VectorOperations::Min(Vector &left) {
	if (left.count == 0 || !TypeIsNumeric(left.type)) {
		return Value();
	}
	Value maximum_value = Value::MaximumValue(left.type);
	Value result = maximum_value;
	_generic_unary_fold_loop<operators::Min>(left, result);
	result.is_null =
	    Value::Equals(result, maximum_value); // check if any tuples qualified
	return result;
}

Value VectorOperations::AnyTrue(Vector &left) {
	if (left.type != TypeId::BOOLEAN) {
		throw InvalidTypeException(
		    left.type, "AnyTrue can only be computed for boolean columns!");
	}
	if (left.count == 0) {
		return Value(false);
	}

	Value result = Value(false);
	_generic_unary_fold_loop<operators::AnyTrue>(left, result);
	return result;
}

Value VectorOperations::AllTrue(Vector &left) {
	if (left.type != TypeId::BOOLEAN) {
		throw InvalidTypeException(
		    left.type, "AllTrue can only be computed for boolean columns!");
	}
	if (left.count == 0) {
		return Value(false);
	}

	Value result = Value(true);
	_generic_unary_fold_loop<operators::AllTrue>(left, result);
	return result;
}

bool VectorOperations::Contains(Vector &vector, Value &value) {
	if (vector.count == 0) {
		return false;
	}
	// first perform a comparison using Equals
	// then return TRUE if any of the comparisons are true
	// FIXME: this can be done more efficiently in one loop
	Vector right(value.CastAs(vector.type));
	Vector comparison_result(TypeId::BOOLEAN, true, false);
	VectorOperations::Equals(vector, right, comparison_result);
	auto result = VectorOperations::AnyTrue(comparison_result);
	assert(result.type == TypeId::BOOLEAN);
	return result.value_.boolean;
}

bool VectorOperations::HasNull(Vector &left) { return left.nullmask.any(); }

Value VectorOperations::MaximumStringLength(Vector &left) {
	if (left.type != TypeId::VARCHAR) {
		throw InvalidTypeException(
		    left.type,
		    "String length can only be computed for char array columns!");
	}
	auto result = Value::POINTER(0);
	if (left.count == 0) {
		return result;
	}
	_templated_unary_fold<const char *, uint64_t,
	                      operators::MaximumStringLength>(
	    left, &result.value_.pointer);
	return result;
}
