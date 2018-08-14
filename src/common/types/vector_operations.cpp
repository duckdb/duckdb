
#include "common/types/vector_operations.hpp"
#include "common/exception.hpp"
#include "common/types/hash.hpp"
#include "common/types/operators.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Templated Looping Functions
//===--------------------------------------------------------------------===//
template <class T, class RES, class OP>
void _templated_unary_loop(Vector &left, Vector &result) {
	T *ldata = (T *)left.data;
	RES *result_data = (RES *)result.data;
	if (left.sel_vector) {
		for (size_t i = 0; i < left.count; i++) {
			result_data[i] = OP::Operation(ldata[left.sel_vector[i]]);
		}
	} else {
		for (size_t i = 0; i < left.count; i++) {
			result_data[i] = OP::Operation(ldata[i]);
		}
	}
	result.count = left.count;
}

template <class T, class RES, class OP, class EXEC>
void _templated_binary_loop_handling(Vector &left, Vector &right,
                                     Vector &result) {
	T *ldata = (T *)left.data;
	T *rdata = (T *)right.data;
	RES *result_data = (RES *)result.data;

	if (left.count == right.count) {
		if (left.sel_vector && right.sel_vector) {
			for (size_t i = 0; i < left.count; i++) {
				result_data[i] = EXEC::template Operation<T, T, RES, OP>(
				    ldata[left.sel_vector[i]], rdata[right.sel_vector[i]]);
			}
		} else if (left.sel_vector) {
			for (size_t i = 0; i < left.count; i++) {
				result_data[i] = EXEC::template Operation<T, T, RES, OP>(
				    ldata[left.sel_vector[i]], rdata[i]);
			}
		} else if (right.sel_vector) {
			for (size_t i = 0; i < left.count; i++) {
				result_data[i] = EXEC::template Operation<T, T, RES, OP>(
				    ldata[i], rdata[right.sel_vector[i]]);
			}
		} else {
			for (size_t i = 0; i < left.count; i++) {
				result_data[i] =
				    EXEC::template Operation<T, T, RES, OP>(ldata[i], rdata[i]);
			}
		}
		result.count = left.count;
	} else if (left.count == 1) {
		if (right.sel_vector) {
			for (size_t i = 0; i < right.count; i++) {
				result_data[i] = EXEC::template Operation<T, T, RES, OP>(
				    ldata[0], rdata[right.sel_vector[i]]);
			}
		} else {
			for (size_t i = 0; i < right.count; i++) {
				result_data[i] =
				    EXEC::template Operation<T, T, RES, OP>(ldata[0], rdata[i]);
			}
		}
		result.count = right.count;
	} else if (right.count == 1) {
		if (left.sel_vector) {
			for (size_t i = 0; i < left.count; i++) {
				result_data[i] = EXEC::template Operation<T, T, RES, OP>(
				    ldata[left.sel_vector[i]], rdata[0]);
			}
		} else {
			for (size_t i = 0; i < left.count; i++) {
				result_data[i] =
				    EXEC::template Operation<T, T, RES, OP>(ldata[i], rdata[0]);
			}
		}
		result.count = left.count;
	} else {
		throw Exception("Vector lengths don't match");
	}
}

template <class T, class RES, class OP>
void _templated_binary_loop(Vector &left, Vector &right, Vector &result,
                            bool can_have_null) {
	if (can_have_null) {
		_templated_binary_loop_handling<T, RES, OP,
		                                operators::ExecuteWithNullHandling>(
		    left, right, result);
	} else {
		_templated_binary_loop_handling<T, RES, OP,
		                                operators::ExecuteWithoutNullHandling>(
		    left, right, result);
	}
}

//===--------------------------------------------------------------------===//
// Type Switches
//===--------------------------------------------------------------------===//
template <class OP, class RES>
void _fixed_return_unary_loop(Vector &left, Vector &result) {
	switch (left.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		_templated_unary_loop<int8_t, RES, OP>(left, result);
		break;
	case TypeId::SMALLINT:
		_templated_unary_loop<int16_t, RES, OP>(left, result);
		break;
	case TypeId::INTEGER:
		_templated_unary_loop<int32_t, RES, OP>(left, result);
		break;
	case TypeId::BIGINT:
		_templated_unary_loop<int64_t, RES, OP>(left, result);
		break;
	case TypeId::DECIMAL:
		_templated_unary_loop<double, RES, OP>(left, result);
		break;
	case TypeId::POINTER:
		_templated_unary_loop<uint64_t, RES, OP>(left, result);
		break;
	case TypeId::VARCHAR:
		_templated_unary_loop<char *, RES, OP>(left, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

template <class OP>
void _same_return_unary_int_loop(Vector &left, Vector &result) {
	switch (left.type) {
	case TypeId::TINYINT:
		_templated_unary_loop<int8_t, int8_t, OP>(left, result);
		break;
	case TypeId::SMALLINT:
		_templated_unary_loop<int16_t, int16_t, OP>(left, result);
		break;
	case TypeId::INTEGER:
		_templated_unary_loop<int32_t, int32_t, OP>(left, result);
		break;
	case TypeId::BIGINT:
		_templated_unary_loop<int64_t, int64_t, OP>(left, result);
		break;
	case TypeId::DECIMAL:
		_templated_unary_loop<double, double, OP>(left, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

template <class OP>
void _generic_binary_loop(Vector &left, Vector &right, Vector &result,
                          bool can_have_null) {
	if (left.type != right.type) {
		throw NotImplementedException("Type cast not implemented here!");
	}
	switch (left.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		_templated_binary_loop<int8_t, int8_t, OP>(left, right, result,
		                                           can_have_null);
		break;
	case TypeId::SMALLINT:
		_templated_binary_loop<int16_t, int16_t, OP>(left, right, result,
		                                             can_have_null);
		break;
	case TypeId::INTEGER:
		_templated_binary_loop<int32_t, int32_t, OP>(left, right, result,
		                                             can_have_null);
		break;
	case TypeId::BIGINT:
		_templated_binary_loop<int64_t, int64_t, OP>(left, right, result,
		                                             can_have_null);
		break;
	case TypeId::DECIMAL:
		_templated_binary_loop<double, double, OP>(left, right, result,
		                                           can_have_null);
		break;
	case TypeId::POINTER:
		_templated_binary_loop<uint64_t, uint64_t, OP>(left, right, result,
		                                               can_have_null);
		break;
	case TypeId::DATE:
		_templated_binary_loop<date_t, date_t, OP>(left, right, result,
		                                           can_have_null);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

template <class OP, class RES>
void _fixed_return_binary_loop(Vector &left, Vector &right, Vector &result,
                               bool can_have_null) {
	if (left.type != right.type) {
		throw NotImplementedException("Type cast not implemented here!");
	}
	switch (left.type) {
	case TypeId::TINYINT:
		_templated_binary_loop<int8_t, RES, OP>(left, right, result,
		                                        can_have_null);
		break;
	case TypeId::SMALLINT:
		_templated_binary_loop<int16_t, RES, OP>(left, right, result,
		                                         can_have_null);
		break;
	case TypeId::INTEGER:
		_templated_binary_loop<int32_t, RES, OP>(left, right, result,
		                                         can_have_null);
		break;
	case TypeId::BIGINT:
		_templated_binary_loop<int64_t, RES, OP>(left, right, result,
		                                         can_have_null);
		break;
	case TypeId::DECIMAL:
		_templated_binary_loop<double, RES, OP>(left, right, result,
		                                        can_have_null);
		break;
	case TypeId::POINTER:
		_templated_binary_loop<uint64_t, RES, OP>(left, right, result,
		                                          can_have_null);
		break;
	case TypeId::DATE:
		_templated_binary_loop<date_t, RES, OP>(left, right, result,
		                                        can_have_null);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

//===--------------------------------------------------------------------===//
// Numeric Operations
//===--------------------------------------------------------------------===//
void VectorOperations::Add(Vector &left, Vector &right, Vector &result,
                           bool can_have_null) {
	_generic_binary_loop<operators::Addition>(left, right, result,
	                                          can_have_null);
}

void VectorOperations::Subtract(Vector &left, Vector &right, Vector &result,
                                bool can_have_null) {
	_generic_binary_loop<operators::Subtraction>(left, right, result,
	                                             can_have_null);
}

void VectorOperations::Multiply(Vector &left, Vector &right, Vector &result,
                                bool can_have_null) {
	_generic_binary_loop<operators::Multiplication>(left, right, result,
	                                                can_have_null);
}

void VectorOperations::Divide(Vector &left, Vector &right, Vector &result,
                              bool can_have_null) {
	_generic_binary_loop<operators::Division>(left, right, result,
	                                          can_have_null);
}

void VectorOperations::Modulo(Vector &left, Vector &right, Vector &result,
                              bool can_have_null) {
	_generic_binary_loop<operators::Modulo>(left, right, result, can_have_null);
}

void VectorOperations::Abs(Vector &left, Vector &result) {
	_same_return_unary_int_loop<operators::Abs>(left, result);
}

void VectorOperations::Not(Vector &left, Vector &result) {
	if (left.type != TypeId::BOOLEAN) {
		throw Exception("NOT() needs a boolean input");
	}
	_fixed_return_unary_loop<operators::Not, int8_t>(left, result);
}

void VectorOperations::NotNull(Vector &left, Vector &result) {
	_fixed_return_unary_loop<operators::NotNull, bool>(left, result);
}

//===--------------------------------------------------------------------===//
// Right-Hand Side Numeric Helpers
//===--------------------------------------------------------------------===//
template <VectorOperations::vector_function OP>
static void _numeric_operator_right(Vector &left, int64_t right, Vector &result,
                                    bool can_have_null) {
	Vector constant(Value::Numeric(left.type, right));
	OP(left, constant, result, can_have_null);
}

void VectorOperations::Add(Vector &left, int64_t right, Vector &result,
                           bool can_have_null) {
	_numeric_operator_right<VectorOperations::Add>(left, right, result,
	                                               can_have_null);
}

void VectorOperations::Subtract(Vector &left, int64_t right, Vector &result,
                                bool can_have_null) {
	_numeric_operator_right<VectorOperations::Subtract>(left, right, result,
	                                                    can_have_null);
}

void VectorOperations::Multiply(Vector &left, int64_t right, Vector &result,
                                bool can_have_null) {
	_numeric_operator_right<VectorOperations::Multiply>(left, right, result,
	                                                    can_have_null);
}

void VectorOperations::Divide(Vector &left, int64_t right, Vector &result,
                              bool can_have_null) {
	_numeric_operator_right<VectorOperations::Divide>(left, right, result,
	                                                  can_have_null);
}

void VectorOperations::Modulo(Vector &left, int64_t right, Vector &result,
                              bool can_have_null) {
	_numeric_operator_right<VectorOperations::Modulo>(left, right, result,
	                                                  can_have_null);
}

//===--------------------------------------------------------------------===//
// Left-Hand Side Numeric Helpers
//===--------------------------------------------------------------------===//
template <VectorOperations::vector_function OP>
static void _numeric_operator_left(int64_t left, Vector &right, Vector &result,
                                   bool can_have_null) {
	Vector constant(Value::Numeric(right.type, left));
	OP(constant, right, result, can_have_null);
}

void VectorOperations::Add(int64_t left, Vector &right, Vector &result,
                           bool can_have_null) {
	_numeric_operator_left<VectorOperations::Add>(left, right, result,
	                                              can_have_null);
}

void VectorOperations::Subtract(int64_t left, Vector &right, Vector &result,
                                bool can_have_null) {
	_numeric_operator_left<VectorOperations::Subtract>(left, right, result,
	                                                   can_have_null);
}

void VectorOperations::Multiply(int64_t left, Vector &right, Vector &result,
                                bool can_have_null) {
	_numeric_operator_left<VectorOperations::Multiply>(left, right, result,
	                                                   can_have_null);
}

void VectorOperations::Divide(int64_t left, Vector &right, Vector &result,
                              bool can_have_null) {
	_numeric_operator_left<VectorOperations::Divide>(left, right, result,
	                                                 can_have_null);
}

void VectorOperations::Modulo(int64_t left, Vector &right, Vector &result,
                              bool can_have_null) {
	_numeric_operator_left<VectorOperations::Modulo>(left, right, result,
	                                                 can_have_null);
}

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//
void VectorOperations::Equals(Vector &left, Vector &right, Vector &result,
                              bool can_have_null) {
	_fixed_return_binary_loop<operators::Equals, bool>(left, right, result,
	                                                   can_have_null);
}

void VectorOperations::NotEquals(Vector &left, Vector &right, Vector &result,
                                 bool can_have_null) {
	_fixed_return_binary_loop<operators::NotEquals, bool>(left, right, result,
	                                                      can_have_null);
}

void VectorOperations::GreaterThan(Vector &left, Vector &right, Vector &result,
                                   bool can_have_null) {
	_fixed_return_binary_loop<operators::GreaterThan, bool>(left, right, result,
	                                                        can_have_null);
}

void VectorOperations::GreaterThanEquals(Vector &left, Vector &right,
                                         Vector &result, bool can_have_null) {
	_fixed_return_binary_loop<operators::GreaterThanEquals, bool>(
	    left, right, result, can_have_null);
}

void VectorOperations::LessThan(Vector &left, Vector &right, Vector &result,
                                bool can_have_null) {
	_fixed_return_binary_loop<operators::LessThan, bool>(left, right, result,
	                                                     can_have_null);
}

void VectorOperations::LessThanEquals(Vector &left, Vector &right,
                                      Vector &result, bool can_have_null) {
	_fixed_return_binary_loop<operators::LessThanEquals, bool>(
	    left, right, result, can_have_null);
}

void VectorOperations::And(Vector &left, Vector &right, Vector &result,
                           bool can_have_null) {
	if (left.type != TypeId::BOOLEAN || right.type != TypeId::BOOLEAN) {
		throw NotImplementedException("FIXME cast");
	}

	if (left.count == right.count) {
		if (can_have_null) {
			// null handling happens inside operator
			_templated_binary_loop<
			    int8_t, int8_t, operators::AndNull>(left, right, result, false);
		} else {
			_templated_binary_loop<
			    bool, bool, operators::And>(left, right, result, false);
		}

	} else {
		throw Exception("Vector lengths don't match");
	}
}

void VectorOperations::Or(Vector &left, Vector &right, Vector &result,
                          bool can_have_null) {
	if (left.type != TypeId::BOOLEAN || right.type != TypeId::BOOLEAN) {
		throw NotImplementedException("FIXME cast");
	}

	if (left.count == right.count) {

		if (can_have_null) {
			// null handling happens inside operator
			_templated_binary_loop_handling<
			    int8_t, int8_t, operators::OrNull,
			    operators::ExecuteWithoutNullHandling>(left, right, result);
		} else {
			_templated_binary_loop_handling<
			    bool, bool, operators::Or,
			    operators::ExecuteWithoutNullHandling>(left, right, result);
		}

	} else {
		throw Exception("Vector lengths don't match");
	}
}
//===--------------------------------------------------------------------===//
// Set all elements of a vector to the constant value
//===--------------------------------------------------------------------===//
void VectorOperations::Set(Vector &result, Value value) {
	if (value.type != result.type) {
		value = value.CastAs(result.type);
	}

	Vector left(value);
	_generic_binary_loop<operators::PickLeft>(left, result, result, false);
}

//===--------------------------------------------------------------------===//
// Hash functions
//===--------------------------------------------------------------------===//

void VectorOperations::Hash(Vector &left, Vector &result) {
	_fixed_return_unary_loop<operators::Hash, int32_t>(left, result);
}

void VectorOperations::CombineHash(Vector &left, Vector &right,
                                   Vector &result) {
	if (left.type != TypeId::INTEGER) {
		throw NotImplementedException(
		    "Left argument must be 32-bit integer hash");
	}
	VectorOperations::Hash(right, result);
	_templated_binary_loop<int32_t, int32_t, operators::XOR>(left, result,
	                                                         result, false);
}
