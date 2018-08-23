
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
			result_data[left.sel_vector[i]] =
			    OP::Operation(ldata[left.sel_vector[i]]);
		}
	} else {
		for (size_t i = 0; i < left.count; i++) {
			result_data[i] = OP::Operation(ldata[i]);
		}
	}
	result.nullmask = left.nullmask;
	result.sel_vector = left.sel_vector;
	result.count = left.count;
}

template <class T, class RES, class OP>
void _templated_unary_loop_null(Vector &left, Vector &result) {
	T *ldata = (T *)left.data;
	RES *result_data = (RES *)result.data;
	if (left.sel_vector) {
		for (size_t i = 0; i < left.count; i++) {
			result_data[left.sel_vector[i]] = OP::Operation(
			    ldata[left.sel_vector[i]], left.nullmask[left.sel_vector[i]]);
		}
	} else {
		for (size_t i = 0; i < left.count; i++) {
			result_data[i] = OP::Operation(ldata[i], left.nullmask[i]);
		}
	}
	result.nullmask.reset();
	result.sel_vector = left.sel_vector;
	result.count = left.count;
}

template <class T, class RES, class OP>
void _templated_binary_loop(Vector &left, Vector &right, Vector &result) {
	T *ldata = (T *)left.data;
	T *rdata = (T *)right.data;
	RES *result_data = (RES *)result.data;

	if (left.count == 1 && !left.sel_vector) {
		if (left.nullmask[0]) {
			// left side is constant NULL, set everything to NULL
			result.nullmask.set();
		} else {
			// left side is normal constant, use right nullmask and do
			// computation
			T constant = ldata[0];
			result.nullmask = right.nullmask;
			if (right.sel_vector) {
				for (size_t i = 0; i < right.count; i++) {
					result_data[right.sel_vector[i]] =
					    OP::Operation(constant, rdata[right.sel_vector[i]]);
				}
			} else {
				for (size_t i = 0; i < right.count; i++) {
					result_data[i] = OP::Operation(constant, rdata[i]);
				}
			}
		}
		result.sel_vector = right.sel_vector;
		result.count = right.count;
	} else if (right.count == 1 && !right.sel_vector) {
		if (right.nullmask[0]) {
			// right side is constant NULL, set everything to NULL
			result.nullmask.set();
		} else {
			// right side is normal constant, use left nullmask and do
			// computation
			T constant = rdata[0];
			result.nullmask = left.nullmask;
			if (left.sel_vector) {
				for (size_t i = 0; i < left.count; i++) {
					result_data[left.sel_vector[i]] =
					    OP::Operation(ldata[left.sel_vector[i]], constant);
				}
			} else {
				for (size_t i = 0; i < left.count; i++) {
					result_data[i] = OP::Operation(ldata[i], constant);
				}
			}
		}
		result.sel_vector = left.sel_vector;
		result.count = left.count;
	} else if (left.count == right.count) {
		result.nullmask = left.nullmask | right.nullmask;
		if (left.sel_vector) {
			assert(right.sel_vector);
			for (size_t i = 0; i < left.count; i++) {
				assert(left.sel_vector[i] == right.sel_vector[i]);
				result_data[left.sel_vector[i]] = OP::Operation(
				    ldata[left.sel_vector[i]], rdata[left.sel_vector[i]]);
			}
		} else {
			assert(!right.sel_vector);
			for (size_t i = 0; i < left.count; i++) {
				result_data[i] = OP::Operation(ldata[i], rdata[i]);
			}
		}
		result.sel_vector = left.sel_vector;
		result.count = left.count;
	} else {
		throw Exception("Vector lengths don't match");
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

template <class OP, class RES>
void _fixed_return_unary_loop_null(Vector &left, Vector &result) {
	switch (left.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		_templated_unary_loop_null<int8_t, RES, OP>(left, result);
		break;
	case TypeId::SMALLINT:
		_templated_unary_loop_null<int16_t, RES, OP>(left, result);
		break;
	case TypeId::INTEGER:
		_templated_unary_loop_null<int32_t, RES, OP>(left, result);
		break;
	case TypeId::BIGINT:
		_templated_unary_loop_null<int64_t, RES, OP>(left, result);
		break;
	case TypeId::DECIMAL:
		_templated_unary_loop_null<double, RES, OP>(left, result);
		break;
	case TypeId::POINTER:
		_templated_unary_loop_null<uint64_t, RES, OP>(left, result);
		break;
	case TypeId::VARCHAR:
		_templated_unary_loop_null<char *, RES, OP>(left, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

template <class OP> void _generic_unary_loop(Vector &left, Vector &result) {
	switch (left.type) {
	case TypeId::BOOLEAN:
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
void _generic_binary_loop(Vector &left, Vector &right, Vector &result) {
	if (left.type != right.type) {
		throw NotImplementedException("Type cast not implemented here!");
	}

	switch (left.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		_templated_binary_loop<int8_t, int8_t, OP>(left, right, result);
		break;
	case TypeId::SMALLINT:
		_templated_binary_loop<int16_t, int16_t, OP>(left, right, result);
		break;
	case TypeId::INTEGER:
		_templated_binary_loop<int32_t, int32_t, OP>(left, right, result);
		break;
	case TypeId::BIGINT:
		_templated_binary_loop<int64_t, int64_t, OP>(left, right, result);
		break;
	case TypeId::DECIMAL:
		_templated_binary_loop<double, double, OP>(left, right, result);
		break;
	case TypeId::POINTER:
		_templated_binary_loop<uint64_t, uint64_t, OP>(left, right, result);
		break;
	case TypeId::DATE:
		_templated_binary_loop<date_t, date_t, OP>(left, right, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

template <class OP, class RES>
void _fixed_return_binary_loop(Vector &left, Vector &right, Vector &result) {
	if (left.type != right.type) {
		throw NotImplementedException("Type cast not implemented here!");
	}

	switch (left.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		_templated_binary_loop<int8_t, RES, OP>(left, right, result);
		break;
	case TypeId::SMALLINT:
		_templated_binary_loop<int16_t, RES, OP>(left, right, result);
		break;
	case TypeId::INTEGER:
		_templated_binary_loop<int32_t, RES, OP>(left, right, result);
		break;
	case TypeId::BIGINT:
		_templated_binary_loop<int64_t, RES, OP>(left, right, result);
		break;
	case TypeId::DECIMAL:
		_templated_binary_loop<double, RES, OP>(left, right, result);
		break;
	case TypeId::POINTER:
		_templated_binary_loop<uint64_t, RES, OP>(left, right, result);
		break;
	case TypeId::DATE:
		_templated_binary_loop<date_t, RES, OP>(left, right, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

//===--------------------------------------------------------------------===//
// Numeric Operations
//===--------------------------------------------------------------------===//
void VectorOperations::Add(Vector &left, Vector &right, Vector &result) {
	_generic_binary_loop<operators::Addition>(left, right, result);
}

void VectorOperations::Subtract(Vector &left, Vector &right, Vector &result) {
	_generic_binary_loop<operators::Subtraction>(left, right, result);
}

void VectorOperations::Multiply(Vector &left, Vector &right, Vector &result) {
	_generic_binary_loop<operators::Multiplication>(left, right, result);
}

void VectorOperations::Divide(Vector &left, Vector &right, Vector &result) {
	_generic_binary_loop<operators::Division>(left, right, result);
}

void VectorOperations::Modulo(Vector &left, Vector &right, Vector &result) {
	_generic_binary_loop<operators::Modulo>(left, right, result);
}

void VectorOperations::Abs(Vector &left, Vector &result) {
	_generic_unary_loop<operators::Abs>(left, result);
}

void VectorOperations::Not(Vector &left, Vector &result) {
	if (left.type != TypeId::BOOLEAN) {
		throw Exception("NOT() needs a boolean input");
	}
	_generic_unary_loop<operators::Not>(left, result);
}

//===--------------------------------------------------------------------===//
// Is NULL/Is Not NULL
//===--------------------------------------------------------------------===//
template <bool INVERSE> void _is_null_loop(Vector &left, Vector &result) {
	if (result.type != TypeId::BOOLEAN) {
		throw Exception("IS (NOT) NULL returns a boolean!");
	}
	bool *res = (bool *)result.data;
	result.nullmask.reset();
	if (left.sel_vector) {
		for (size_t i = 0; i < left.count; i++) {
			res[left.sel_vector[i]] = INVERSE
			                              ? !left.nullmask[left.sel_vector[i]]
			                              : left.nullmask[left.sel_vector[i]];
		}
	} else {
		for (size_t i = 0; i < left.count; i++) {
			res[i] = INVERSE ? !left.nullmask[i] : left.nullmask[i];
		}
	}
	result.sel_vector = left.sel_vector;
	result.count = left.count;
}

void VectorOperations::IsNotNull(Vector &left, Vector &result) {
	_is_null_loop<true>(left, result);
}

void VectorOperations::IsNull(Vector &left, Vector &result) {
	_is_null_loop<false>(left, result);
}

//===--------------------------------------------------------------------===//
// Right-Hand Side Numeric Helpers
//===--------------------------------------------------------------------===//
template <VectorOperations::vector_function OP>
static void _numeric_operator_right(Vector &left, int64_t right,
                                    Vector &result) {
	Vector constant(Value::Numeric(left.type, right));
	OP(left, constant, result);
}

void VectorOperations::Add(Vector &left, int64_t right, Vector &result) {
	_numeric_operator_right<VectorOperations::Add>(left, right, result);
}

void VectorOperations::Subtract(Vector &left, int64_t right, Vector &result) {
	_numeric_operator_right<VectorOperations::Subtract>(left, right, result);
}

void VectorOperations::Multiply(Vector &left, int64_t right, Vector &result) {
	_numeric_operator_right<VectorOperations::Multiply>(left, right, result);
}

void VectorOperations::Divide(Vector &left, int64_t right, Vector &result) {
	_numeric_operator_right<VectorOperations::Divide>(left, right, result);
}

void VectorOperations::Modulo(Vector &left, int64_t right, Vector &result) {
	_numeric_operator_right<VectorOperations::Modulo>(left, right, result);
}

//===--------------------------------------------------------------------===//
// Left-Hand Side Numeric Helpers
//===--------------------------------------------------------------------===//
template <VectorOperations::vector_function OP>
static void _numeric_operator_left(int64_t left, Vector &right,
                                   Vector &result) {
	Vector constant(Value::Numeric(right.type, left));
	OP(constant, right, result);
}

void VectorOperations::Add(int64_t left, Vector &right, Vector &result) {
	_numeric_operator_left<VectorOperations::Add>(left, right, result);
}

void VectorOperations::Subtract(int64_t left, Vector &right, Vector &result) {
	_numeric_operator_left<VectorOperations::Subtract>(left, right, result);
}

void VectorOperations::Multiply(int64_t left, Vector &right, Vector &result) {
	_numeric_operator_left<VectorOperations::Multiply>(left, right, result);
}

void VectorOperations::Divide(int64_t left, Vector &right, Vector &result) {
	_numeric_operator_left<VectorOperations::Divide>(left, right, result);
}

void VectorOperations::Modulo(int64_t left, Vector &right, Vector &result) {
	_numeric_operator_left<VectorOperations::Modulo>(left, right, result);
}

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//
void VectorOperations::Equals(Vector &left, Vector &right, Vector &result) {
	_fixed_return_binary_loop<operators::Equals, int8_t>(left, right, result);
}

void VectorOperations::NotEquals(Vector &left, Vector &right, Vector &result) {
	_fixed_return_binary_loop<operators::NotEquals, int8_t>(left, right,
	                                                        result);
}

void VectorOperations::GreaterThan(Vector &left, Vector &right,
                                   Vector &result) {
	_fixed_return_binary_loop<operators::GreaterThan, int8_t>(left, right,
	                                                          result);
}

void VectorOperations::GreaterThanEquals(Vector &left, Vector &right,
                                         Vector &result) {
	_fixed_return_binary_loop<operators::GreaterThanEquals, int8_t>(left, right,
	                                                                result);
}

void VectorOperations::LessThan(Vector &left, Vector &right, Vector &result) {
	_fixed_return_binary_loop<operators::LessThan, int8_t>(left, right, result);
}

void VectorOperations::LessThanEquals(Vector &left, Vector &right,
                                      Vector &result) {
	_fixed_return_binary_loop<operators::LessThanEquals, int8_t>(left, right,
	                                                             result);
}

//===--------------------------------------------------------------------===//
// AND/OR
//===--------------------------------------------------------------------===//
template <class OP, class NULLOP>
void _templated_bool_nullmask_op(Vector &left, Vector &right, Vector &result) {
	bool *ldata = (bool *)left.data;
	bool *rdata = (bool *)right.data;
	bool *result_data = (bool *)result.data;

	if (left.count == right.count) {
		if (left.sel_vector) {
			assert(right.sel_vector);
			for (size_t i = 0; i < left.count; i++) {
				size_t index = left.sel_vector[i];
				assert(left.sel_vector[i] == right.sel_vector[i]);
				result_data[index] = OP::Operation(ldata[index], rdata[index]);
				result.nullmask[index] = NULLOP::Operation(
				    ldata[index], rdata[index], left.nullmask[index],
				    right.nullmask[index]);
			}
		} else {
			assert(!right.sel_vector);
			for (size_t i = 0; i < left.count; i++) {
				result_data[i] = OP::Operation(ldata[i], rdata[i]);
				result.nullmask[i] = NULLOP::Operation(
				    ldata[i], rdata[i], left.nullmask[i], right.nullmask[i]);
			}
		}
		result.sel_vector = left.sel_vector;
		result.count = left.count;
	} else if (left.count == 1 && !left.sel_vector) {
		bool left_null = left.nullmask[0];
		bool constant = ldata[0];
		if (right.sel_vector) {
			for (size_t i = 0; i < right.count; i++) {
				size_t index = right.sel_vector[i];
				result_data[index] = OP::Operation(constant, rdata[index]);
				result.nullmask[index] = NULLOP::Operation(
				    constant, rdata[i], left_null, right.nullmask[index]);
			}
		} else {
			for (size_t i = 0; i < right.count; i++) {
				result_data[i] = OP::Operation(constant, rdata[i]);
				result.nullmask[i] = NULLOP::Operation(
				    constant, rdata[i], left_null, right.nullmask[i]);
			}
		}
		result.sel_vector = right.sel_vector;
		result.count = right.count;
	} else if (right.count == 1) {
		// AND/OR operations are commutative
		_templated_bool_nullmask_op<OP, NULLOP>(right, left, result);
	} else {
		throw Exception("Vector lengths don't match");
	}
}

/*
SQL AND Rules:

TRUE  AND TRUE   = TRUE
TRUE  AND FALSE  = FALSE
TRUE  AND NULL   = NULL
FALSE AND TRUE   = FALSE
FALSE AND FALSE  = FALSE
FALSE AND NULL   = FALSE
NULL  AND TRUE   = NULL
NULL  AND FALSE  = FALSE
NULL  AND NULL   = NULL

Basically:
- Only true if both are true
- False if either is false (regardless of NULLs)
- NULL otherwise
*/

namespace operators {
struct AndMask {
	static inline bool Operation(bool left, bool right, bool left_null,
	                             bool right_null) {
		return (left_null && (right_null || right)) || (right_null && left);
	}
};
} // namespace operators

void VectorOperations::And(Vector &left, Vector &right, Vector &result) {
	if (left.type != TypeId::BOOLEAN || right.type != TypeId::BOOLEAN) {
		throw NotImplementedException("FIXME cast");
	}

	_templated_bool_nullmask_op<operators::And, operators::AndMask>(left, right,
	                                                                result);
}

/*
SQL OR Rules:

OR
TRUE  OR TRUE  = TRUE
TRUE  OR FALSE = TRUE
TRUE  OR NULL  = TRUE
FALSE OR TRUE  = TRUE
FALSE OR FALSE = FALSE
FALSE OR NULL  = NULL
NULL  OR TRUE  = TRUE
NULL  OR FALSE = NULL
NULL  OR NULL  = NULL

Basically:
- Only false if both are false
- True if either is true (regardless of NULLs)
- NULL otherwise
*/
namespace operators {
struct OrMask {
	static inline bool Operation(bool left, bool right, bool left_null,
	                             bool right_null) {
		return (left_null && (right_null || !right)) || (right_null && !left);
	}
};
} // namespace operators
void VectorOperations::Or(Vector &left, Vector &right, Vector &result) {
	if (left.type != TypeId::BOOLEAN || right.type != TypeId::BOOLEAN) {
		throw NotImplementedException("FIXME cast");
	}

	_templated_bool_nullmask_op<operators::Or, operators::OrMask>(left, right,
	                                                              result);
}
//===--------------------------------------------------------------------===//
// Set all elements of a vector to the constant value
//===--------------------------------------------------------------------===//
void VectorOperations::Set(Vector &result, Value value) {
	if (value.type != result.type) {
		value = value.CastAs(result.type);
	}

	if (value.is_null) {
		// initialize the NULL mask with all 1
		result.nullmask.set();
	} else {
		// set all values in the nullmask to 0
		result.nullmask.reset();
		Vector left(value);
		_generic_binary_loop<operators::PickLeft>(left, result, result);
	}
}

//===--------------------------------------------------------------------===//
// Hash functions
//===--------------------------------------------------------------------===//

void VectorOperations::Hash(Vector &left, Vector &result) {
	_fixed_return_unary_loop_null<operators::Hash, int32_t>(left, result);
}

void VectorOperations::CombineHash(Vector &left, Vector &right,
                                   Vector &result) {
	if (left.type != TypeId::INTEGER) {
		throw NotImplementedException(
		    "Left argument must be 32-bit integer hash");
	}
	VectorOperations::Hash(right, result);
	_templated_binary_loop<int32_t, int32_t, operators::XOR>(left, result,
	                                                         result);
}
