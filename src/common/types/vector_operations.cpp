
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
	auto ldata = (T *)left.data;
	auto result_data = (RES *)result.data;
	result.nullmask = left.nullmask;
	VectorOperations::Exec(left, [&](size_t i, size_t k) {
		result_data[i] = OP::Operation(ldata[i]);
	});
	result.sel_vector = left.sel_vector;
	result.count = left.count;
}

template <class T, class RES, class OP>
void _templated_unary_loop_null(Vector &left, Vector &result) {
	auto ldata = (T *)left.data;
	auto result_data = (RES *)result.data;
	result.nullmask.reset();
	VectorOperations::Exec(left, [&](size_t i, size_t k) {
		result_data[i] = OP::Operation(ldata[i], left.nullmask[i]);
	});
	result.sel_vector = left.sel_vector;
	result.count = left.count;
}

template <class T, class RES, class OP, bool IGNORENULL>
void _templated_binary_loop(Vector &left, Vector &right, Vector &result) {
	auto ldata = (T *)left.data;
	auto rdata = (T *)right.data;
	auto result_data = (RES *)result.data;

	if (left.IsConstant()) {
		if (left.nullmask[0]) {
			// left side is constant NULL, set everything to NULL
			result.nullmask.set();
		} else {
			// left side is normal constant, use right nullmask and do
			// computation
			T constant = ldata[0];
			result.nullmask = right.nullmask;
			VectorOperations::Exec(right, [&](size_t i, size_t k) {
				if (!IGNORENULL || !right.nullmask[i]) {
					result_data[i] = OP::Operation(constant, rdata[i]);
				}
			});
		}
		result.sel_vector = right.sel_vector;
		result.count = right.count;
	} else if (right.IsConstant()) {
		if (right.nullmask[0]) {
			// right side is constant NULL, set everything to NULL
			result.nullmask.set();
		} else {
			// right side is normal constant, use left nullmask and do
			// computation
			T constant = rdata[0];
			result.nullmask = left.nullmask;
			VectorOperations::Exec(left, [&](size_t i, size_t k) {
				if (!IGNORENULL || !right.nullmask[i]) {
					result_data[i] = OP::Operation(ldata[i], constant);
				}
			});
		}
		result.sel_vector = left.sel_vector;
		result.count = left.count;
	} else if (left.count == right.count) {
		// OR nullmasks together
		result.nullmask = left.nullmask | right.nullmask;
		assert(left.sel_vector == right.sel_vector);
		VectorOperations::Exec(left, [&](size_t i, size_t k) {
			if (!IGNORENULL || !right.nullmask[i]) {
				result_data[i] = OP::Operation(ldata[i], rdata[i]);
			}
		});
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
	case TypeId::DATE:
		_templated_unary_loop_null<date_t, RES, OP>(left, result);
		break;
	case TypeId::TIMESTAMP:
		_templated_unary_loop_null<timestamp_t, RES, OP>(left, result);
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
	case TypeId::POINTER:
		_templated_unary_loop<uint64_t, uint64_t, OP>(left, result);
		break;
	case TypeId::DATE:
		_templated_unary_loop<date_t, date_t, OP>(left, result);
		break;
	case TypeId::TIMESTAMP:
		_templated_unary_loop<timestamp_t, timestamp_t, OP>(left, result);
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
		_templated_binary_loop<int8_t, int8_t, OP, false>(left, right, result);
		break;
	case TypeId::SMALLINT:
		_templated_binary_loop<int16_t, int16_t, OP, false>(left, right,
		                                                    result);
		break;
	case TypeId::INTEGER:
		_templated_binary_loop<int32_t, int32_t, OP, false>(left, right,
		                                                    result);
		break;
	case TypeId::BIGINT:
		_templated_binary_loop<int64_t, int64_t, OP, false>(left, right,
		                                                    result);
		break;
	case TypeId::DECIMAL:
		_templated_binary_loop<double, double, OP, false>(left, right, result);
		break;
	case TypeId::POINTER:
		_templated_binary_loop<uint64_t, uint64_t, OP, false>(left, right,
		                                                      result);
		break;
	case TypeId::DATE:
		_templated_binary_loop<date_t, date_t, OP, false>(left, right, result);
		break;
	case TypeId::TIMESTAMP:
		_templated_binary_loop<timestamp_t, timestamp_t, OP, false>(left, right,
		                                                            result);
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
		_templated_binary_loop<int8_t, RES, OP, false>(left, right, result);
		break;
	case TypeId::SMALLINT:
		_templated_binary_loop<int16_t, RES, OP, false>(left, right, result);
		break;
	case TypeId::INTEGER:
		_templated_binary_loop<int32_t, RES, OP, false>(left, right, result);
		break;
	case TypeId::BIGINT:
		_templated_binary_loop<int64_t, RES, OP, false>(left, right, result);
		break;
	case TypeId::DECIMAL:
		_templated_binary_loop<double, RES, OP, false>(left, right, result);
		break;
	case TypeId::POINTER:
		_templated_binary_loop<uint64_t, RES, OP, false>(left, right, result);
		break;
	case TypeId::DATE:
		_templated_binary_loop<date_t, RES, OP, false>(left, right, result);
		break;
	case TypeId::TIMESTAMP:
		_templated_binary_loop<timestamp_t, RES, OP, false>(left, right,
		                                                    result);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

void VectorOperations::Not(Vector &left, Vector &result) {
	if (left.type != TypeId::BOOLEAN) {
		throw InvalidTypeException(left.type, "NOT() needs a boolean input");
	}
	_templated_unary_loop<int8_t, int8_t, operators::Not>(left, result);
}

//===--------------------------------------------------------------------===//
// Is NULL/Is Not NULL
//===--------------------------------------------------------------------===//
template <bool INVERSE> void _is_null_loop(Vector &left, Vector &result) {
	if (result.type != TypeId::BOOLEAN) {
		throw InvalidTypeException(result.type,
		                           "IS (NOT) NULL returns a boolean!");
	}
	auto res = (bool *)result.data;
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
// Comparison Operations
//===--------------------------------------------------------------------===//
void VectorOperations::Equals(Vector &left, Vector &right, Vector &result) {
	if (left.type == TypeId::VARCHAR) {
		if (right.type != TypeId::VARCHAR) {
			throw TypeMismatchException(left.type, right.type,
			                            "Can't compare different types");
		}
		_templated_binary_loop<char *, int8_t, operators::EqualsVarchar, true>(
		    left, right, result);
	} else {
		_fixed_return_binary_loop<operators::Equals, int8_t>(left, right,
		                                                     result);
	}
}

void VectorOperations::NotEquals(Vector &left, Vector &right, Vector &result) {
	if (left.type == TypeId::VARCHAR) {
		if (right.type != TypeId::VARCHAR) {
			throw TypeMismatchException(left.type, right.type,
			                            "Can't compare different types");
		}
		_templated_binary_loop<char *, int8_t, operators::NotEqualsVarchar,
		                       true>(left, right, result);
	} else {
		_fixed_return_binary_loop<operators::NotEquals, int8_t>(left, right,
		                                                        result);
	}
}

void VectorOperations::GreaterThan(Vector &left, Vector &right,
                                   Vector &result) {
	if (left.type == TypeId::VARCHAR) {
		if (right.type != TypeId::VARCHAR) {
			throw TypeMismatchException(left.type, right.type,
			                            "Can't compare different types");
		}
		_templated_binary_loop<char *, int8_t, operators::GreaterThanVarchar,
		                       true>(left, right, result);
	} else {
		_fixed_return_binary_loop<operators::GreaterThan, int8_t>(left, right,
		                                                          result);
	}
}

void VectorOperations::GreaterThanEquals(Vector &left, Vector &right,
                                         Vector &result) {
	if (left.type == TypeId::VARCHAR) {
		if (right.type != TypeId::VARCHAR) {
			throw TypeMismatchException(left.type, right.type,
			                            "Can't compare different types");
		}
		_templated_binary_loop<char *, int8_t,
		                       operators::GreaterThanEqualsVarchar, true>(
		    left, right, result);
	} else {
		_fixed_return_binary_loop<operators::GreaterThanEquals, int8_t>(
		    left, right, result);
	}
}

void VectorOperations::LessThan(Vector &left, Vector &right, Vector &result) {
	if (left.type == TypeId::VARCHAR) {
		if (right.type != TypeId::VARCHAR) {
			throw TypeMismatchException(left.type, right.type,
			                            "Can't compare different types");
		}
		_templated_binary_loop<char *, int8_t, operators::LessThanVarchar,
		                       true>(left, right, result);
	} else {
		_fixed_return_binary_loop<operators::LessThan, int8_t>(left, right,
		                                                       result);
	}
}

void VectorOperations::LessThanEquals(Vector &left, Vector &right,
                                      Vector &result) {
	if (left.type == TypeId::VARCHAR) {
		if (right.type != TypeId::VARCHAR) {
			throw TypeMismatchException(left.type, right.type,
			                            "Can't compare different types");
		}
		_templated_binary_loop<char *, int8_t, operators::LessThanEqualsVarchar,
		                       true>(left, right, result);
	} else {
		_fixed_return_binary_loop<operators::LessThanEquals, int8_t>(
		    left, right, result);
	}
}

//===--------------------------------------------------------------------===//
// AND/OR
//===--------------------------------------------------------------------===//
template <class OP, class NULLOP>
void _templated_bool_nullmask_op(Vector &left, Vector &right, Vector &result) {
	bool *ldata = (bool *)left.data;
	bool *rdata = (bool *)right.data;
	bool *result_data = (bool *)result.data;

	if (left.IsConstant()) {
		bool left_null = left.nullmask[0];
		bool constant = ldata[0];
		VectorOperations::Exec(right, [&](size_t i, size_t k) {
			result_data[i] = OP::Operation(constant, rdata[i]);
			result.nullmask[i] = NULLOP::Operation(
			    constant, rdata[i], left_null, right.nullmask[i]);
		});
		result.sel_vector = right.sel_vector;
		result.count = right.count;
	} else if (right.IsConstant()) {
		// AND/OR operations are commutative
		_templated_bool_nullmask_op<OP, NULLOP>(right, left, result);
	} else if (left.count == right.count) {
		assert(left.sel_vector == right.sel_vector);
		VectorOperations::Exec(left, [&](size_t i, size_t k) {
			result_data[i] = OP::Operation(ldata[i], rdata[i]);
			result.nullmask[i] = NULLOP::Operation(
			    ldata[i], rdata[i], left.nullmask[i], right.nullmask[i]);
		});
		result.sel_vector = left.sel_vector;
		result.count = left.count;
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
		if (left.type < TypeId::VARCHAR) {
			_generic_binary_loop<operators::PickLeft>(left, result, result);
		} else if (left.type == TypeId::VARCHAR) {
			auto str = result.string_heap.AddString(value.str_value);
			const char **dataptr = (const char **)result.data;
			VectorOperations::Exec(
			    result, [&](size_t i, size_t k) { dataptr[i] = str; });
		} else {
			throw NotImplementedException("Unimplemented type for Set");
		}
	}
}

//===--------------------------------------------------------------------===//
// Hash functions
//===--------------------------------------------------------------------===//

#include "common/operator/numeric_bitwise_operators.hpp"

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
	_templated_binary_loop<int32_t, int32_t, operators::BitwiseXOR, false>(
	    left, result, result);
	// VectorOperations::BitwiseXORInPlace(result, left);
}
