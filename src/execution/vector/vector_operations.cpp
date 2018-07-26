
#include "execution/vector/vector_operations.hpp"
#include "common/exception.hpp"
#include "common/types/hash.hpp"

using namespace duckdb;
using namespace std;

// Basic loop body used by functions here
// Does a type switch and selects a template based on the vector types

#define VECTOR_LOOP_BODY_UNARY(LOOP_FUNCTION)                                  \
	do {                                                                       \
		switch (left.type) {                                                   \
		case TypeId::TINYINT:                                                  \
			LOOP_FUNCTION<int8_t>(left, result);                               \
			break;                                                             \
		case TypeId::SMALLINT:                                                 \
			LOOP_FUNCTION<int16_t>(left, result);                              \
			break;                                                             \
		case TypeId::INTEGER:                                                  \
			LOOP_FUNCTION<int32_t>(left, result);                              \
			break;                                                             \
		case TypeId::BIGINT:                                                   \
			LOOP_FUNCTION<int64_t>(left, result);                              \
			break;                                                             \
		case TypeId::DECIMAL:                                                  \
			LOOP_FUNCTION<double>(left, result);                               \
			break;                                                             \
		default:                                                               \
			throw NotImplementedException("Unimplemented type");               \
		}                                                                      \
	} while (0)

#define VECTOR_LOOP_BODY_BINARY(LOOP_FUNCTION)                                 \
	do {                                                                       \
		if (left.type != right.type) {                                         \
			throw NotImplementedException("Type cast not implemented here!");  \
		}                                                                      \
		switch (left.type) {                                                   \
		case TypeId::TINYINT:                                                  \
			LOOP_FUNCTION<int8_t>(left, right, result);                        \
			break;                                                             \
		case TypeId::SMALLINT:                                                 \
			LOOP_FUNCTION<int16_t>(left, right, result);                       \
			break;                                                             \
		case TypeId::INTEGER:                                                  \
			LOOP_FUNCTION<int32_t>(left, right, result);                       \
			break;                                                             \
		case TypeId::BIGINT:                                                   \
			LOOP_FUNCTION<int64_t>(left, right, result);                       \
			break;                                                             \
		case TypeId::DECIMAL:                                                  \
			LOOP_FUNCTION<double>(left, right, result);                        \
			break;                                                             \
		default:                                                               \
			throw NotImplementedException("Unimplemented type");               \
		}                                                                      \
	} while (0)

// Actual generated loop function (using templates)
// Has support for OP(vector, vector), OP(scalar, vector) and OP(vector, scalar)

#define NUMERIC_LOOP_FUNCTION_UNARY(NAME, OPERATOR)                            \
	template <class T> void NAME##_LOOP(Vector &left, Vector &result) {        \
		T *ldata = (T *)left.data;                                             \
		T *result_data = (T *)result.data;                                     \
		if (left.sel_vector) {                                                 \
			for (size_t i = 0; i < left.count; i++) {                          \
				result_data[i] = OPERATOR(ldata[left.sel_vector[i]]);          \
			}                                                                  \
		} else {                                                               \
			for (size_t i = 0; i < left.count; i++) {                          \
				result_data[i] = OPERATOR(ldata[i]);                           \
			}                                                                  \
		}                                                                      \
		result.count = left.count;                                             \
	}

#define NUMERIC_FOLD_FUNCTION_UNARY(NAME, OPERATOR)                            \
	template <class T> void NAME##_LOOP(Vector &left, Vector &result) {        \
		T *ldata = (T *)left.data;                                             \
		T *result_data = (T *)result.data;                                     \
		result_data[0] = 0;                                                    \
		if (left.sel_vector) {                                                 \
			for (size_t i = 0; i < left.count; i++) {                          \
				result_data[0] OPERATOR ldata[left.sel_vector[i]];             \
			}                                                                  \
		} else {                                                               \
			for (size_t i = 0; i < left.count; i++) {                          \
				result_data[0] OPERATOR ldata[i];                              \
			}                                                                  \
		}                                                                      \
		result.count = 1;                                                      \
	}

#define GENERIC_LOOP_FUNCTION_BINARY(NAME, OPERATOR, RESTYPE)                  \
	template <class T>                                                         \
	void NAME##_LOOP(Vector &left, Vector &right, Vector &result) {            \
		T *ldata = (T *)left.data;                                             \
		T *rdata = (T *)right.data;                                            \
		RESTYPE *result_data = (RESTYPE *)result.data;                         \
		if (left.count == right.count) {                                       \
			if (left.sel_vector && right.sel_vector) {                         \
				for (size_t i = 0; i < left.count; i++) {                      \
					result_data[i] = ldata[left.sel_vector[i]] OPERATOR        \
					    rdata[right.sel_vector[i]];                            \
				}                                                              \
			} else if (left.sel_vector) {                                      \
				for (size_t i = 0; i < left.count; i++) {                      \
					result_data[i] =                                           \
					    ldata[left.sel_vector[i]] OPERATOR rdata[i];           \
				}                                                              \
			} else if (right.sel_vector) {                                     \
				for (size_t i = 0; i < left.count; i++) {                      \
					result_data[i] =                                           \
					    ldata[i] OPERATOR rdata[right.sel_vector[i]];          \
				}                                                              \
			} else {                                                           \
				for (size_t i = 0; i < left.count; i++) {                      \
					result_data[i] = ldata[i] OPERATOR rdata[i];               \
				}                                                              \
			}                                                                  \
			result.count = left.count;                                         \
		} else if (left.count == 1) {                                          \
			if (right.sel_vector) {                                            \
				for (size_t i = 0; i < right.count; i++) {                     \
					result_data[i] =                                           \
					    ldata[0] OPERATOR rdata[right.sel_vector[i]];          \
				}                                                              \
			} else {                                                           \
				for (size_t i = 0; i < right.count; i++) {                     \
					result_data[i] = ldata[0] OPERATOR rdata[i];               \
				}                                                              \
			}                                                                  \
			result.count = right.count;                                        \
		} else if (right.count == 1) {                                         \
			if (left.sel_vector) {                                             \
				for (size_t i = 0; i < left.count; i++) {                      \
					result_data[i] =                                           \
					    ldata[left.sel_vector[i]] OPERATOR rdata[0];           \
				}                                                              \
			} else {                                                           \
				for (size_t i = 0; i < left.count; i++) {                      \
					result_data[i] = ldata[i] OPERATOR rdata[0];               \
				}                                                              \
			}                                                                  \
			result.count = left.count;                                         \
		} else {                                                               \
			throw Exception("Vector lengths don't match");                     \
		}                                                                      \
	}

#define NUMERIC_LOOP_FUNCTION_BINARY(NAME, OPERATOR)                           \
	GENERIC_LOOP_FUNCTION_BINARY(NAME, OPERATOR, T)
#define BOOLEAN_LOOP_FUNCTION_BINARY(NAME, OPERATOR)                           \
	GENERIC_LOOP_FUNCTION_BINARY(NAME, OPERATOR, bool)

//===--------------------------------------------------------------------===//
// Numeric Operations
//===--------------------------------------------------------------------===//

NUMERIC_LOOP_FUNCTION_BINARY(ADDITION, +);
NUMERIC_LOOP_FUNCTION_BINARY(SUBTRACT, -);
NUMERIC_LOOP_FUNCTION_BINARY(MULTIPLY, *);
NUMERIC_LOOP_FUNCTION_BINARY(DIVIDE, /);

void VectorOperations::Add(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY_BINARY(ADDITION_LOOP);
}

void VectorOperations::Subtract(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY_BINARY(SUBTRACT_LOOP);
}

void VectorOperations::Multiply(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY_BINARY(MULTIPLY_LOOP);
}

void VectorOperations::Divide(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY_BINARY(DIVIDE_LOOP);
}

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//
BOOLEAN_LOOP_FUNCTION_BINARY(EQ, ==);
BOOLEAN_LOOP_FUNCTION_BINARY(NEQ, !=);
BOOLEAN_LOOP_FUNCTION_BINARY(GE, >);
BOOLEAN_LOOP_FUNCTION_BINARY(GEQ, >=);
BOOLEAN_LOOP_FUNCTION_BINARY(LE, <);
BOOLEAN_LOOP_FUNCTION_BINARY(LEQ, <=);

void VectorOperations::Equals(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY_BINARY(EQ_LOOP);
}

void VectorOperations::NotEquals(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY_BINARY(NEQ_LOOP);
}

void VectorOperations::GreaterThan(Vector &left, Vector &right,
                                   Vector &result) {
	VECTOR_LOOP_BODY_BINARY(GE_LOOP);
}

void VectorOperations::GreaterThanEquals(Vector &left, Vector &right,
                                         Vector &result) {
	VECTOR_LOOP_BODY_BINARY(GEQ_LOOP);
}

void VectorOperations::LessThan(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY_BINARY(LE_LOOP);
}

void VectorOperations::LessThanEquals(Vector &left, Vector &right,
                                      Vector &result) {
	VECTOR_LOOP_BODY_BINARY(LEQ_LOOP);
}

BOOLEAN_LOOP_FUNCTION_BINARY(AND, &&);
BOOLEAN_LOOP_FUNCTION_BINARY(OR, &&);

void VectorOperations::And(Vector &left, Vector &right, Vector &result) {
	if (left.type != TypeId::BOOLEAN || right.type != TypeId::BOOLEAN) {
		throw NotImplementedException("FIXME cast");
	}

	if (left.count == right.count) {
		AND_LOOP<bool>(left, right, result);
	} else {
		throw Exception("Vector lengths don't match");
	}
}

void VectorOperations::Or(Vector &left, Vector &right, Vector &result) {
	if (left.type != TypeId::BOOLEAN || right.type != TypeId::BOOLEAN) {
		throw NotImplementedException("FIXME cast");
	}

	if (left.count == right.count) {
		OR_LOOP<bool>(left, right, result);
	} else {
		throw Exception("Vector lengths don't match");
	}
}

NUMERIC_FOLD_FUNCTION_UNARY(SUM, +=);

void VectorOperations::Sum(Vector &left, Vector &result) {
	VECTOR_LOOP_BODY_UNARY(SUM_LOOP);
}

void VectorOperations::Count(Vector &left, Vector &result) {
	Vector count_vector(Value((int32_t)left.count));
	VectorOperations::Add(result, count_vector, result);
}

void VectorOperations::Average(Vector &left, Vector &result) {
	Vector count_vector(Value((int32_t)left.count));
	VECTOR_LOOP_BODY_UNARY(SUM_LOOP);
	VectorOperations::Divide(result, count_vector, result);
}

//===--------------------------------------------------------------------===//
// Hash functions
//===--------------------------------------------------------------------===//
NUMERIC_LOOP_FUNCTION_UNARY(HASH, Hash);

void VectorOperations::Hash(Vector &left, Vector &result) {
	VECTOR_LOOP_BODY_UNARY(HASH_LOOP);
}

void VectorOperations::CombineHash(Vector &left, Vector& right, Vector &result) {
	throw NotImplementedException("Combine hash! FIXME: xor");
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
template <class T> void COPY_LOOP(Vector &source, void *target) {
	T *sdata = (T *)source.data;
	T *result = (T *)target;
	if (source.sel_vector) {
		for (size_t i = 0; i < source.count; i++) {
			result[i] = sdata[source.sel_vector[i]];
		}
	} else {
		for (size_t i = 0; i < source.count; i++) {
			result[i] = sdata[i];
		}
	}
}

void VectorOperations::Copy(Vector &source, void *target) {
	if (source.count == 0)
		return;
	switch (source.type) {
	case TypeId::TINYINT:
		COPY_LOOP<int8_t>(source, target);
		break;
	case TypeId::SMALLINT:
		COPY_LOOP<int16_t>(source, target);
		break;
	case TypeId::INTEGER:
		COPY_LOOP<int32_t>(source, target);
		break;
	case TypeId::BIGINT:
		COPY_LOOP<int64_t>(source, target);
		break;
	case TypeId::DECIMAL:
		COPY_LOOP<double>(source, target);
		break;
	default:
		throw NotImplementedException("Unimplemented type for copy");
	}
}

