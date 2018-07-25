
#include "execution/vector/vector_operations.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

// Basic loop body used by functions here
// Does a type switch and selects a template based on the vector types
#define VECTOR_LOOP_BODY(LOOP_FUNCTION) do {\
	if (left.type != right.type) { \
		throw NotImplementedException("Type cast not implemented here!"); \
	} \
	switch (left.type) { \
	case TypeId::TINYINT: \
		LOOP_FUNCTION<int8_t>(left, right, result); \
		break; \
	case TypeId::SMALLINT: \
		LOOP_FUNCTION<int16_t>(left, right, result); \
		break; \
	case TypeId::INTEGER: \
		LOOP_FUNCTION<int32_t>(left, right, result); \
		break; \
	case TypeId::BIGINT: \
		LOOP_FUNCTION<int64_t>(left, right, result); \
		break; \
	case TypeId::DECIMAL: \
		LOOP_FUNCTION<double>(left, right, result); \
		break; \
	default: \
		throw NotImplementedException("Unimplemented type"); \
	} \
	result.count = left.count; \
	} while (0)

// Actual generated loop function (using templates)
// Has support for OP(vector, vector), OP(scalar, vector) and OP(vector, scalar)

#define GENERIC_LOOP_FUNCTION(NAME, OPERATOR, RESTYPE) \
template <class T> \
void NAME##_LOOP(Vector &left, Vector &right, Vector &result) { \
	T *ldata = (T *)left.data; \
	T *rdata = (T *)right.data; \
	RESTYPE *result_data = (RESTYPE *)result.data; \
	if (left.count == right.count) { \
		for (size_t i = 0; i < left.count; i++) { \
			result_data[i] = ldata[i] OPERATOR rdata[i]; \
		} \
	} else if (left.count == 1) { \
		for (size_t i = 0; i < right.count; i++) { \
			result_data[i] = ldata[0] OPERATOR rdata[i]; \
		} \
	} else if (right.count == 1) { \
		for (size_t i = 0; i < left.count; i++) { \
			result_data[i] = ldata[i] OPERATOR rdata[0]; \
		} \
	} else { \
		throw Exception("Vector lengths don't match"); \
	} \
}

#define NUMERIC_LOOP_FUNCTION(NAME, OPERATOR) GENERIC_LOOP_FUNCTION(NAME, OPERATOR, T)
#define BOOLEAN_LOOP_FUNCTION(NAME, OPERATOR) GENERIC_LOOP_FUNCTION(NAME, OPERATOR, bool)

//===--------------------------------------------------------------------===//
// Numeric Operations
//===--------------------------------------------------------------------===//

NUMERIC_LOOP_FUNCTION(ADDITION, +);
NUMERIC_LOOP_FUNCTION(SUBTRACT, -);
NUMERIC_LOOP_FUNCTION(MULTIPLY, *);
NUMERIC_LOOP_FUNCTION(DIVIDE, /);

void VectorOperations::Add(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY(ADDITION_LOOP);
}

void VectorOperations::Subtract(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY(SUBTRACT_LOOP);
}

void VectorOperations::Multiply(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY(MULTIPLY_LOOP);
}

void VectorOperations::Divide(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY(DIVIDE_LOOP);
}

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//
BOOLEAN_LOOP_FUNCTION(EQ, ==);
BOOLEAN_LOOP_FUNCTION(NEQ, !=);
BOOLEAN_LOOP_FUNCTION(GE, >);
BOOLEAN_LOOP_FUNCTION(GEQ, >=);
BOOLEAN_LOOP_FUNCTION(LE, <);
BOOLEAN_LOOP_FUNCTION(LEQ, <=);

void VectorOperations::Equals(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY(EQ_LOOP);
}

void VectorOperations::NotEquals(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY(NEQ_LOOP);
}

void VectorOperations::GreaterThan(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY(GE_LOOP);
}

void VectorOperations::GreaterThanEquals(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY(GEQ_LOOP);
}

void VectorOperations::LessThan(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY(LE_LOOP);
}

void VectorOperations::LessThanEquals(Vector &left, Vector &right, Vector &result) {
	VECTOR_LOOP_BODY(LEQ_LOOP);
}

typedef bool(*binary_bool_op)(bool, bool);

template<binary_bool_op op> 
void BOOLEAN_LOOP(Vector &left, Vector &right, Vector &result) {
	bool *ldata = (bool *)left.data;
	bool *rdata = (bool *)right.data;
	bool *result_data = (bool *)result.data;
	for (size_t i = 0; i < left.count; i++) {
		result_data[i] = op(ldata[i], rdata[i]);
	}
}
static inline bool boolean_and(bool a, bool b) { return a && b; }
static inline bool boolean_or(bool a, bool b) { return a || b; }

void VectorOperations::And(Vector &left, Vector &right, Vector &result) {
	if (left.type != TypeId::BOOLEAN || right.type != TypeId::BOOLEAN) {
		throw NotImplementedException("FIXME cast");
	}

	if (left.count == right.count) {
		BOOLEAN_LOOP<boolean_and>(left, right, result);
	} else {
		throw Exception("Vector lengths don't match");
	}
}

void VectorOperations::Or(Vector &left, Vector &right, Vector &result) {
	if (left.type != TypeId::BOOLEAN || right.type != TypeId::BOOLEAN) {
		throw NotImplementedException("FIXME cast");
	}

	if (left.count == right.count) {
		BOOLEAN_LOOP<boolean_or>(left, right, result);
	} else {
		throw Exception("Vector lengths don't match");
	}
}


template <class T>
void COPY_LOOP(Vector &source, void* target) {
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

void VectorOperations::Copy(Vector& source, void* target) {
	if (source.count == 0) return;
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
