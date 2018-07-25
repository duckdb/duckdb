
#include "execution/vector/vector_operations.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

// FIXME: find a better way to do this with templates

template <class T>
void ADDITION_LOOP(Vector &left, Vector &right, Vector &result) {
	T *ldata = (T *)left.data;
	T *rdata = (T *)right.data;
	T *result_data = (T *)result.data;
	for (size_t i = 0; i < left.count; i++) {
		result_data[i] = ldata[i] + rdata[i];
	}
}

void VectorOperations::Add(Vector &left, Vector &right, Vector &result) {
	if (left.type != right.type) {
		throw NotImplementedException("not implemented");
	}
	if (left.count == right.count) {
		switch (left.type) {
		case TypeId::TINYINT:
			ADDITION_LOOP<int8_t>(left, right, result);
			break;
		case TypeId::SMALLINT:
			ADDITION_LOOP<int16_t>(left, right, result);
			break;
		case TypeId::INTEGER:
			ADDITION_LOOP<int32_t>(left, right, result);
			break;
		case TypeId::BIGINT:
			ADDITION_LOOP<int64_t>(left, right, result);
			break;
		case TypeId::DECIMAL:
			ADDITION_LOOP<double>(left, right, result);
			break;
		default:
			throw NotImplementedException("Unimplemented type");
		}
		result.count = left.count;
	} else {
		throw Exception("Vector lengths don't match");
	}
}

void VectorOperations::Subtract(Vector &left, Vector &right, Vector &result) {
	throw NotImplementedException("Unimplemented");
}

template <class T>
void MULTIPLY_LOOP(Vector &left, Vector &right, Vector &result) {
	T *ldata = (T *)left.data;
	T *rdata = (T *)right.data;
	T *result_data = (T *)result.data;
	for (size_t i = 0; i < left.count; i++) {
		result_data[i] = ldata[i] * rdata[i];
	}
}

void VectorOperations::Multiply(Vector &left, Vector &right, Vector &result) {
	if (left.type != right.type) {
		throw NotImplementedException("not implemented");
	}
	if (left.count == right.count) {
		switch (left.type) {
		case TypeId::TINYINT:
			MULTIPLY_LOOP<int8_t>(left, right, result);
			break;
		case TypeId::SMALLINT:
			MULTIPLY_LOOP<int16_t>(left, right, result);
			break;
		case TypeId::INTEGER:
			MULTIPLY_LOOP<int32_t>(left, right, result);
			break;
		case TypeId::BIGINT:
			MULTIPLY_LOOP<int64_t>(left, right, result);
			break;
		case TypeId::DECIMAL:
			MULTIPLY_LOOP<double>(left, right, result);
			break;
		default:
			throw NotImplementedException("Unimplemented type");
		}
		result.count = left.count;
	} else {
		throw Exception("Vector lengths don't match");
	}
}

void VectorOperations::Divide(Vector &left, Vector &right, Vector &result) {
	throw NotImplementedException("Unimplemented");
}

template <class T>
void COMPARE_LOOP(Vector &left, Vector &right, Vector &result) {
	T *ldata = (T *)left.data;
	T *rdata = (T *)right.data;
	bool *result_data = (bool *)result.data;
	for (size_t i = 0; i < left.count; i++) {
		result_data[i] = ldata[i] == rdata[i];
	}
}

template <class T>
void COMPARE_LOOP_CONSTANT(Vector &left, Vector &right, Vector &result) {
	T *ldata = (T *)left.data;
	T *rdata = (T *)right.data;
	bool *result_data = (bool *)result.data;
	for (size_t i = 0; i < left.count; i++) {
		result_data[i] = ldata[i] == rdata[0];
	}
}


void VectorOperations::Compare(Vector &left, Vector &right, Vector &result) {
	if (left.type != right.type) {
		throw NotImplementedException("not implemented");
	}

	if (left.count == right.count) {
		switch (left.type) {
		case TypeId::TINYINT:
			COMPARE_LOOP<int8_t>(left, right, result);
			break;
		case TypeId::SMALLINT:
			COMPARE_LOOP<int16_t>(left, right, result);
			break;
		case TypeId::INTEGER:
			COMPARE_LOOP<int32_t>(left, right, result);
			break;
		case TypeId::BIGINT:
			COMPARE_LOOP<int64_t>(left, right, result);
			break;
		case TypeId::DECIMAL:
			COMPARE_LOOP<double>(left, right, result);
			break;
		default:
			throw NotImplementedException("Unimplemented type");
		}
	} else if (right.count == 1) {
		switch (left.type) {
		case TypeId::TINYINT:
			COMPARE_LOOP_CONSTANT<int8_t>(left, right, result);
			break;
		case TypeId::SMALLINT:
			COMPARE_LOOP_CONSTANT<int16_t>(left, right, result);
			break;
		case TypeId::INTEGER:
			COMPARE_LOOP_CONSTANT<int32_t>(left, right, result);
			break;
		case TypeId::BIGINT:
			COMPARE_LOOP_CONSTANT<int64_t>(left, right, result);
			break;
		case TypeId::DECIMAL:
			COMPARE_LOOP_CONSTANT<double>(left, right, result);
			break;
		default:
			throw NotImplementedException("Unimplemented type");
		}
	} else if (left.count == 1) {
		return Compare(right, left, result);
	} else {
		throw Exception("Vector lengths don't match");
	}
	result.count = left.count;
}


template <class T>
void GREATER_THAN_LOOP(Vector &left, Vector &right, Vector &result) {
	T *ldata = (T *)left.data;
	T *rdata = (T *)right.data;
	bool *result_data = (bool *)result.data;
	for (size_t i = 0; i < left.count; i++) {
		result_data[i] = ldata[i] > rdata[i];
	}
}

template <class T>
void GREATER_THAN_LOOP_CONSTANT(Vector &left, Vector &right, Vector &result) {
	T *ldata = (T *)left.data;
	T *rdata = (T *)right.data;
	bool *result_data = (bool *)result.data;
	for (size_t i = 0; i < left.count; i++) {
		result_data[i] = ldata[i] > rdata[0];
	}
}

void VectorOperations::GreaterThan(Vector &left, Vector &right, Vector &result) {
	if (left.type != right.type) {
		throw NotImplementedException("not implemented");
	}

	if (left.count == right.count) {
		switch (left.type) {
		case TypeId::TINYINT:
			GREATER_THAN_LOOP<int8_t>(left, right, result);
			break;
		case TypeId::SMALLINT:
			GREATER_THAN_LOOP<int16_t>(left, right, result);
			break;
		case TypeId::INTEGER:
			GREATER_THAN_LOOP<int32_t>(left, right, result);
			break;
		case TypeId::BIGINT:
			GREATER_THAN_LOOP<int64_t>(left, right, result);
			break;
		case TypeId::DECIMAL:
			GREATER_THAN_LOOP<double>(left, right, result);
			break;
		default:
			throw NotImplementedException("Unimplemented type");
		}
	} else if (right.count == 1) {
		switch (left.type) {
		case TypeId::TINYINT:
			GREATER_THAN_LOOP_CONSTANT<int8_t>(left, right, result);
			break;
		case TypeId::SMALLINT:
			GREATER_THAN_LOOP_CONSTANT<int16_t>(left, right, result);
			break;
		case TypeId::INTEGER:
			GREATER_THAN_LOOP_CONSTANT<int32_t>(left, right, result);
			break;
		case TypeId::BIGINT:
			GREATER_THAN_LOOP_CONSTANT<int64_t>(left, right, result);
			break;
		case TypeId::DECIMAL:
			GREATER_THAN_LOOP_CONSTANT<double>(left, right, result);
			break;
		default:
			throw NotImplementedException("Unimplemented type");
		}
	} else if (left.count == 1) {
		return Compare(right, left, result);
	} else {
		throw Exception("Vector lengths don't match");
	}
	result.count = left.count;
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
