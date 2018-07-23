
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
