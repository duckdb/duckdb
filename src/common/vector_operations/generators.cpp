//===--------------------------------------------------------------------===//
// generators.cpp
// Description: This file contains the implementation of different generators
//===--------------------------------------------------------------------===//

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class T>
void generate_sequence_function(T *__restrict result_data, T value, T increment, size_t count,
                                sel_t *__restrict sel_vector) {
	VectorOperations::Exec(sel_vector, count, [&](size_t i, size_t k) {
		result_data[i] = value;
		value += increment;
	});
}

template <class T> void templated_generate_sequence(Vector &result, T start, T increment) {
	auto ldata = (T *)result.data;
	generate_sequence_function<T>(ldata, start, increment, result.count, result.sel_vector);
}

void VectorOperations::GenerateSequence(Vector &result, int64_t start, int64_t increment) {
	if (!TypeIsNumeric(result.type)) {
		throw InvalidTypeException(result.type, "Can only generate sequences for numeric values!");
	}

	switch (result.type) {
	case TypeId::TINYINT:
		templated_generate_sequence<int8_t>(result, start, increment);
		break;
	case TypeId::SMALLINT:
		templated_generate_sequence<int16_t>(result, start, increment);
		break;
	case TypeId::INTEGER:
		templated_generate_sequence<int32_t>(result, start, increment);
		break;
	case TypeId::BIGINT:
		templated_generate_sequence<int64_t>(result, start, increment);
		break;
	case TypeId::FLOAT:
		templated_generate_sequence<float>(result, start, increment);
		break;
	case TypeId::DOUBLE:
		templated_generate_sequence<double>(result, start, increment);
		break;
	case TypeId::POINTER:
		templated_generate_sequence<uint64_t>(result, start, increment);
		break;
	default:
		throw NotImplementedException("Unimplemented type for generate sequence");
	}
}
