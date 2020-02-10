//===--------------------------------------------------------------------===//
// generators.cpp
// Description: This file contains the implementation of different generators
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class T>
void generate_sequence_function(T *__restrict result_data, T value, T increment, index_t count,
                                sel_t *__restrict sel_vector, bool ignore_sel_vector) {
	if (ignore_sel_vector) {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			result_data[i] = value;
			value += increment;
		});
	} else {
		if (sel_vector) {
			for (index_t i = 0; i < count; i++) {
				auto idx = sel_vector[i];
				result_data[idx] = value + increment * idx;
			}
		} else {
			for (index_t i = 0; i < count; i++) {
				result_data[i] = value;
				value += increment;
			}
		}
	}
}

template <class T> void templated_generate_sequence(Vector &result, T start, T increment, bool ignore_sel_vector) {
	auto ldata = (T *)result.GetData();
	generate_sequence_function<T>(ldata, start, increment, result.size(), result.sel_vector(), ignore_sel_vector);
}

void VectorOperations::GenerateSequence(Vector &result, int64_t start, int64_t increment, bool ignore_sel_vector) {
	if (!TypeIsNumeric(result.type)) {
		throw InvalidTypeException(result.type, "Can only generate sequences for numeric values!");
	}
	switch (result.type) {
	case TypeId::INT8:
		if (start > numeric_limits<int8_t>::max() || increment > numeric_limits<int8_t>::max()) {
			throw Exception("Sequence start or increment out of type range");
		}
		templated_generate_sequence<int8_t>(result, (int8_t)start, (int8_t)increment, ignore_sel_vector);
		break;
	case TypeId::INT16:
		if (start > numeric_limits<int16_t>::max() || increment > numeric_limits<int16_t>::max()) {
			throw Exception("Sequence start or increment out of type range");
		}
		templated_generate_sequence<int16_t>(result, (int16_t)start, (int16_t)increment, ignore_sel_vector);
		break;
	case TypeId::INT32:
		if (start > numeric_limits<int32_t>::max() || increment > numeric_limits<int32_t>::max()) {
			throw Exception("Sequence start or increment out of type range");
		}
		templated_generate_sequence<int32_t>(result, (int32_t)start, (int32_t)increment, ignore_sel_vector);
		break;
	case TypeId::INT64:
		templated_generate_sequence<int64_t>(result, start, increment, ignore_sel_vector);
		break;
	case TypeId::FLOAT:
		templated_generate_sequence<float>(result, start, increment, ignore_sel_vector);
		break;
	case TypeId::DOUBLE:
		templated_generate_sequence<double>(result, start, increment, ignore_sel_vector);
		break;
	default:
		throw NotImplementedException("Unimplemented type for generate sequence");
	}
}
