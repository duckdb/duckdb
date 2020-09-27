//===--------------------------------------------------------------------===//
// generators.cpp
// Description: This file contains the implementation of different generators
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {
using namespace std;

template <class T> void templated_generate_sequence(Vector &result, idx_t count, int64_t start, int64_t increment) {
	assert(result.type.IsNumeric());
	if (start > NumericLimits<T>::Maximum() || increment > NumericLimits<T>::Maximum()) {
		throw Exception("Sequence start or increment out of type range");
	}
	result.vector_type = VectorType::FLAT_VECTOR;
	auto result_data = FlatVector::GetData<T>(result);
	auto value = (T)start;
	for (idx_t i = 0; i < count; i++) {
		result_data[i] = value;
		value += increment;
	}
}

void VectorOperations::GenerateSequence(Vector &result, idx_t count, int64_t start, int64_t increment) {
	if (!result.type.IsNumeric()) {
		throw InvalidTypeException(result.type, "Can only generate sequences for numeric values!");
	}
	switch (result.type.InternalType()) {
	case PhysicalType::INT8:
		templated_generate_sequence<int8_t>(result, count, start, increment);
		break;
	case PhysicalType::INT16:
		templated_generate_sequence<int16_t>(result, count, start, increment);
		break;
	case PhysicalType::INT32:
		templated_generate_sequence<int32_t>(result, count, start, increment);
		break;
	case PhysicalType::INT64:
		templated_generate_sequence<int64_t>(result, count, start, increment);
		break;
	case PhysicalType::FLOAT:
		templated_generate_sequence<float>(result, count, start, increment);
		break;
	case PhysicalType::DOUBLE:
		templated_generate_sequence<double>(result, count, start, increment);
		break;
	default:
		throw NotImplementedException("Unimplemented type for generate sequence");
	}
}

template <class T>
void templated_generate_sequence(Vector &result, idx_t count, const SelectionVector &sel, int64_t start,
                                 int64_t increment) {
	assert(result.type.IsNumeric());
	if (start > NumericLimits<T>::Maximum() || increment > NumericLimits<T>::Maximum()) {
		throw Exception("Sequence start or increment out of type range");
	}
	result.vector_type = VectorType::FLAT_VECTOR;
	auto result_data = FlatVector::GetData<T>(result);
	auto value = (T)start;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		result_data[idx] = value + increment * idx;
	}
}

void VectorOperations::GenerateSequence(Vector &result, idx_t count, const SelectionVector &sel, int64_t start,
                                        int64_t increment) {
	if (!result.type.IsNumeric()) {
		throw InvalidTypeException(result.type, "Can only generate sequences for numeric values!");
	}
	switch (result.type.InternalType()) {
	case PhysicalType::INT8:
		templated_generate_sequence<int8_t>(result, count, sel, start, increment);
		break;
	case PhysicalType::INT16:
		templated_generate_sequence<int16_t>(result, count, sel, start, increment);
		break;
	case PhysicalType::INT32:
		templated_generate_sequence<int32_t>(result, count, sel, start, increment);
		break;
	case PhysicalType::INT64:
		templated_generate_sequence<int64_t>(result, count, sel, start, increment);
		break;
	case PhysicalType::FLOAT:
		templated_generate_sequence<float>(result, count, sel, start, increment);
		break;
	case PhysicalType::DOUBLE:
		templated_generate_sequence<double>(result, count, sel, start, increment);
		break;
	default:
		throw NotImplementedException("Unimplemented type for generate sequence");
	}
}

} // namespace duckdb
