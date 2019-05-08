//===--------------------------------------------------------------------===//
// sort.cpp
// Description: This file contains the implementation of the sort operator
//===--------------------------------------------------------------------===//

#include "common/exception.hpp"
#include "common/operator/comparison_operators.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class T, class OP>
static sel_t templated_quicksort_initial(T *data, sel_t *sel_vector, sel_t result[], uint64_t count) {
	// select pivot
	sel_t pivot = 0;
	sel_t low = 0, high = count - 1;
	if (sel_vector) {
		// now insert elements
		for (uint64_t i = 1; i < count; i++) {
			if (OP::Operation(data[sel_vector[i]], data[pivot])) {
				result[low++] = sel_vector[i];
			} else {
				result[high--] = sel_vector[i];
			}
		}
		assert(low == high);
		result[low] = sel_vector[pivot];
	} else {
		// now insert elements
		for (uint64_t i = 1; i < count; i++) {
			if (OP::Operation(data[i], data[pivot])) {
				result[low++] = i;
			} else {
				result[high--] = i;
			}
		}
		assert(low == high);
		result[low] = pivot;
	}
	return low;
}

template <class T, class OP> static void templated_quicksort_inplace(T *data, sel_t result[], sel_t left, sel_t right) {
	if (left >= right) {
		return;
	}

	sel_t middle = left + (right - left) / 2;
	sel_t pivot = result[middle];

	// move the mid point value to the front.
	sel_t i = left + 1;
	sel_t j = right;

	std::swap(result[middle], result[left]);
	while (i <= j) {
		while (i <= j && (OP::Operation(data[result[i]], data[pivot]))) {
			i++;
		}

		while (i <= j && OP::Operation(data[pivot], data[result[j]])) {
			j--;
		}

		if (i < j) {
			std::swap(result[i], result[j]);
		}
	}
	std::swap(result[i - 1], result[left]);
	sel_t part = i - 1;

	if (part > 0) {
		templated_quicksort_inplace<T, OP>(data, result, left, part - 1);
	}
	templated_quicksort_inplace<T, OP>(data, result, part + 1, right);
}

template <class T, class OP> void templated_quicksort(T *data, sel_t *sel_vector, uint64_t count, sel_t result[]) {
	auto part = templated_quicksort_initial<T, OP>(data, sel_vector, result, count);
	if (part > count) {
		return;
	}
	templated_quicksort_inplace<T, OP>(data, result, 0, part);
	templated_quicksort_inplace<T, OP>(data, result, part + 1, count - 1);
}

template <class T> static void templated_quicksort(Vector &vector, sel_t *sel_vector, uint64_t count, sel_t result[]) {
	auto data = (T *)vector.data;
	// quicksort without nulls
	templated_quicksort<T, duckdb::LessThanEquals>(data, sel_vector, count, result);
}

void VectorOperations::Sort(Vector &vector, sel_t *sel_vector, uint64_t count, sel_t result[]) {
	if (count == 0) {
		return;
	}
#ifdef DEBUG
	VectorOperations::Exec(sel_vector, count, [&](uint64_t i, uint64_t k) { assert(!vector.nullmask[i]); });
#endif
	switch (vector.type) {
	case TypeId::TINYINT:
		templated_quicksort<int8_t>(vector, sel_vector, count, result);
		break;
	case TypeId::SMALLINT:
		templated_quicksort<int16_t>(vector, sel_vector, count, result);
		break;
	case TypeId::INTEGER:
		templated_quicksort<int32_t>(vector, sel_vector, count, result);
		break;
	case TypeId::BIGINT:
		templated_quicksort<int64_t>(vector, sel_vector, count, result);
		break;
	case TypeId::FLOAT:
		templated_quicksort<float>(vector, sel_vector, count, result);
		break;
	case TypeId::DOUBLE:
		templated_quicksort<double>(vector, sel_vector, count, result);
		break;
	case TypeId::VARCHAR:
		templated_quicksort<const char *>(vector, sel_vector, count, result);
		break;
	case TypeId::POINTER:
		templated_quicksort<uint64_t>(vector, sel_vector, count, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type for sort");
	}
}

void VectorOperations::Sort(Vector &vector, sel_t result[]) {
	// first we extract NULL values
	sel_t not_null_sel_vector[STANDARD_VECTOR_SIZE], null_sel_vector[STANDARD_VECTOR_SIZE];
	sel_t *sel_vector;
	uint64_t count = Vector::NotNullSelVector(vector, not_null_sel_vector, sel_vector, null_sel_vector);
	if (count == vector.count) {
		// no NULL values
		// we don't need to use the selection vector at all
		VectorOperations::Sort(vector, nullptr, vector.count, result);
	} else {
		// first fill in the NULL values
		uint64_t null_count = vector.count - count;
		for (uint64_t i = 0; i < null_count; i++) {
			result[i] = null_sel_vector[i];
		}
		// now sort the remainder
		VectorOperations::Sort(vector, not_null_sel_vector, count, result + null_count);
	}
}
