//===--------------------------------------------------------------------===//
// sort.cpp
// Description: This file contains the implementation of the sort operator
//===--------------------------------------------------------------------===//

#include "common/exception.hpp"
#include "common/operator/comparison_operators.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class T, class OP, bool HAS_NULL>
static sel_t templated_quicksort_initial(T *data, nullmask_t &mask,
                                         sel_t result[], size_t count) {
	// select pivot
	sel_t pivot = 0;
	sel_t low = 0, high = count - 1;
	if (HAS_NULL) {
		// set pivot to a not-null value
		while (pivot < count && mask[pivot]) {
			pivot++;
		}
		if (pivot == count) {
			// everything is NULL
			// array is already sorted!
			for (size_t i = 0; i < count; i++) {
				result[i] = i;
			}
			return (sel_t)-1;
		}
	}
	// now insert elements
	for (size_t i = 1; i < count; i++) {
		if (OP::Operation(data[i], data[pivot]) || (HAS_NULL && mask[i])) {
			result[low++] = i;
		} else {
			result[high--] = i;
		}
	}
	assert(low == high);
	result[low] = pivot;
	return low;
}

template <class T, class OP, bool HAS_NULL>
static void templated_quicksort_inplace(T *data, nullmask_t &mask,
                                        sel_t result[], sel_t left,
                                        sel_t right) {
	if (left >= right) {
		return;
	}

	sel_t middle = left + (right - left) / 2;
	if (HAS_NULL && mask[result[middle]]) {
		middle = left;
		// set pivot to a not-null value
		while (middle < right && mask[result[middle]]) {
			middle++;
		}
		if (middle == right) {
			// array is sorted already!
			// everything is NULL
			return;
		}
	}
	sel_t pivot = result[middle];

	// move the mid point value to the front.
	sel_t i = left + 1;
	sel_t j = right;

	std::swap(result[middle], result[left]);
	while (i <= j) {
		while (i <= j && (OP::Operation(data[result[i]], data[pivot]) ||
		                  (HAS_NULL && mask[result[i]]))) {
			i++;
		}

		while (i <= j && OP::Operation(data[pivot], data[result[j]]) &&
		       (!HAS_NULL || !mask[result[i]])) {
			j--;
		}

		if (i < j) {
			std::swap(result[i], result[j]);
		}
	}
	std::swap(result[i - 1], result[left]);
	sel_t part = i - 1;

	templated_quicksort_inplace<T, OP, HAS_NULL>(data, mask, result, left,
	                                             part - 1);
	templated_quicksort_inplace<T, OP, HAS_NULL>(data, mask, result, part + 1,
	                                             right);
}

template <class T, class OP, bool HAS_NULL>
void templated_quicksort(T *data, nullmask_t &nullmask, size_t count,
                         sel_t result[]) {
	auto part = templated_quicksort_initial<T, OP, HAS_NULL>(data, nullmask,
	                                                         result, count);
	if (part > count) {
		return;
	}
	templated_quicksort_inplace<T, OP, HAS_NULL>(data, nullmask, result, 0,
	                                             part);
	templated_quicksort_inplace<T, OP, HAS_NULL>(data, nullmask, result,
	                                             part + 1, count - 1);
}

template <class T, class OP>
static void templated_quicksort(Vector &vector, sel_t result[]) {
	if (vector.count == 0)
		return;
	auto data = (T *)vector.data;
	if (vector.nullmask.any()) {
		// quicksort with nulls
		templated_quicksort<T, OP, true>(data, vector.nullmask, vector.count,
		                                 result);
	} else {
		// quicksort without nulls
		templated_quicksort<T, OP, false>(data, vector.nullmask, vector.count,
		                                  result);
	}
}

void VectorOperations::Sort(Vector &vector, sel_t result[]) {
	if (vector.sel_vector) {
		throw Exception(
		    "Cannot sort a vector with a sel_vector, call Flatten() first");
	}
	// now we order
	switch (vector.type) {
	case TypeId::TINYINT:
		templated_quicksort<int8_t, operators::LessThanEquals>(vector, result);
		break;
	case TypeId::SMALLINT:
		templated_quicksort<int16_t, operators::LessThanEquals>(vector, result);
		break;
	case TypeId::DATE:
	case TypeId::INTEGER:
		templated_quicksort<int32_t, operators::LessThanEquals>(vector, result);
		break;
	case TypeId::TIMESTAMP:
	case TypeId::BIGINT:
		templated_quicksort<int64_t, operators::LessThanEquals>(vector, result);
		break;
	case TypeId::DECIMAL:
		templated_quicksort<double, operators::LessThanEquals>(vector, result);
		break;
	case TypeId::VARCHAR:
		templated_quicksort<const char *, operators::LessThanEquals>(vector,
		                                                             result);
		break;
	default:
		throw NotImplementedException("Unimplemented type for sort");
	}
}