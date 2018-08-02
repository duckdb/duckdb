
#include "common/exception.hpp"
#include "common/types/hash.hpp"
#include "common/types/operators.hpp"
#include "common/types/vector_operations.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Templated Looping Functions
//===--------------------------------------------------------------------===//
template <class T, class OP>
void _scatter_templated_loop(Vector &source, T **destination, size_t count) {
	T *ldata = (T *)source.data;
	if (count == (size_t)-1) {
		if (source.sel_vector) {
			for (size_t i = 0; i < source.count; i++) {
				destination[i][0] = OP::Operation(ldata[source.sel_vector[i]],
				                                  destination[i][0]);
			}
		} else {
			for (size_t i = 0; i < source.count; i++) {
				destination[i][0] = OP::Operation(ldata[i], destination[i][0]);
			}
		}
	} else {
		for (size_t i = 0; i < count; i++) {
			destination[i][0] = OP::Operation(ldata[0], destination[i][0]);
		}
	}
}

template <class T, class OP>
void _gather_templated_loop(T **source, Vector &result) {
	T *ldata = (T *)result.data;
	for (size_t i = 0; i < result.count; i++) {
		ldata[i] = OP::Operation(source[i][0], ldata[i]);
	}
}

template <class OP>
static void _generic_scatter_loop(Vector &source, void **destination,
                                  size_t count) {
	switch (source.type) {
	case TypeId::TINYINT:
		_scatter_templated_loop<int8_t, OP>(source, (int8_t **)destination,
		                                    count);
		break;
	case TypeId::SMALLINT:
		_scatter_templated_loop<int16_t, OP>(source, (int16_t **)destination,
		                                     count);
		break;
	case TypeId::INTEGER:
		_scatter_templated_loop<int32_t, OP>(source, (int32_t **)destination,
		                                     count);
		break;
	case TypeId::BIGINT:
		_scatter_templated_loop<int64_t, OP>(source, (int64_t **)destination,
		                                     count);
		break;
	case TypeId::DECIMAL:
		_scatter_templated_loop<double, OP>(source, (double **)destination,
		                                    count);
		break;
	case TypeId::POINTER:
		_scatter_templated_loop<uint64_t, OP>(source, (uint64_t **)destination,
		                                      count);
		break;
	case TypeId::DATE:
		_scatter_templated_loop<date_t, OP>(source, (date_t **)destination,
		                                    count);
		break;
	default:
		throw NotImplementedException("Unimplemented type for scatter");
	}
}

template <class OP>
static void _generic_gather_loop(void **source, Vector &dest) {
	switch (dest.type) {
	case TypeId::TINYINT:
		_gather_templated_loop<int8_t, OP>((int8_t **)source, dest);
		break;
	case TypeId::SMALLINT:
		_gather_templated_loop<int16_t, OP>((int16_t **)source, dest);
		break;
	case TypeId::INTEGER:
		_gather_templated_loop<int32_t, OP>((int32_t **)source, dest);
		break;
	case TypeId::BIGINT:
		_gather_templated_loop<int64_t, OP>((int64_t **)source, dest);
		break;
	case TypeId::DECIMAL:
		_gather_templated_loop<double, OP>((double **)source, dest);
		break;
	case TypeId::POINTER:
		_gather_templated_loop<uint64_t, OP>((uint64_t **)source, dest);
		break;
	case TypeId::DATE:
		_gather_templated_loop<date_t, OP>((date_t **)source, dest);
		break;
	case TypeId::VARCHAR:
		_gather_templated_loop<char *, OP>((char ***)source, dest);
		break;
	default:
		throw NotImplementedException("Unimplemented type for gather");
	}
}

//===--------------------------------------------------------------------===//
// Scatter methods
//===--------------------------------------------------------------------===//
void VectorOperations::Scatter::Set(Vector &source, void **dest, size_t count) {
	_generic_scatter_loop<operators::PickLeft>(source, dest, count);
}

void VectorOperations::Scatter::Add(Vector &source, void **dest, size_t count) {
	_generic_scatter_loop<operators::Addition>(source, dest, count);
}

void VectorOperations::Scatter::Max(Vector &source, void **dest, size_t count) {
	_generic_scatter_loop<operators::Max>(source, dest, count);
}

void VectorOperations::Scatter::Min(Vector &source, void **dest, size_t count) {
	_generic_scatter_loop<operators::Min>(source, dest, count);
}

void VectorOperations::Gather::Set(void **source, Vector &dest) {
	_generic_gather_loop<operators::PickLeft>(source, dest);
}
