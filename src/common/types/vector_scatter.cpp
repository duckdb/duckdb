
#include "common/exception.hpp"
#include "common/types/hash.hpp"
#include "common/types/operators.hpp"
#include "common/types/vector_operations.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Templated Looping Functions
//===--------------------------------------------------------------------===//
template <class T, class OP, class EXEC>
void _scatter_templated_loop_handling(Vector &source, Vector &dest) {
	T *ldata = (T *)source.data;
	T **destination = (T **)dest.data;
	if (source.count == dest.count) {
		if (dest.sel_vector && source.sel_vector) {
			for (size_t i = 0; i < source.count; i++) {
				destination[dest.sel_vector[i]][0] =
				    EXEC::template Operation<T, T, T, OP>(
				        ldata[source.sel_vector[i]],
				        destination[dest.sel_vector[i]][0]);
			}
		} else if (dest.sel_vector) {
			for (size_t i = 0; i < source.count; i++) {
				destination[dest.sel_vector[i]][0] =
				    EXEC::template Operation<T, T, T, OP>(
				        ldata[i], destination[dest.sel_vector[i]][0]);
			}
		} else if (source.sel_vector) {
			for (size_t i = 0; i < source.count; i++) {
				destination[i][0] = EXEC::template Operation<T, T, T, OP>(
				    ldata[source.sel_vector[i]], destination[i][0]);
			}
		} else {
			for (size_t i = 0; i < source.count; i++) {
				destination[i][0] = EXEC::template Operation<T, T, T, OP>(
				    ldata[i], destination[i][0]);
			}
		}
	} else if (source.count == 1) {
		if (dest.sel_vector) {
			for (size_t i = 0; i < dest.count; i++) {
				destination[dest.sel_vector[i]][0] =
				    EXEC::template Operation<T, T, T, OP>(
				        ldata[0], destination[dest.sel_vector[i]][0]);
			}
		} else {
			for (size_t i = 0; i < dest.count; i++) {
				destination[i][0] = EXEC::template Operation<T, T, T, OP>(
				    ldata[0], destination[i][0]);
			}
		}
	} else {
		throw Exception("Could not scatter to all destination spots!");
	}
}
template <class T, class OP>
void _scatter_templated_loop(Vector &source, Vector &dest, bool ignore_nulls) {
	if (ignore_nulls) {
		_scatter_templated_loop_handling<T, OP, operators::ExecuteIgnoreNull>(
		    source, dest);
	} else {
		_scatter_templated_loop_handling<T, OP,
		                                 operators::ExecuteWithoutNullHandling>(
		    source, dest);
	}
}

template <class T, class OP>
void _gather_templated_loop(Vector &src, Vector &result) {
	T **source = (T **)src.data;
	T *ldata = (T *)result.data;
	for (size_t i = 0; i < result.count; i++) {
		ldata[i] = OP::Operation(source[i][0], ldata[i]);
	}
}

template <class OP>
static void _generic_scatter_loop(Vector &source, Vector &dest,
                                  bool ignore_nulls) {
	if (dest.type != TypeId::POINTER) {
		throw Exception("Cannot scatter to non-pointer type!");
	}
	switch (source.type) {
	case TypeId::TINYINT:
		_scatter_templated_loop<int8_t, OP>(source, dest, ignore_nulls);
		break;
	case TypeId::SMALLINT:
		_scatter_templated_loop<int16_t, OP>(source, dest, ignore_nulls);
		break;
	case TypeId::INTEGER:
		_scatter_templated_loop<int32_t, OP>(source, dest, ignore_nulls);
		break;
	case TypeId::BIGINT:
		_scatter_templated_loop<int64_t, OP>(source, dest, ignore_nulls);
		break;
	case TypeId::DECIMAL:
		_scatter_templated_loop<double, OP>(source, dest, ignore_nulls);
		break;
	case TypeId::POINTER:
		_scatter_templated_loop<uint64_t, OP>(source, dest, ignore_nulls);
		break;
	case TypeId::DATE:
		_scatter_templated_loop<date_t, OP>(source, dest, ignore_nulls);
		break;
	default:
		throw NotImplementedException("Unimplemented type for scatter");
	}
}

template <class OP>
static void _generic_gather_loop(Vector &source, Vector &dest) {
	if (source.type != TypeId::POINTER) {
		throw Exception("Cannot gather from non-pointer type!");
	}
	switch (dest.type) {
	case TypeId::TINYINT:
		_gather_templated_loop<int8_t, OP>(source, dest);
		break;
	case TypeId::SMALLINT:
		_gather_templated_loop<int16_t, OP>(source, dest);
		break;
	case TypeId::INTEGER:
		_gather_templated_loop<int32_t, OP>(source, dest);
		break;
	case TypeId::BIGINT:
		_gather_templated_loop<int64_t, OP>(source, dest);
		break;
	case TypeId::DECIMAL:
		_gather_templated_loop<double, OP>(source, dest);
		break;
	case TypeId::POINTER:
		_gather_templated_loop<uint64_t, OP>(source, dest);
		break;
	case TypeId::DATE:
		_gather_templated_loop<date_t, OP>(source, dest);
		break;
	case TypeId::VARCHAR:
		_gather_templated_loop<char *, OP>(source, dest);
		break;
	default:
		throw NotImplementedException("Unimplemented type for gather");
	}
}

//===--------------------------------------------------------------------===//
// Scatter methods
//===--------------------------------------------------------------------===//
void VectorOperations::Scatter::Set(Vector &source, Vector &dest) {
	_generic_scatter_loop<operators::PickLeft>(source, dest, false);
}

void VectorOperations::Scatter::Add(Vector &source, Vector &dest) {
	_generic_scatter_loop<operators::Addition>(source, dest, true);
}

void VectorOperations::Scatter::Max(Vector &source, Vector &dest) {
	_generic_scatter_loop<operators::Max>(source, dest, true);
}

void VectorOperations::Scatter::Min(Vector &source, Vector &dest) {
	_generic_scatter_loop<operators::Min>(source, dest, true);
}

void VectorOperations::Scatter::SetCount(Vector &source, Vector &dest) {
	_generic_scatter_loop<operators::SetCount>(source, dest, false);
}

void VectorOperations::Scatter::AddOne(Vector &source, Vector &dest) {
	_generic_scatter_loop<operators::AddOne>(source, dest, true);
}

void VectorOperations::Gather::Set(Vector &source, Vector &dest) {
	_generic_gather_loop<operators::PickLeft>(source, dest);
}
