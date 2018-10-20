
#include "common/exception.hpp"
#include "common/types/hash.hpp"
#include "common/types/null_value.hpp"
#include "common/types/operators.hpp"
#include "common/types/vector_operations.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Templated Looping Functions
//===--------------------------------------------------------------------===//
template <class T, class OP>
void _scatter_templated_loop(Vector &source, Vector &dest) {
	auto ldata = (T *)source.data;
	auto destination = (T **)dest.data;
	if (source.IsConstant()) {
		// special case: source is a constant
		if (source.nullmask[0]) {
			return;
		}

		auto constant = ldata[0];
		VectorOperations::Exec(dest, [&](size_t i, size_t k) {
			if (!IsNullValue<T>(destination[i][0])) {
				destination[i][0] = OP::Operation(
				    constant, destination[i][0]);
			} else {
				destination[i][0] = constant;
			}
		});
	} else if (source.count == dest.count) {
		// source and dest are equal-length vectors
		assert(dest.sel_vector == source.sel_vector);
		VectorOperations::Exec(dest, [&](size_t i, size_t k) {
			if (!source.nullmask[i]) {
				if (!IsNullValue<T>(destination[i][0])) {
					destination[i][0] =
					    OP::Operation(ldata[i], destination[i][0]);
				} else {
					destination[i][0] = ldata[i];
				}
			}
		});
	} else {
		throw Exception("Could not scatter to all destination spots!");
	}
}

template <class T, class OP>
void _gather_templated_loop(Vector &src, Vector &result) {
	T **source = (T **)src.data;
	T *ldata = (T *)result.data;
	for (size_t i = 0; i < result.count; i++) {
		if (IsNullValue<T>(source[i][0])) {
			result.nullmask.set(i);
		} else {
			ldata[i] = OP::Operation(source[i][0], ldata[i]);
		}
	}
}

template <class OP>
static void _generic_scatter_loop(Vector &source, Vector &dest) {
	if (dest.type != TypeId::POINTER) {
		throw InvalidTypeException(dest.type,
		                           "Cannot scatter to non-pointer type!");
	}
	switch (source.type) {
	case TypeId::TINYINT:
		_scatter_templated_loop<int8_t, OP>(source, dest);
		break;
	case TypeId::SMALLINT:
		_scatter_templated_loop<int16_t, OP>(source, dest);
		break;
	case TypeId::INTEGER:
		_scatter_templated_loop<int32_t, OP>(source, dest);
		break;
	case TypeId::BIGINT:
		_scatter_templated_loop<int64_t, OP>(source, dest);
		break;
	case TypeId::DECIMAL:
		_scatter_templated_loop<double, OP>(source, dest);
		break;
	case TypeId::POINTER:
		_scatter_templated_loop<uint64_t, OP>(source, dest);
		break;
	case TypeId::DATE:
		_scatter_templated_loop<date_t, OP>(source, dest);
		break;
	default:
		throw NotImplementedException("Unimplemented type for scatter");
	}
}

template <class OP>
static void _generic_gather_loop(Vector &source, Vector &dest) {
	if (source.type != TypeId::POINTER) {
		throw InvalidTypeException(source.type,
		                           "Cannot gather from non-pointer type!");
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
	_generic_scatter_loop<operators::PickLeft>(source, dest);
}

void VectorOperations::Scatter::Add(Vector &source, Vector &dest) {
	_generic_scatter_loop<operators::Addition>(source, dest);
}

void VectorOperations::Scatter::Max(Vector &source, Vector &dest) {
	_generic_scatter_loop<operators::Max>(source, dest);
}

void VectorOperations::Scatter::Min(Vector &source, Vector &dest) {
	_generic_scatter_loop<operators::Min>(source, dest);
}

void VectorOperations::Scatter::AddOne(Vector &source, Vector &dest) {
	_generic_scatter_loop<operators::AddOne>(source, dest);
}

void VectorOperations::Gather::Set(Vector &source, Vector &dest) {
	_generic_gather_loop<operators::PickLeft>(source, dest);
}

void VectorOperations::Scatter::SetFirst(Vector &source, Vector &dest) {
	_generic_scatter_loop<operators::PickRight>(source, dest);
}
