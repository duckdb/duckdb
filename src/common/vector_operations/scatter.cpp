//===--------------------------------------------------------------------===//
// scatter.cpp
// Description: This file contains the implementation of the scatter operations
//===--------------------------------------------------------------------===//

#include "common/operator/aggregate_operators.hpp"
#include "common/operator/constant_operators.hpp"
#include "common/operator/numeric_binary_operators.hpp"
#include "common/vector_operations/scatter_loops.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class OP> static void generic_scatter_loop(Vector &source, Vector &dest) {
	if (dest.type != TypeId::POINTER) {
		throw InvalidTypeException(dest.type, "Cannot scatter to non-pointer type!");
	}
	switch (source.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		scatter_templated_loop<int8_t, OP>(source, dest);
		break;
	case TypeId::SMALLINT:
		scatter_templated_loop<int16_t, OP>(source, dest);
		break;
	case TypeId::INTEGER:
		scatter_templated_loop<int32_t, OP>(source, dest);
		break;
	case TypeId::BIGINT:
		scatter_templated_loop<int64_t, OP>(source, dest);
		break;
	case TypeId::FLOAT:
		scatter_templated_loop<float, OP>(source, dest);
		break;
	case TypeId::DOUBLE:
		scatter_templated_loop<double, OP>(source, dest);
		break;
	default:
		throw NotImplementedException("Unimplemented type for scatter");
	}
}

void VectorOperations::Scatter::Set(Vector &source, Vector &dest) {
	if (source.type == TypeId::VARCHAR) {
		scatter_templated_loop<char *, duckdb::PickLeft>(source, dest);
	} else {
		generic_scatter_loop<duckdb::PickLeft>(source, dest);
	}
}

void VectorOperations::Scatter::SetFirst(Vector &source, Vector &dest) {
	if (source.type == TypeId::VARCHAR) {
		scatter_templated_loop<char *, duckdb::PickRight>(source, dest);
	} else {
		generic_scatter_loop<duckdb::PickRight>(source, dest);
	}
}

void VectorOperations::Scatter::Add(Vector &source, Vector &dest) {
	generic_scatter_loop<duckdb::Add>(source, dest);
}

void VectorOperations::Scatter::Max(Vector &source, Vector &dest) {
	generic_scatter_loop<duckdb::Max>(source, dest);
}

void VectorOperations::Scatter::Min(Vector &source, Vector &dest) {
	generic_scatter_loop<duckdb::Min>(source, dest);
}

void VectorOperations::Scatter::AddOne(Vector &source, Vector &dest) {
	assert(dest.type == TypeId::POINTER);
	auto destinations = (int64_t **)dest.data;
	VectorOperations::Exec(source, [&](index_t i, index_t k) {
		if (!source.nullmask[i]) {
			(*destinations[i])++;
		}
	});
}

template <class T> static void scatter_set_loop(Vector &source, Vector &dest) {
	auto data = (T*) source.data;
	auto destination = (T**) dest.data;
	VectorOperations::Exec(source, [&](index_t i, index_t k) {
		*destination[i] = data[i];
	});
}

void VectorOperations::Scatter::SetAll(Vector &source, Vector &dest) {
	if (dest.type != TypeId::POINTER) {
		throw InvalidTypeException(dest.type, "Cannot scatter to non-pointer type!");
	}
	switch (source.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		scatter_set_loop<int8_t>(source, dest);
		break;
	case TypeId::SMALLINT:
		scatter_set_loop<int16_t>(source, dest);
		break;
	case TypeId::INTEGER:
		scatter_set_loop<int32_t>(source, dest);
		break;
	case TypeId::BIGINT:
		scatter_set_loop<int64_t>(source, dest);
		break;
	case TypeId::FLOAT:
		scatter_set_loop<float>(source, dest);
		break;
	case TypeId::DOUBLE:
		scatter_set_loop<double>(source, dest);
		break;
	case TypeId::VARCHAR:
		scatter_set_loop<const char*>(source, dest);
		break;
	default:
		throw NotImplementedException("Unimplemented type for scatter");
	}
}
