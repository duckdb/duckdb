//===--------------------------------------------------------------------===//
// scatter.cpp
// Description: This file contains the implementation of the scatter operations
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/aggregate_operators.hpp"
#include "duckdb/common/operator/constant_operators.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/common/vector_operations/scatter_loops.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class OP> static void numeric_scatter_loop(Vector &source, Vector &dest) {
	if (dest.type != TypeId::POINTER) {
		throw InvalidTypeException(dest.type, "Cannot scatter to non-pointer type!");
	}
	switch (source.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		scatter_templated_loop<int8_t, OP>(source, dest);
		break;
	case TypeId::INT16:
		scatter_templated_loop<int16_t, OP>(source, dest);
		break;
	case TypeId::INT32:
		scatter_templated_loop<int32_t, OP>(source, dest);
		break;
	case TypeId::INT64:
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

template <class OP> static void generic_scatter_loop(Vector &source, Vector &dest) {
	switch (source.type) {
	case TypeId::VARCHAR:
		scatter_templated_loop<string_t, OP>(source, dest);
		break;
	default:
		numeric_scatter_loop<OP>(source, dest);
		break;
	}
}

void VectorOperations::Scatter::Set(Vector &source, Vector &dest) {
	source.Normalify();
	if (source.type == TypeId::VARCHAR) {
		scatter_templated_loop<string_t, duckdb::PickLeft>(source, dest);
	} else {
		generic_scatter_loop<duckdb::PickLeft>(source, dest);
	}
}

void VectorOperations::Scatter::SetFirst(Vector &source, Vector &dest) {
	source.Normalify();
	if (source.type == TypeId::VARCHAR) {
		scatter_templated_loop<string_t, duckdb::PickRight>(source, dest);
	} else {
		generic_scatter_loop<duckdb::PickRight>(source, dest);
	}
}

void VectorOperations::Scatter::Add(Vector &source, Vector &dest) {
	source.Normalify();
	numeric_scatter_loop<duckdb::Add>(source, dest);
}

void VectorOperations::Scatter::Max(Vector &source, Vector &dest) {
	source.Normalify();
	generic_scatter_loop<duckdb::Max>(source, dest);
}

void VectorOperations::Scatter::Min(Vector &source, Vector &dest) {
	source.Normalify();
	generic_scatter_loop<duckdb::Min>(source, dest);
}

void VectorOperations::Scatter::AddOne(Vector &source, Vector &dest) {
	assert(dest.type == TypeId::POINTER);
	auto destinations = (int64_t **)dest.GetData();
	if (source.vector_type == VectorType::CONSTANT_VECTOR) {
		if (source.nullmask[0]) {
			// constant NULL, ignore value
			return;
		}
		VectorOperations::Exec(dest, [&](index_t i, index_t k) { (*destinations[i])++; });

	} else if (source.vector_type == VectorType::SEQUENCE_VECTOR) {
		VectorOperations::Exec(source, [&](index_t i, index_t k) {
			if (!source.nullmask[i]) {
				(*destinations[i])++;
			}
		});
	} else {
		source.Normalify();
		assert(source.vector_type == VectorType::FLAT_VECTOR);
		VectorOperations::Exec(source, [&](index_t i, index_t k) {
			if (!source.nullmask[i]) {
				(*destinations[i])++;
			}
		});
	}
}

template <class T, bool IGNORE_NULL> static void scatter_set_loop(Vector &source, data_ptr_t dest[], index_t offset) {
	auto data = (T *)source.GetData();
	if (source.vector_type == VectorType::CONSTANT_VECTOR) {
		if (!source.nullmask[0]) {
			VectorOperations::Exec(source, [&](index_t i, index_t k) {
				auto destination = (T *)(dest[i] + offset);
				*destination = data[0];
			});
		} else {
			VectorOperations::Exec(source, [&](index_t i, index_t k) {
				auto destination = (T *)(dest[i] + offset);
				*destination = NullValue<T>();
			});
		}
	} else {
		assert(source.vector_type == VectorType::FLAT_VECTOR);
		if (IGNORE_NULL || !source.nullmask.any()) {
			VectorOperations::Exec(source, [&](index_t i, index_t k) {
				auto destination = (T *)(dest[i] + offset);
				*destination = data[i];
			});
		} else {
			VectorOperations::Exec(source, [&](index_t i, index_t k) {
				auto destination = (T *)(dest[i] + offset);
				if (source.nullmask[i]) {
					*destination = NullValue<T>();
				} else {
					*destination = data[i];
				}
			});
		}
	}
}

template <bool IGNORE_NULL = false>
static void scatter_set_all_loop(Vector &source, data_ptr_t dest[], index_t offset) {
	switch (source.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		scatter_set_loop<int8_t, IGNORE_NULL>(source, dest, offset);
		break;
	case TypeId::INT16:
		scatter_set_loop<int16_t, IGNORE_NULL>(source, dest, offset);
		break;
	case TypeId::INT32:
		scatter_set_loop<int32_t, IGNORE_NULL>(source, dest, offset);
		break;
	case TypeId::INT64:
		scatter_set_loop<int64_t, IGNORE_NULL>(source, dest, offset);
		break;
	case TypeId::HASH:
		scatter_set_loop<uint64_t, IGNORE_NULL>(source, dest, offset);
		break;
	case TypeId::FLOAT:
		scatter_set_loop<float, IGNORE_NULL>(source, dest, offset);
		break;
	case TypeId::DOUBLE:
		scatter_set_loop<double, IGNORE_NULL>(source, dest, offset);
		break;
	case TypeId::VARCHAR:
		scatter_set_loop<string_t, IGNORE_NULL>(source, dest, offset);
		break;
	default:
		throw NotImplementedException("Unimplemented type for scatter");
	}
}

void VectorOperations::Scatter::SetAll(Vector &source, Vector &dest, bool set_null, index_t offset) {
	if (dest.type != TypeId::POINTER) {
		throw InvalidTypeException(dest.type, "Cannot scatter to non-pointer type!");
	}
	auto dest_data = (data_ptr_t *)dest.GetData();
	if (set_null) {
		scatter_set_all_loop<false>(source, dest_data, offset);
	} else {
		scatter_set_all_loop<true>(source, dest_data, offset);
	}
}
