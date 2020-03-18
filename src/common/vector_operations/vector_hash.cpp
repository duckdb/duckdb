//===--------------------------------------------------------------------===//
// hash.cpp
// Description: This file contains the vectorized hash implementations
//===--------------------------------------------------------------------===//

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/null_value.hpp"

using namespace duckdb;
using namespace std;

struct HashOp {
	template <class T> static inline uint64_t Operation(T input, bool is_null) {
		return duckdb::Hash<T>(is_null ? duckdb::NullValue<T>() : input);
	}
};

template <class T>
static inline void tight_loop_hash(T *__restrict ldata, uint64_t *__restrict result_data, idx_t count,
                                   const SelectionVector *__restrict sel_vector, nullmask_t &nullmask) {
	if (nullmask.any()) {
		for(idx_t i = 0; i < count; i++) {
			auto idx = sel_vector->get_index(i);
			result_data[i] = HashOp::Operation(ldata[idx], nullmask[idx]);
		}
	} else {
		for(idx_t i = 0; i < count; i++) {
			auto idx = sel_vector->get_index(i);
			result_data[i] = duckdb::Hash<T>(ldata[idx]);
		}
	}
}

template <class T> void templated_loop_hash(Vector &input, Vector &result) {

	if (input.vector_type == VectorType::CONSTANT_VECTOR) {
		result.vector_type = VectorType::CONSTANT_VECTOR;

		auto ldata = ConstantVector::GetData<T>(input);
		auto result_data = ConstantVector::GetData<uint64_t>(result);
		result_data[0] = HashOp::Operation(ldata[0], ConstantVector::IsNull(input));
	} else {
		result.vector_type = VectorType::FLAT_VECTOR;

		VectorData idata;
		input.Orrify(idata);

		tight_loop_hash<T>((T*) idata.data, FlatVector::GetData<uint64_t>(result), input.size(), idata.sel, *idata.nullmask);
	}
}

void VectorOperations::Hash(Vector &input, Vector &result) {
	assert(result.type == TypeId::HASH);
	assert(input.SameCardinality(result));
	switch (input.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		templated_loop_hash<int8_t>(input, result);
		break;
	case TypeId::INT16:
		templated_loop_hash<int16_t>(input, result);
		break;
	case TypeId::INT32:
		templated_loop_hash<int32_t>(input, result);
		break;
	case TypeId::INT64:
		templated_loop_hash<int64_t>(input, result);
		break;
	case TypeId::FLOAT:
		templated_loop_hash<float>(input, result);
		break;
	case TypeId::DOUBLE:
		templated_loop_hash<double>(input, result);
		break;
	case TypeId::VARCHAR:
		templated_loop_hash<string_t>(input, result);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for hash");
	}
}

static inline uint64_t combine_hash(uint64_t a, uint64_t b) {
	return (a * UINT64_C(0xbf58476d1ce4e5b9)) ^ b;
}

template <class T>
static inline void tight_loop_combine_hash(T *__restrict ldata, uint64_t *__restrict hash_data, idx_t count,
                                          const SelectionVector *__restrict sel_vector, nullmask_t &nullmask) {
	if (nullmask.any()) {
		for(idx_t i = 0; i < count; i++) {
			auto idx = sel_vector->get_index(i);
			auto other_hash = HashOp::Operation(ldata[idx], nullmask[idx]);
			hash_data[i] = combine_hash(hash_data[i], other_hash);
		}
	} else {
		for(idx_t i = 0; i < count; i++) {
			auto idx = sel_vector->get_index(i);
			auto other_hash = duckdb::Hash<T>(ldata[idx]);
			hash_data[i] = combine_hash(hash_data[i], other_hash);
		}
	}
}

template <class T> void templated_loop_combine_hash(Vector &input, Vector &hashes) {
	if (input.vector_type == VectorType::CONSTANT_VECTOR && hashes.vector_type == VectorType::CONSTANT_VECTOR) {
		auto ldata = ConstantVector::GetData<T>(input);
		auto hash_data = ConstantVector::GetData<uint64_t>(hashes);

		auto other_hash = HashOp::Operation(ldata[0], ConstantVector::IsNull(input));
		hash_data[0] = combine_hash(hash_data[0], other_hash);
	} else {
		VectorData idata;

		input.Orrify(idata);
		hashes.Normalify();

		tight_loop_combine_hash<T>((T *)idata.data, FlatVector::GetData<uint64_t>(hashes), input.size(), idata.sel, *idata.nullmask);
	}
}

void VectorOperations::CombineHash(Vector &hashes, Vector &input) {
	assert(hashes.type == TypeId::HASH);
	assert(input.SameCardinality(hashes));
	switch (input.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		templated_loop_combine_hash<int8_t>(input, hashes);
		break;
	case TypeId::INT16:
		templated_loop_combine_hash<int16_t>(input, hashes);
		break;
	case TypeId::INT32:
		templated_loop_combine_hash<int32_t>(input, hashes);
		break;
	case TypeId::INT64:
		templated_loop_combine_hash<int64_t>(input, hashes);
		break;
	case TypeId::FLOAT:
		templated_loop_combine_hash<float>(input, hashes);
		break;
	case TypeId::DOUBLE:
		templated_loop_combine_hash<double>(input, hashes);
		break;
	case TypeId::VARCHAR:
		templated_loop_combine_hash<string_t>(input, hashes);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for hash");
	}
}
