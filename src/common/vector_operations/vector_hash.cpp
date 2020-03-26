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
	template <class T> static inline hash_t Operation(T input, bool is_null) {
		return duckdb::Hash<T>(is_null ? duckdb::NullValue<T>() : input);
	}
};

template <bool HAS_RSEL, class T>
static inline void tight_loop_hash(T *__restrict ldata, hash_t *__restrict result_data, const SelectionVector *rsel,
                                   idx_t count, const SelectionVector *__restrict sel_vector, nullmask_t &nullmask) {
	if (nullmask.any()) {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			result_data[ridx] = HashOp::Operation(ldata[idx], nullmask[idx]);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			result_data[ridx] = duckdb::Hash<T>(ldata[idx]);
		}
	}
}

template <bool HAS_RSEL, class T>
static inline void templated_loop_hash(Vector &input, Vector &result, const SelectionVector *rsel, idx_t count) {
	if (input.vector_type == VectorType::CONSTANT_VECTOR) {
		result.vector_type = VectorType::CONSTANT_VECTOR;

		auto ldata = ConstantVector::GetData<T>(input);
		auto result_data = ConstantVector::GetData<hash_t>(result);
		*result_data = HashOp::Operation(*ldata, ConstantVector::IsNull(input));
	} else {
		result.vector_type = VectorType::FLAT_VECTOR;

		VectorData idata;
		input.Orrify(count, idata);

		tight_loop_hash<HAS_RSEL, T>((T *)idata.data, FlatVector::GetData<hash_t>(result), rsel, count, idata.sel,
		                             *idata.nullmask);
	}
}

template <bool HAS_RSEL>
static inline void hash_type_switch(Vector &input, Vector &result, const SelectionVector *rsel, idx_t count) {
	assert(result.type == TypeId::HASH);
	switch (input.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		templated_loop_hash<HAS_RSEL, int8_t>(input, result, rsel, count);
		break;
	case TypeId::INT16:
		templated_loop_hash<HAS_RSEL, int16_t>(input, result, rsel, count);
		break;
	case TypeId::INT32:
		templated_loop_hash<HAS_RSEL, int32_t>(input, result, rsel, count);
		break;
	case TypeId::INT64:
		templated_loop_hash<HAS_RSEL, int64_t>(input, result, rsel, count);
		break;
	case TypeId::FLOAT:
		templated_loop_hash<HAS_RSEL, float>(input, result, rsel, count);
		break;
	case TypeId::DOUBLE:
		templated_loop_hash<HAS_RSEL, double>(input, result, rsel, count);
		break;
	case TypeId::VARCHAR:
		templated_loop_hash<HAS_RSEL, string_t>(input, result, rsel, count);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for hash");
	}
}

void VectorOperations::Hash(Vector &input, Vector &result, idx_t count) {
	hash_type_switch<false>(input, result, nullptr, count);
}

void VectorOperations::Hash(Vector &input, Vector &result, const SelectionVector &sel, idx_t count) {
	hash_type_switch<true>(input, result, &sel, count);
}

static inline hash_t combine_hash(hash_t a, hash_t b) {
	return (a * UINT64_C(0xbf58476d1ce4e5b9)) ^ b;
}

template <bool HAS_RSEL, class T>
static inline void tight_loop_combine_hash_constant(T *__restrict ldata, hash_t constant_hash,
                                                    hash_t *__restrict hash_data, const SelectionVector *rsel,
                                                    idx_t count, const SelectionVector *__restrict sel_vector,
                                                    nullmask_t &nullmask) {
	if (nullmask.any()) {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			auto other_hash = HashOp::Operation(ldata[idx], nullmask[idx]);
			hash_data[ridx] = combine_hash(constant_hash, other_hash);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			auto other_hash = duckdb::Hash<T>(ldata[idx]);
			hash_data[ridx] = combine_hash(constant_hash, other_hash);
		}
	}
}

template <bool HAS_RSEL, class T>
static inline void tight_loop_combine_hash(T *__restrict ldata, hash_t *__restrict hash_data,
                                           const SelectionVector *rsel, idx_t count,
                                           const SelectionVector *__restrict sel_vector, nullmask_t &nullmask) {
	if (nullmask.any()) {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			auto other_hash = HashOp::Operation(ldata[idx], nullmask[idx]);
			hash_data[ridx] = combine_hash(hash_data[ridx], other_hash);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			auto other_hash = duckdb::Hash<T>(ldata[idx]);
			hash_data[ridx] = combine_hash(hash_data[ridx], other_hash);
		}
	}
}

template <bool HAS_RSEL, class T>
void templated_loop_combine_hash(Vector &input, Vector &hashes, const SelectionVector *rsel, idx_t count) {
	if (input.vector_type == VectorType::CONSTANT_VECTOR && hashes.vector_type == VectorType::CONSTANT_VECTOR) {
		auto ldata = ConstantVector::GetData<T>(input);
		auto hash_data = ConstantVector::GetData<hash_t>(hashes);

		auto other_hash = HashOp::Operation(*ldata, ConstantVector::IsNull(input));
		*hash_data = combine_hash(*hash_data, other_hash);
	} else {
		VectorData idata;
		input.Orrify(count, idata);
		if (hashes.vector_type == VectorType::CONSTANT_VECTOR) {
			// mix constant with non-constant, first get the constant value
			auto constant_hash = *ConstantVector::GetData<hash_t>(hashes);
			// now re-initialize the hashes vector to an empty flat vector
			hashes.Initialize(hashes.type);
			tight_loop_combine_hash_constant<HAS_RSEL, T>((T *)idata.data, constant_hash,
			                                              FlatVector::GetData<hash_t>(hashes), rsel, count, idata.sel,
			                                              *idata.nullmask);
		} else {
			assert(hashes.vector_type == VectorType::FLAT_VECTOR);
			tight_loop_combine_hash<HAS_RSEL, T>((T *)idata.data, FlatVector::GetData<hash_t>(hashes), rsel, count,
			                                     idata.sel, *idata.nullmask);
		}
	}
}

template <bool HAS_RSEL>
static inline void combine_hash_type_switch(Vector &hashes, Vector &input, const SelectionVector *rsel, idx_t count) {
	assert(hashes.type == TypeId::HASH);
	switch (input.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		templated_loop_combine_hash<HAS_RSEL, int8_t>(input, hashes, rsel, count);
		break;
	case TypeId::INT16:
		templated_loop_combine_hash<HAS_RSEL, int16_t>(input, hashes, rsel, count);
		break;
	case TypeId::INT32:
		templated_loop_combine_hash<HAS_RSEL, int32_t>(input, hashes, rsel, count);
		break;
	case TypeId::INT64:
		templated_loop_combine_hash<HAS_RSEL, int64_t>(input, hashes, rsel, count);
		break;
	case TypeId::FLOAT:
		templated_loop_combine_hash<HAS_RSEL, float>(input, hashes, rsel, count);
		break;
	case TypeId::DOUBLE:
		templated_loop_combine_hash<HAS_RSEL, double>(input, hashes, rsel, count);
		break;
	case TypeId::VARCHAR:
		templated_loop_combine_hash<HAS_RSEL, string_t>(input, hashes, rsel, count);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for hash");
	}
}

void VectorOperations::CombineHash(Vector &hashes, Vector &input, idx_t count) {
	combine_hash_type_switch<false>(hashes, input, nullptr, count);
}

void VectorOperations::CombineHash(Vector &hashes, Vector &input, const SelectionVector &rsel, idx_t count) {
	combine_hash_type_switch<true>(hashes, input, &rsel, count);
}
