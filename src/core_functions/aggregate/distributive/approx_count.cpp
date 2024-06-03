#include "duckdb/core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/hyperloglog.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

#include "hyperloglog.hpp"

#include <math.h>

namespace duckdb {

// Algorithms from
// "New cardinality estimation algorithms for HyperLogLog sketches"
// Otmar Ertl, arXiv:1702.01284
struct ApproxDistinctCountState {
	static constexpr idx_t P = 6;
	static constexpr idx_t Q = 64 - P;
	static constexpr idx_t M = 1 << P;
	static constexpr double ALPHA = 0.721347520444481703680; // 1 / (2 log(2))

	ApproxDistinctCountState() {
		::memset(k, 0, sizeof(k));
	}

	//! Taken from https://stackoverflow.com/a/72088344
	static inline uint8_t CountTrailingZeros(const uint64_t &x) {
		static constexpr const uint64_t DEBRUIJN = 0x03f79d71b4cb0a89;
		static constexpr const uint8_t LOOKUP[] = {0,  47, 1,  56, 48, 27, 2,  60, 57, 49, 41, 37, 28, 16, 3,  61,
		                                           54, 58, 35, 52, 50, 42, 21, 44, 38, 32, 29, 23, 17, 11, 4,  62,
		                                           46, 55, 26, 59, 40, 36, 15, 53, 34, 51, 20, 43, 31, 22, 10, 45,
		                                           25, 39, 14, 33, 19, 30, 9,  24, 13, 18, 8,  12, 7,  6,  5,  63};
		return LOOKUP[(DEBRUIJN * (x ^ (x - 1))) >> 58];
	}

	inline void Update(const idx_t &i, const uint8_t &z) {
		k[i] = MaxValue<uint8_t>(k[i], z);
	}

	//! Algorithm 1
	inline void InsertElement(hash_t h) {
		const auto i = h & ((1 << P) - 1);
		h >>= P;
		h |= hash_t(1) << Q;
		const uint8_t z = CountTrailingZeros(h) + 1;
		Update(i, z);
	}

	//! Algorithm 2
	inline void Merge(const ApproxDistinctCountState &other) {
		for (idx_t i = 0; i < M; ++i) {
			Update(i, other.k[i]);
		}
	}

	//! Algorithm 4
	void ExtractCounts(uint32_t *c) const {
		for (idx_t i = 0; i < M; ++i) {
			c[k[i]]++;
		}
	}

	//! Algorithm 6
	static int64_t EstimateCardinality(uint32_t *c) {
		auto z = M * duckdb_hll::hllTau((M - c[Q]) / double(M));

		for (idx_t k = Q; k >= 1; --k) {
			z += c[k];
			z *= 0.5;
		}

		z += M * duckdb_hll::hllSigma(c[0] / double(M));

		return llroundl(ALPHA * M * M / z);
	}

	idx_t Count() const {
		uint32_t c[Q + 2] = {0};
		ExtractCounts(c);
		return idx_t(EstimateCardinality(c));
	}

	uint8_t k[M];
};

struct ApproxCountDistinctFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target.Merge(source);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		target = UnsafeNumericCast<T>(state.Count());
	}

	static bool IgnoreNull() {
		return true;
	}
};

static void ApproxCountDistinctSimpleUpdateFunction(Vector inputs[], AggregateInputData &, idx_t input_count,
                                                    data_ptr_t state, idx_t count) {
	D_ASSERT(input_count == 1);
	auto &input = inputs[0];
	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);

	if (count > STANDARD_VECTOR_SIZE) {
		throw InternalException("ApproxCountDistinct - count must be at most vector size");
	}
	Vector hash_vec(LogicalType::HASH, count);
	VectorOperations::Hash(input, hash_vec, count);

	UnifiedVectorFormat hdata;
	hash_vec.ToUnifiedFormat(count, hdata);
	const auto *hashes = UnifiedVectorFormat::GetData<hash_t>(hdata);
	auto agg_state = reinterpret_cast<ApproxDistinctCountState *>(state);

	if (hash_vec.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (idata.validity.RowIsValid(0)) {
			agg_state->InsertElement(hashes[0]);
		}
	} else {
		for (idx_t i = 0; i < count; ++i) {
			if (idata.validity.RowIsValid(idata.sel->get_index(i))) {
				const auto hash = hashes[hdata.sel->get_index(i)];
				agg_state->InsertElement(hash);
			}
		}
	}
}

static void ApproxCountDistinctUpdateFunction(Vector inputs[], AggregateInputData &, idx_t input_count,
                                              Vector &state_vector, idx_t count) {
	D_ASSERT(input_count == 1);
	auto &input = inputs[0];
	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);

	if (count > STANDARD_VECTOR_SIZE) {
		throw InternalException("ApproxCountDistinct - count must be at most vector size");
	}
	Vector hash_vec(LogicalType::HASH, count);
	VectorOperations::Hash(input, hash_vec, count);

	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	const auto states = UnifiedVectorFormat::GetDataNoConst<ApproxDistinctCountState *>(sdata);

	UnifiedVectorFormat hdata;
	hash_vec.ToUnifiedFormat(count, hdata);
	const auto *hashes = UnifiedVectorFormat::GetData<hash_t>(hdata);
	for (idx_t i = 0; i < count; i++) {
		if (idata.validity.RowIsValid(idata.sel->get_index(i))) {
			auto agg_state = states[sdata.sel->get_index(i)];
			const auto hash = hashes[hdata.sel->get_index(i)];
			agg_state->InsertElement(hash);
		}
	}
}

AggregateFunction GetApproxCountDistinctFunction(const LogicalType &input_type) {
	auto fun = AggregateFunction(
	    {input_type}, LogicalTypeId::BIGINT, AggregateFunction::StateSize<ApproxDistinctCountState>,
	    AggregateFunction::StateInitialize<ApproxDistinctCountState, ApproxCountDistinctFunction>,
	    ApproxCountDistinctUpdateFunction,
	    AggregateFunction::StateCombine<ApproxDistinctCountState, ApproxCountDistinctFunction>,
	    AggregateFunction::StateFinalize<ApproxDistinctCountState, int64_t, ApproxCountDistinctFunction>,
	    ApproxCountDistinctSimpleUpdateFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

AggregateFunction ApproxCountDistinctFun::GetFunction() {
	return GetApproxCountDistinctFunction(LogicalType::ANY);
}

} // namespace duckdb
