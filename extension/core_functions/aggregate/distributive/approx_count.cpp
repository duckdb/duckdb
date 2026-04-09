#include <stdint.h>
#include <memory>
#include <new>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hyperloglog.hpp"
#include "core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {
struct AggregateFinalizeData;
struct AggregateInputData;

// Algorithms from
// "New cardinality estimation algorithms for HyperLogLog sketches"
// Otmar Ertl, arXiv:1702.01284
namespace {

struct ApproxDistinctCountState {
	HyperLogLogP<10> hll;
};

struct ApproxCountDistinctFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target.hll.Merge(source.hll);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		target = UnsafeNumericCast<T>(state.hll.Count());
	}

	static bool IgnoreNull() {
		return true;
	}
};

void ApproxCountDistinctSimpleUpdateFunction(Vector inputs[], AggregateInputData &, idx_t input_count, data_ptr_t state,
                                             idx_t count) {
	D_ASSERT(input_count == 1);
	auto &input = inputs[0];

	if (count > STANDARD_VECTOR_SIZE) {
		throw InternalException("ApproxCountDistinct - count must be at most vector size");
	}
	Vector hash_vec(LogicalType::HASH, count);
	VectorOperations::Hash(input, hash_vec, count);

	auto agg_state = reinterpret_cast<ApproxDistinctCountState *>(state);
	agg_state->hll.Update(input, hash_vec, count);
}

void ApproxCountDistinctUpdateFunction(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vector,
                                       idx_t count) {
	D_ASSERT(input_count == 1);
	auto &input = inputs[0];

	auto input_validity = input.Validity(count);

	if (count > STANDARD_VECTOR_SIZE) {
		throw InternalException("ApproxCountDistinct - count must be at most vector size");
	}
	Vector hash_vec(LogicalType::HASH, count);
	VectorOperations::Hash(input, hash_vec, count);

	auto states = state_vector.Values<ApproxDistinctCountState *>(count);
	auto hashes = hash_vec.Values<hash_t>(count);
	for (idx_t i = 0; i < count; i++) {
		if (!input_validity.IsValid(i)) {
			continue;
		}
		auto agg_state = states[i].GetValue();
		const auto hash = hashes[i].GetValue();
		agg_state->hll.InsertElement(hash);
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
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace

AggregateFunction ApproxCountDistinctFun::GetFunction() {
	return GetApproxCountDistinctFunction(LogicalType::ANY);
}

} // namespace duckdb
