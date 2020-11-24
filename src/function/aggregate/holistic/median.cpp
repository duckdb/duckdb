#include "duckdb/function/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/decimal.hpp"

using namespace std;

namespace duckdb {

struct median_state_t {
	double *v;
	idx_t len;
    idx_t pos;
};

struct MedianFunction {
	template <class STATE> static void Initialize(STATE *state) {
		state->v = nullptr;
		state->len = 0;
		state->pos = 0;
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		throw NotImplementedException("COMBINE not implemented for MEDIAN");
	}

	template <class STATE> static void Destroy(STATE *state) {
		if (state->v) {
			delete state->v;
		}
	}
	static bool IgnoreNull() {
		return true;
	}
};

static void median_update(Vector inputs[], idx_t input_count, Vector &state_vector, idx_t count) {
	D_ASSERT(input_count == 1);

	// TODO why do we need to orrify state vector?
	VectorData sdata;
	state_vector.Orrify(count, sdata);

	VectorData idata;
	inputs[0].Orrify(count, idata);

	auto states = (median_state_t **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
        if (idata.nullmask && (*idata.nullmask)[i]) {
            continue;
        }

		auto state = states[sdata.sel->get_index(i)];
		if (!state->v) {
            state->len = 42;
            state->v = new double[state->len];
		}
		auto val = ((double *)idata.data)[idata.sel->get_index(i)];
		state->v[state->pos++] = val;
	}
}

static void median_finalize(Vector &state_vector, FunctionData *, Vector &result, idx_t count) {
	VectorData sdata;
	state_vector.Orrify(count, sdata);
	auto states = (median_state_t **)sdata.data;

	result.Initialize(LogicalType::DOUBLE);
	auto result_ptr = FlatVector::GetData<double>(result);

	for (idx_t i = 0; i < count; i++) {
		auto state = states[sdata.sel->get_index(i)];
		D_ASSERT(state->v);

        std::sort(state->v, state->v + state->pos);
		result_ptr[i] = state->v[state->pos / 2];
	}
}

void MedianFun::RegisterFunction(BuiltinFunctions &set) {
	auto median = AggregateFunction(
	    "median", {LogicalType::DOUBLE}, LogicalType::DOUBLE, AggregateFunction::StateSize<median_state_t>,
	    AggregateFunction::StateInitialize<median_state_t, MedianFunction>, median_update, nullptr,
	    median_finalize, nullptr, nullptr, AggregateFunction::StateDestroy<median_state_t, MedianFunction>);
	set.AddFunction(median);
}

} // namespace duckdb
