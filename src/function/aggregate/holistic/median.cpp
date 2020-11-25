#include "duckdb/function/aggregate/holistic_functions.hpp"
#include <algoritm>

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

	// TODO
	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		throw NotImplementedException("COMBINE not implemented for MEDIAN");
	}

	// TODO simple update

	template <class STATE> static void Destroy(STATE *state) {
		if (state->v) {
			delete state->v;
		}
	}
	static bool IgnoreNull() {
		return true;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, nullmask, 0);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *data, nullmask_t &nullmask, idx_t idx) {
		if (nullmask[idx]) {
			return;
		}
		if (state->pos == state->len) {
			median_state_t old_state;
			old_state.pos = 0;
			if (state->pos > 0) {
				old_state = *state;
			}
			// growing conservatively here since we could be running this on many small groups
			state->len = state->len == 0 ? 1 : state->len * 2;
			state->v = new double[state->len];
			if (old_state.pos > 0) {
				memcpy(state->v, old_state.v, old_state.pos * sizeof(double));
				Destroy(&old_state);
			}
		}
		D_ASSERT(state->v);
		state->v[state->pos++] = data[idx];
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (state->pos == 0) {
			nullmask[idx] = true;
			return;
		}
		std::nth_element(state->v, state->v + (state->pos / 2), state->v + state->pos);
		target[idx] = state->v[state->pos / 2];
	}
};

void MedianFun::RegisterFunction(BuiltinFunctions &set) {
	auto median = AggregateFunction("median", {LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                                AggregateFunction::StateSize<median_state_t>,
	                                AggregateFunction::StateInitialize<median_state_t, MedianFunction>,
	                                AggregateFunction::UnaryScatterUpdate<median_state_t, double, MedianFunction>,
	                                AggregateFunction::StateCombine<median_state_t, MedianFunction>,
	                                AggregateFunction::StateFinalize<median_state_t, double, MedianFunction>, nullptr,
	                                nullptr, AggregateFunction::StateDestroy<median_state_t, MedianFunction>);
	set.AddFunction(median);
}

} // namespace duckdb
