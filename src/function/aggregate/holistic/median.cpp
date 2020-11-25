#include "duckdb/function/aggregate/holistic_functions.hpp"
#include <algorithm>

using namespace std;

namespace duckdb {

struct median_state_t {
	data_ptr_t v;
	idx_t len;
	idx_t pos;
};

template <class T> struct MedianOperation {
	template <class STATE> static void Initialize(STATE *state) {
		state->v = nullptr;
		state->len = 0;
		state->pos = 0;
	}

	static void resize_state(median_state_t *state, idx_t new_len) {
		if (new_len <= state->len) {
			return;
		}
		median_state_t old_state;
		old_state.pos = 0;
		if (state->pos > 0) {
			old_state = *state;
		}
		// growing conservatively here since we could be running this on many small groups
		state->len = new_len;
		state->v = (data_ptr_t) new T[state->len];
		if (old_state.pos > 0) {
			memcpy(state->v, old_state.v, old_state.pos * sizeof(T));
			Destroy(&old_state);
		}
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
			resize_state(state, state->len == 0 ? 1 : state->len * 2);
		}
		D_ASSERT(state->v);
		((T *)state->v)[state->pos++] = data[idx];
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		if (target->len == 0) {
			*target = source;
			return;
		}
		resize_state(target, target->pos + source.pos);
		memcpy(target->v + target->pos, source.v, source.pos);
		target->pos += source.pos;
	}

	template <class TARGET_TYPE, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, TARGET_TYPE *target, nullmask_t &nullmask,
	                     idx_t idx) {
		if (state->pos == 0) {
			nullmask[idx] = true;
			return;
		}
		D_ASSERT(state->v);
		std::nth_element(state->v, state->v + (state->pos / 2), state->v + state->pos);
		target[idx] = ((T *)state->v)[state->pos / 2];
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

void MedianFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet median("median");

	median.AddFunction(AggregateFunction::UnaryAggregate<median_state_t, float, float, MedianOperation<float>>(
	    LogicalType::FLOAT, LogicalType::FLOAT));

	median.AddFunction(AggregateFunction::UnaryAggregate<median_state_t, double, double, MedianOperation<double>>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));

	median.AddFunction(AggregateFunction::UnaryAggregate<median_state_t, int8_t, int8_t, MedianOperation<int8_t>>(
	    LogicalType::TINYINT, LogicalType::TINYINT));

	median.AddFunction(AggregateFunction::UnaryAggregate<median_state_t, int16_t, int16_t, MedianOperation<int16_t>>(
	    LogicalType::SMALLINT, LogicalType::SMALLINT));

	median.AddFunction(AggregateFunction::UnaryAggregate<median_state_t, int32_t, int32_t, MedianOperation<int32_t>>(
	    LogicalType::INTEGER, LogicalType::INTEGER));

	median.AddFunction(AggregateFunction::UnaryAggregate<median_state_t, int64_t, int64_t, MedianOperation<int64_t>>(
	    LogicalType::BIGINT, LogicalType::BIGINT));

	median.AddFunction(
	    AggregateFunction::UnaryAggregate<median_state_t, hugeint_t, hugeint_t, MedianOperation<hugeint_t>>(
	        LogicalType::HUGEINT, LogicalType::HUGEINT));

	set.AddFunction(median);
}

} // namespace duckdb
