#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

struct kurtosis_state_t {
	idx_t n;
	double M1, M2, M3, M4;
};

struct KurtosisOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->n = 0;
		state->M1 = state->M2 = state->M3 = state->M4 = 0.0;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, nullmask_t &nullmask,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, bind_data, input, nullmask, 0);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data_, INPUT_TYPE *data, nullmask_t &nullmask, idx_t idx) {
		if (nullmask[idx]) {
			return;
		}
		double delta, delta_n, delta_n2, term1;
		idx_t n1 = state->n;
		state->n++;
		delta = data[idx] - state->M1;
		delta_n = delta / state->n;
		delta_n2 = delta_n * delta_n;
		term1 = delta * delta_n * n1;
		state->M1 += delta_n;
		state->M4 += term1 * delta_n2 * (state->n * state->n - 3 * state->n + 3) + 6 * delta_n2 * state->M2 -
		             4 * delta_n * state->M3;
		state->M3 += term1 * delta_n * (state->n - 2) - 3 * delta_n * state->M2;
		state->M2 += term1;
	}

	template <class STATE, class OP>
	static void Combine(STATE source, STATE *target) {
		if (source.n == 0) {
			return;
		}
		if (target->n == 0) {
			target->n = source.n;
            target->M1 = source.M1;
            target->M2 = source.M2;
            target->M3 = source.M3;
            target->M4 = source.M4;
			return;
		}
		double combined_n = source.n + target->n;

		double delta = target->M1 - source.M1;
		double delta2 = delta * delta;
		double delta3 = delta * delta2;
		double delta4 = delta2 * delta2;

		double combined_M1 = (source.n * source.M1 + target->n * target->M1) / combined_n;

		double combined_M2 = source.M2 + target->M2 + delta2 * source.n * target->n / combined_n;

		double combined_M3 =
		    source.M3 + target->M3 + delta3 * source.n * target->n * (source.n - target->n) / (combined_n * combined_n);
		combined_M3 += 3.0 * delta * (source.n * target->M2 - target->n * source.M2) / combined_n;

		double combined_M4 = source.M4 + target->M4 +
		                     delta4 * source.n * target->n *
		                         (source.n * source.n - source.n * target->n + target->n * target->n) /
		                         (combined_n * combined_n * combined_n);
		combined_M4 += 6.0 * delta2 * (source.n * source.n * target->M2 + target->n * target->n * source.M2) /
		                   (combined_n * combined_n) +
		               4.0 * delta * (source.n * target->M3 - target->n * source.M3) / combined_n;

		target->n = combined_n;
		target->M1 = combined_M1;
		target->M2 = combined_M2;
		target->M3 = combined_M3;
		target->M4 = combined_M4;
	}

	template <class TARGET_TYPE, class STATE>
	static void Finalize(Vector &result, FunctionData *bind_data_, STATE *state, TARGET_TYPE *target,
	                     nullmask_t &nullmask, idx_t idx) {
		auto n = (double) state->n;
		if (n == 0 || ((n-1) * (n-2) * (n-3)) == 0 || pow(sqrt(state->M2/(n-1.0)),4) == 0 || ((n -2) * (n-3)) == 0) {
			nullmask[idx] = true;
			return;
		}
		target[idx] = ((n * (n+1))/ ((n-1) * (n-2) * (n-3)));
        target[idx] *= state->M4/ pow(state->M2/(n-1.0),2);
		target[idx] -= 3*(pow(n -1,2)/ ((n -2) * (n-3)));
	}

	static bool IgnoreNull() {
		return false;
	}
};


void KurtosisFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet functionSet("kurtosis");
	functionSet.AddFunction(AggregateFunction::UnaryAggregate<kurtosis_state_t, double, double, KurtosisOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(functionSet);
}

} // namespace duckdb