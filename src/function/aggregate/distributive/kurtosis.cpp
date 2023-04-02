#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include <cstdint>
#include <limits>
#include <type_traits>
using std::uint64_t;

namespace duckdb {

struct KurtosisState {
	idx_t n;
	double m1;
	double m2;
	double m3;
	double m4;
};

struct KurtosisFlagBiasCorrection {};

struct KurtosisFlagNoBiasCorrection {};

template <class KURTOSIS_FLAG>
struct KurtosisOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->n = 0;
		state->m1 = state->m2 = state->m3 = state->m4 = 0.0;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *input,
	                              ValidityMask &mask, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, aggr_input_data, input, mask, 0);
		}
	}

	/*
	Formula here is taken from wikipedia:
	    https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
	But adds a bias correction at the end in the case of sample kurtosis.
	A reference for the correction can be found on:
	Joanes, Derrick N., and Christine A. Gill.
	"Comparing measures of sample skewness and kurtosis."
	Journal of the Royal Statistical Society: Series D (The Statistician) 47.1 (1998): 183-189.
	*/
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *data, ValidityMask &mask, idx_t idx) {
		const double n1 = state->n;
		state->n++;

		const double delta = data[idx] - state->m1;
		const double delta_n = delta / state->n;
		const double delta_n2 = delta_n * delta_n;
		const double term1 = delta * delta_n * n1;
		// Note: for n<=2^32, this can be calculated more precisely with integer types
		double n_poly;
		if (state->n <= UINT32_MAX) {
			const uint64_t n_u64 = state->n;
			n_poly = n_u64 * n_u64 - uint64_t(3) * n_u64 + uint64_t(3);
		} else {
			const double n_dbl = state->n;
			n_poly = n_dbl * n_dbl - 3.0 * n_dbl + 3.0;
		}

		state->m1 += delta_n;
		state->m4 += term1 * delta_n2 * n_poly + 6.0 * delta_n2 * state->m2 - 4.0 * delta_n * state->m3;
		state->m3 += term1 * delta_n * (state->n - idx_t(2)) - 3.0 * delta_n * state->m2;
		state->m2 += term1;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		if (source.n == 0) {
			return;
		}
		target->n += source.n;
		target->m1 += source.m1;
		target->m2 += source.m2;
		target->m3 += source.m3;
		target->m4 += source.m4;
	}

	template <class TARGET_TYPE, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, TARGET_TYPE *target, ValidityMask &mask,
	                     idx_t idx) {
		if (state->n <= 3 || !state->m2) {
			mask.SetInvalid(idx);
			return;
		}

		const idx_t n1 = state->n - idx_t(1);
		if (((state->m2 * state->m2) - 3 * n1) == 0) { // LCOV_EXCL_START
			mask.SetInvalid(idx);
		} // LCOV_EXCL_STOP

		if (std::is_same<KURTOSIS_FLAG, KurtosisFlagNoBiasCorrection>::value) {
			target[idx] = (state->n * state->m4) / (state->m2 * state->m2) - 3.0;
		} else {
			const double g2 = (state->n * state->m4) / (state->m2 * state->m2);
			const double cdiff = 3.0 * (state->n - idx_t(1));
			double ratio;
			if (state->n <= UINT32_MAX) {
				const uint64_t n_u64 = state->n;
				const uint64_t div = (n_u64 - uint64_t(2)) * (n_u64 - uint64_t(3));
				ratio = static_cast<double>(n_u64 - uint64_t(1)) / static_cast<double>(div);
			} else {
				const double n_dbl = state->n;
				ratio = (n_dbl - 1.0) / ((n_dbl - 2.0) * (n_dbl - 3.0));
			}

			target[idx] = ratio * ((state->n + 1.0) * g2 - cdiff);
		}

		if (!Value::DoubleIsFinite(target[idx])) {
			throw OutOfRangeException("Kurtosis is out of range!");
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

void KurtosisFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet kurtosis_fun("kurtosis");
	kurtosis_fun.AddFunction(
	    AggregateFunction::UnaryAggregate<KurtosisState, double, double, KurtosisOperation<KurtosisFlagBiasCorrection>>(
	        LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(kurtosis_fun);

	AggregateFunctionSet kurtosis_samp_fun("kurtosis_samp");
	kurtosis_samp_fun.AddFunction(
	    AggregateFunction::UnaryAggregate<KurtosisState, double, double, KurtosisOperation<KurtosisFlagBiasCorrection>>(
	        LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(kurtosis_samp_fun);

	AggregateFunctionSet kurtosis_pop_fun("kurtosis_pop");
	kurtosis_pop_fun.AddFunction(AggregateFunction::UnaryAggregate<KurtosisState, double, double,
	                                                               KurtosisOperation<KurtosisFlagNoBiasCorrection>>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(kurtosis_pop_fun);
}

} // namespace duckdb
