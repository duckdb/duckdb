#include "core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

namespace {

struct KurtosisState {
	idx_t n;
	double sum;
	double sum_sqr;
	double sum_cub;
	double sum_four;
};

struct KurtosisFlagBiasCorrection {};

struct KurtosisFlagNoBiasCorrection {};

template <class KURTOSIS_FLAG>
struct KurtosisOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.n = 0;
		state.sum = state.sum_sqr = state.sum_cub = state.sum_four = 0.0;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		state.n++;
		state.sum += input;
		state.sum_sqr += pow(input, 2);
		state.sum_cub += pow(input, 3);
		state.sum_four += pow(input, 4);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (source.n == 0) {
			return;
		}
		target.n += source.n;
		target.sum += source.sum;
		target.sum_sqr += source.sum_sqr;
		target.sum_cub += source.sum_cub;
		target.sum_four += source.sum_four;
	}

	template <class TARGET_TYPE, class STATE>
	static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data) {
		auto n = (double)state.n;
		if (n <= 1) {
			finalize_data.ReturnNull();
			return;
		}
		if (std::is_same<KURTOSIS_FLAG, KurtosisFlagBiasCorrection>::value && n <= 3) {
			finalize_data.ReturnNull();
			return;
		}
		double temp = 1 / n;
		//! This is necessary due to linux 32 bits
		long double temp_aux = 1 / n;
		if (state.sum_sqr - state.sum * state.sum * temp == 0 ||
		    state.sum_sqr - state.sum * state.sum * temp_aux == 0) {
			finalize_data.ReturnNull();
			return;
		}
		double m4 =
		    temp * (state.sum_four - 4 * state.sum_cub * state.sum * temp +
		            6 * state.sum_sqr * state.sum * state.sum * temp * temp - 3 * pow(state.sum, 4) * pow(temp, 3));

		double m2 = temp * (state.sum_sqr - state.sum * state.sum * temp);
		if (m2 <= 0) { // m2 shouldn't be below 0 but floating points are weird
			finalize_data.ReturnNull();
			return;
		}
		if (std::is_same<KURTOSIS_FLAG, KurtosisFlagNoBiasCorrection>::value) {
			target = m4 / (m2 * m2) - 3;
		} else {
			target = (n - 1) * ((n + 1) * m4 / (m2 * m2) - 3 * (n - 1)) / ((n - 2) * (n - 3));
		}
		if (!Value::DoubleIsFinite(target)) {
			throw OutOfRangeException("Kurtosis is out of range!");
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

} // namespace

AggregateFunction KurtosisFun::GetFunction() {
	auto result =
	    AggregateFunction::UnaryAggregate<KurtosisState, double, double, KurtosisOperation<KurtosisFlagBiasCorrection>>(
	        LogicalType::DOUBLE, LogicalType::DOUBLE);
	result.SetFallible();
	return result;
}

AggregateFunction KurtosisPopFun::GetFunction() {
	auto result = AggregateFunction::UnaryAggregate<KurtosisState, double, double,
	                                                KurtosisOperation<KurtosisFlagNoBiasCorrection>>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE);
	result.SetFallible();
	return result;
}

} // namespace duckdb
