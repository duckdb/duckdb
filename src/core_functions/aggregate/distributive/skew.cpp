#include "duckdb/core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

struct SkewState {
	size_t n;
	double sum;
	double sum_sqr;
	double sum_cub;
};

struct SkewnessOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.n = 0;
		state.sum = state.sum_sqr = state.sum_cub = 0;
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
	}

	template <class TARGET_TYPE, class STATE>
	static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data) {
		if (state.n <= 2) {
			finalize_data.ReturnNull();
			return;
		}
		double n = state.n;
		double temp = 1 / n;
		auto p = std::pow(temp * (state.sum_sqr - state.sum * state.sum * temp), 3);
		if (p < 0) {
			p = 0; // Shouldn't be below 0 but floating points are weird
		}
		double div = std::sqrt(p);
		if (div == 0) {
			finalize_data.ReturnNull();
			return;
		}
		double temp1 = std::sqrt(n * (n - 1)) / (n - 2);
		target = temp1 * temp *
		         (state.sum_cub - 3 * state.sum_sqr * state.sum * temp + 2 * pow(state.sum, 3) * temp * temp) / div;
		if (!Value::DoubleIsFinite(target)) {
			throw OutOfRangeException("SKEW is out of range!");
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction SkewnessFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<SkewState, double, double, SkewnessOperation>(LogicalType::DOUBLE,
	                                                                                       LogicalType::DOUBLE);
}

} // namespace duckdb
