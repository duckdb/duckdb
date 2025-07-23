#include "core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/varint.hpp"

namespace duckdb {

namespace {

struct VarintState {
	bool is_set;
	varint_t value;
};

struct VarintOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.is_set = false;
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
		if (!state.is_set) {
			state.is_set = true;
			// We have the allocator here, let's initialize our main varint
			state.value = varint_t::FromBlob(unary_input.input.allocator, input);
		} else {
			state.value += input;
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.is_set) {
			return;
		}
		if (!target.is_set) {
			target.value = source.value;
			target.is_set = true;
			return;
		}
		target.value += source.value;
		target.is_set = true;
	}

	template <class TARGET_TYPE, class STATE>
	static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

} // namespace

AggregateFunction VarintSumFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<VarintState, string_t, varint_t, VarintOperation>(LogicalType::VARINT,
	                                                                                           LogicalType::VARINT);
}

} // namespace duckdb
