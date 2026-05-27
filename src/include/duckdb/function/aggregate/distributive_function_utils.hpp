//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/distributive_function_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct ClusteredStateCopy {
	template <class STATE>
	struct ClusteredLocalState {
		typedef STATE Type;
	};

	template <class STATE>
	static void InitializeClusteredLocal(STATE &local, const STATE &state) {
		local = state;
	}

	template <class STATE>
	static void FlushClusteredLocal(STATE &state, const STATE &local, bool) {
		state = local;
	}
};

template <auto INIT_VALUE>
struct ConstantInit {
	template <class T>
	static constexpr T Value() {
		return T(INIT_VALUE);
	}
};

template <class REDUCE_OP, class INIT_OP>
struct EmptyValAggregate : public ClusteredStateCopy {
	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input) {
		local.empty = false;
		using value_type = decltype(local.val);
		local.val = REDUCE_OP::template Operation<value_type>(local.val, value_type(input));
	}

	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input, idx_t count) {
		if (count != 0) {
			UpdateClusteredLocal(local, input);
		}
	}

	template <class STATE>
	static void Initialize(STATE &state) {
		state.val = INIT_OP::template Value<decltype(state.val)>();
		state.empty = true;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		using value_type = decltype(target.val);
		target.val = REDUCE_OP::template Operation<value_type>(target.val, source.val);
		target.empty = target.empty && source.empty;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.empty) {
			finalize_data.ReturnNull();
			return;
		}
		target = state.val;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		UpdateClusteredLocal(state, input);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct CountFunctionBase {
	static AggregateFunction GetFunction();
};

struct FirstFunctionGetter {
	static AggregateFunction GetFunction(const LogicalType &type);
};

struct LastFunctionGetter {
	static AggregateFunction GetFunction(const LogicalType &type);
};

struct MinFunction {
	static AggregateFunction GetFunction();
};

struct MaxFunction {
	static AggregateFunction GetFunction();
};

} // namespace duckdb
