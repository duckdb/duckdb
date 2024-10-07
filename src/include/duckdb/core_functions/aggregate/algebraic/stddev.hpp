//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/core_functions/aggregate/algebraic/stddev.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include <ctgmath>

namespace duckdb {

struct StddevState {
	uint64_t count;  //  n
	double mean;     //  M1
	double dsquared; //  M2
};

// Streaming approximate standard deviation using Welford's
// method, DOI: 10.2307/1266577
struct STDDevBaseOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.count = 0;
		state.mean = 0;
		state.dsquared = 0;
	}

	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, const INPUT_TYPE &input) {
		// update running mean and d^2
		state.count++;
		const double mean_differential = (input - state.mean) / state.count;
		const double new_mean = state.mean + mean_differential;
		const double dsquared_increment = (input - new_mean) * (input - state.mean);
		const double new_dsquared = state.dsquared + dsquared_increment;

		state.mean = new_mean;
		state.dsquared = new_dsquared;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		Execute(state, input);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (target.count == 0) {
			target = source;
		} else if (source.count > 0) {
			const auto count = target.count + source.count;
			D_ASSERT(count >= target.count); // This is a check that we are not overflowing
			const double target_count = static_cast<double>(target.count);
			const double source_count = static_cast<double>(source.count);
			const double total_count = static_cast<double>(count);
			const auto mean = (source_count * source.mean + target_count * target.mean) / total_count;
			const auto delta = source.mean - target.mean;
			target.dsquared =
			    source.dsquared + target.dsquared + delta * delta * source_count * target_count / total_count;
			target.mean = mean;
			target.count = count;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct VarSampOperation : public STDDevBaseOperation {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count <= 1) {
			finalize_data.ReturnNull();
		} else {
			target = state.dsquared / (state.count - 1);
			if (!Value::DoubleIsFinite(target)) {
				throw OutOfRangeException("VARSAMP is out of range!");
			}
		}
	}
};

struct VarPopOperation : public STDDevBaseOperation {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			target = state.count > 1 ? (state.dsquared / state.count) : 0;
			if (!Value::DoubleIsFinite(target)) {
				throw OutOfRangeException("VARPOP is out of range!");
			}
		}
	}
};

struct STDDevSampOperation : public STDDevBaseOperation {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count <= 1) {
			finalize_data.ReturnNull();
		} else {
			target = sqrt(state.dsquared / (state.count - 1));
			if (!Value::DoubleIsFinite(target)) {
				throw OutOfRangeException("STDDEV_SAMP is out of range!");
			}
		}
	}
};

struct STDDevPopOperation : public STDDevBaseOperation {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			target = state.count > 1 ? sqrt(state.dsquared / state.count) : 0;
			if (!Value::DoubleIsFinite(target)) {
				throw OutOfRangeException("STDDEV_POP is out of range!");
			}
		}
	}
};

struct StandardErrorOfTheMeanOperation : public STDDevBaseOperation {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			target = sqrt(state.dsquared / state.count) / sqrt((state.count));
			if (!Value::DoubleIsFinite(target)) {
				throw OutOfRangeException("SEM is out of range!");
			}
		}
	}
};
} // namespace duckdb
