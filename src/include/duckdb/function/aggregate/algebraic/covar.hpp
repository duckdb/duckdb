//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/algebraic/covar.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {

struct CovarState {
	uint64_t count;
	double meanx;
	double meany;
	double co_moment;
};

struct CovarOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->count = 0;
		state->meanx = 0;
		state->meany = 0;
		state->co_moment = 0;
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, A_TYPE *x_data, B_TYPE *y_data, ValidityMask &amask,
	                      ValidityMask &bmask, idx_t xidx, idx_t yidx) {
		// update running mean and d^2
		const uint64_t n = ++(state->count);

		const auto x = x_data[xidx];
		const double dx = (x - state->meanx);
		const double meanx = state->meanx + dx / n;

		const auto y = y_data[yidx];
		const double dy = (y - state->meany);
		const double meany = state->meany + dy / n;

		const double C = state->co_moment + dx * (y - meany);

		state->meanx = meanx;
		state->meany = meany;
		state->co_moment = C;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		if (target->count == 0) {
			*target = source;
		} else if (source.count > 0) {
			const auto count = target->count + source.count;
			const auto meanx = (source.count * source.meanx + target->count * target->meanx) / count;
			const auto meany = (source.count * source.meany + target->count * target->meany) / count;

			//  Schubert and Gertz SSDBM 2018, equation 21
			const auto deltax = target->meanx - source.meanx;
			const auto deltay = target->meany - source.meany;
			target->co_moment =
			    source.co_moment + target->co_moment + deltax * deltay * source.count * target->count / count;
			target->meanx = meanx;
			target->meany = meany;
			target->count = count;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct CovarPopOperation : public CovarOperation {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (state->count == 0) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = state->co_moment / state->count;
		}
	}
};

struct CovarSampOperation : public CovarOperation {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if ((state->count) < 2) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = state->co_moment / (state->count - 1);
		}
	}
};
} // namespace duckdb
