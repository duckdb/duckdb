#include "duckdb/function/aggregate/algebraic_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/function_set.hpp"
#include <cmath>

using namespace duckdb;
using namespace std;

struct covar_state_t {
	uint64_t count;
	double meanx;
	double meany;
	double co_moment;
};

struct CovarOperation {
	template <class STATE> static void Initialize(STATE *state) {
		state->count = 0;
		state->meanx = 0;
		state->meany = 0;
		state->co_moment = 0;
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, A_TYPE *x_data, B_TYPE *y_data, nullmask_t &anullmask, nullmask_t &bnullmask,
	                      idx_t xidx, idx_t yidx) {
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

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
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
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (state->count == 0) {
			nullmask[idx] = true;
		} else {
			target[idx] = state->co_moment / state->count;
		}
	}
};

struct CovarSampOperation : public CovarOperation {
	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if ((state->count) < 2) {
			nullmask[idx] = true;
		} else {
			target[idx] = state->co_moment / (state->count - 1);
		}
	}
};

void CovarPopFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet covar_pop("covar_pop");
	covar_pop.AddFunction(AggregateFunction::BinaryAggregate<covar_state_t, double, double, double, CovarPopOperation>(
	    SQLType::DOUBLE, SQLType::DOUBLE, SQLType::DOUBLE));
	set.AddFunction(covar_pop);
}

void CovarSampFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet covar_samp("covar_samp");
	covar_samp.AddFunction(
	    AggregateFunction::BinaryAggregate<covar_state_t, double, double, double, CovarSampOperation>(
	        SQLType::DOUBLE, SQLType::DOUBLE, SQLType::DOUBLE));
	set.AddFunction(covar_samp);
}
