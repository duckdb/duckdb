#include "duckdb/function/aggregate/algebraic_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <cmath>

using namespace duckdb;
using namespace std;

struct covar_state_t {
	uint64_t count;
	double meanx;
	double meany;
	double co_moment;
};

static index_t covar_state_size(TypeId return_type) {
	return sizeof(covar_state_t);
}

static void covar_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, covar_state_size(return_type));
}

static void covar_update(Vector inputs[], index_t input_count, Vector &state) {
	assert(input_count == 2);
	// Streaming approximate covariance

	VectorOperations::Exec(state, [&](index_t i, index_t k) {
		if (inputs[0].nullmask[i] || inputs[1].nullmask[i]) {
			return;
		}

		auto state_ptr = (covar_state_t *)((data_ptr_t *)state.data)[i];

		// update running mean and d^2
		const uint64_t n = ++(state_ptr->count);

		const double x = ((double *)inputs[0].data)[i];
		const double dx = (x - state_ptr->meanx);
		const double meanx = state_ptr->meanx + dx / n;

		const double y = ((double *)inputs[1].data)[i];
		const double dy = (y - state_ptr->meany);
		const double meany = state_ptr->meany + dy / n;

		const double C = state_ptr->co_moment + dx * (y - meany);

		state_ptr->meanx = meanx;
		state_ptr->meany = meany;
		state_ptr->co_moment = C;
		// see Finalize() methods below for final step
	});
}

static void covar_combine(Vector &state, Vector &combined) {
	// combine streaming covar states
	auto combined_data = (covar_state_t **)combined.data;
	auto state_data = (covar_state_t *)state.data;

	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto combined_ptr = combined_data[i];
		auto state_ptr = state_data + i;

		if (0 == combined_ptr->count) {
			*combined_ptr = *state_ptr;
		} else if (state_ptr->count) {
			const auto count = combined_ptr->count + state_ptr->count;
			const auto meanx =
			    (state_ptr->count * state_ptr->meanx + combined_ptr->count * combined_ptr->meanx) / count;
			const auto meany =
			    (state_ptr->count * state_ptr->meany + combined_ptr->count * combined_ptr->meany) / count;

			//  Schubert and Gertz SSDBM 2018, equation 21
			const auto deltax = combined_ptr->meanx - state_ptr->meanx;
			const auto deltay = combined_ptr->meany - state_ptr->meany;
			combined_ptr->co_moment = state_ptr->co_moment + combined_ptr->co_moment +
			                          deltax * deltay * state_ptr->count * combined_ptr->count / count;
			combined_ptr->meanx = meanx;
			combined_ptr->meany = meany;
			combined_ptr->count = count;
		}
	});
}

static void covarpop_finalize(Vector &state, Vector &result) {
	// compute finalization of streaming population covariance
	VectorOperations::Exec(result, [&](uint64_t i, uint64_t k) {
		auto state_ptr = (covar_state_t *)((data_ptr_t *)state.data)[i];

		if (state_ptr->count == 0) {
			result.nullmask[i] = true;
			return;
		}
		double res = state_ptr->co_moment / state_ptr->count;

		((double *)result.data)[i] = res;
	});
}

static void covarsamp_finalize(Vector &state, Vector &result) {
	// compute finalization of streaming sample covariance
	VectorOperations::Exec(result, [&](uint64_t i, uint64_t k) {
		auto state_ptr = (covar_state_t *)((data_ptr_t *)state.data)[i];

		if (state_ptr->count < 2) {
			result.nullmask[i] = true;
			return;
		}
		double res = state_ptr->co_moment / (state_ptr->count - 1);

		((double *)result.data)[i] = res;
	});
}

void CovarSampFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("covar_samp", {SQLType::DOUBLE, SQLType::DOUBLE}, SQLType::DOUBLE,
	                                  covar_state_size, covar_initialize, covar_update, covar_combine,
	                                  covarsamp_finalize));
}

void CovarPopFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("covar_pop", {SQLType::DOUBLE, SQLType::DOUBLE}, SQLType::DOUBLE,
	                                  covar_state_size, covar_initialize, covar_update, covar_combine,
	                                  covarpop_finalize));
}
