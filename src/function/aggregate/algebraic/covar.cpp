#include "function/aggregate/algebraic_functions.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include <cmath>

using namespace duckdb;
using namespace std;

struct covar_state_t {
    uint64_t    count;
    double      meanx;
    double      meany;
    double      co_moment;
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

		auto state_ptr = (covar_state_t*) ((data_ptr_t *)state.data)[i];

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

static void covar_combine(Vector &state_a, Vector &state_b, Vector &combined) {
    // combine streaming avg states
    VectorOperations::Exec(state_a, [&](uint64_t i, uint64_t k) {
        auto c_ptr = (covar_state_t*) ((data_ptr_t *)combined.data)[i];
        auto a_ptr = (const covar_state_t*) ((data_ptr_t *)state_a.data)[i];
        auto b_ptr = (const covar_state_t*) ((data_ptr_t *)state_b.data)[i];

        if (0 == a_ptr->count) {
            *c_ptr = *b_ptr;
        } else if (0 == b_ptr->count) {
            *c_ptr = *a_ptr;
        } else {
            c_ptr->count = a_ptr->count + b_ptr->count;
            c_ptr->meanx = ( a_ptr->count * a_ptr->meanx + b_ptr->count * b_ptr->meanx ) / c_ptr->count;
            c_ptr->meany = ( a_ptr->count * a_ptr->meany + b_ptr->count * b_ptr->meany ) / c_ptr->count;

            //  Schubert and Gertz SSDBM 2018, equation 21
            const auto deltax = b_ptr->meanx - a_ptr->meanx;
            const auto deltay = b_ptr->meany - a_ptr->meany;
            c_ptr->co_moment = a_ptr->co_moment + b_ptr->co_moment + deltax * deltay * a_ptr->count * b_ptr->count / c_ptr->count;
        }
    });
}

static void covarpop_finalize(Vector &state, Vector &result) {
	// compute finalization of streaming population covariance
	VectorOperations::Exec(result, [&](uint64_t i, uint64_t k) {
		auto state_ptr = (covar_state_t*) ((data_ptr_t *)state.data)[i];

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
		auto state_ptr = (covar_state_t*) ((data_ptr_t *)state.data)[i];

		if (state_ptr->count < 2) {
			result.nullmask[i] = true;
			return;
		}
		double res = state_ptr->co_moment / (state_ptr->count - 1);

		((double *)result.data)[i] = res;
	});
}

void CovarSamp::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("covar_samp", {SQLType::DOUBLE, SQLType::DOUBLE}, SQLType::DOUBLE, covar_state_size, covar_initialize, covar_update, covar_combine, covarsamp_finalize));
}

void CovarPop::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("covar_pop", {SQLType::DOUBLE, SQLType::DOUBLE}, SQLType::DOUBLE, covar_state_size, covar_initialize, covar_update, covar_combine, covarpop_finalize));
}
