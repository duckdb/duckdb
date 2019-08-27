#include "function/aggregate/algebraic_functions.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include <cmath>

using namespace duckdb;
using namespace std;

struct stddev_state_t {
    uint64_t    count;          //  n
    double      mean;           //  M1
    double      dsquared;       //  M2
};

static index_t stddev_state_size(TypeId return_type) {
	return sizeof(stddev_state_t);
}

static void stddev_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, stddev_state_size(return_type));
}

static void stddev_update(Vector inputs[], index_t input_count, Vector &state) {
	assert(input_count == 1);
	// Streaming approximate standard deviation using Welford's
	// method, DOI: 10.2307/1266577

	VectorOperations::Exec(state, [&](index_t i, index_t k) {
		if (inputs[0].nullmask[i]) {
			return;
		}

		auto state_ptr = (stddev_state_t*) ((data_ptr_t *)state.data)[i];

		// update running mean and d^2
		state_ptr->count++;
		const double new_value = ((double *)inputs[0].data)[i];
		const double mean_differential = (new_value - state_ptr->mean) / state_ptr->count;
		const double new_mean = state_ptr->mean + mean_differential;
		const double dsquared_increment = (new_value - new_mean) * (new_value - state_ptr->mean);
		const double new_dsquared = state_ptr->dsquared + dsquared_increment;

		state_ptr->mean = new_mean;
		state_ptr->dsquared = new_dsquared;
		// see Finalize() method below for final step
	});
}


static void stddev_combine(Vector &state_a, Vector &state_b, Vector &combined) {
    // combine streaming avg states
    VectorOperations::Exec(state_a, [&](uint64_t i, uint64_t k) {
        auto c_ptr = (stddev_state_t*) ((data_ptr_t *)combined.data)[i];
        auto a_ptr = (const stddev_state_t*) ((data_ptr_t *)state_a.data)[i];
        auto b_ptr = (const stddev_state_t*) ((data_ptr_t *)state_b.data)[i];

        if (0 == a_ptr->count) {
            *c_ptr = *b_ptr;
        } else if (0 == b_ptr->count) {
            *c_ptr = *a_ptr;
        } else {
            c_ptr->count = a_ptr->count + b_ptr->count;
            c_ptr->mean = ( a_ptr->count * a_ptr->mean + b_ptr->count * b_ptr->mean ) / c_ptr->count;
            const auto delta = b_ptr->mean - a_ptr->mean;
            c_ptr->dsquared = a_ptr->dsquared + b_ptr->dsquared + delta * delta * a_ptr->count * b_ptr->count / c_ptr->count;
        }
    });
}

static void varsamp_finalize(Vector &state, Vector &result) {
	// compute finalization of streaming stddev of sample
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto state_ptr = (stddev_state_t*) ((data_ptr_t *)state.data)[i];

		if (state_ptr->count == 0) {
			result.nullmask[i] = true;
			return;
		}
		double res = state_ptr->count > 1 ? (state_ptr->dsquared / (state_ptr->count - 1)) : 0;

		((double *)result.data)[i] = res;
	});
}

static void varpop_finalize(Vector &state, Vector &result) {
	// compute finalization of streaming stddev of sample
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto state_ptr = (stddev_state_t*) ((data_ptr_t *)state.data)[i];

		if (state_ptr->count == 0) {
			result.nullmask[i] = true;
			return;
		}
		double res = state_ptr->count > 1 ? (state_ptr->dsquared / state_ptr->count) : 0;

		((double *)result.data)[i] = res;
	});
}

static void stddevsamp_finalize(Vector &state, Vector &result) {
	// compute finalization of streaming stddev of sample
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto state_ptr = (stddev_state_t*) ((data_ptr_t *)state.data)[i];

		if (state_ptr->count == 0) {
			result.nullmask[i] = true;
			return;
		}
		double res = state_ptr->count > 1 ? sqrt(state_ptr->dsquared / (state_ptr->count - 1)) : 0;

		((double *)result.data)[i] = res;
	});
}

static void stddevpop_finalize(Vector &state, Vector &result) {
	// compute finalization of streaming stddev of sample
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto state_ptr = (stddev_state_t*) ((data_ptr_t *)state.data)[i];

		if (state_ptr->count == 0) {
			result.nullmask[i] = true;
			return;
		}
		double res = state_ptr->count > 1 ? sqrt(state_ptr->dsquared / state_ptr->count) : 0;

		((double *)result.data)[i] = res;
	});
}

void StdDevSamp::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("stddev_samp", {SQLType::DOUBLE}, SQLType::DOUBLE, stddev_state_size, stddev_initialize, stddev_update, stddev_combine, stddevsamp_finalize));
}

void StdDevPop::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("stddev_pop", {SQLType::DOUBLE}, SQLType::DOUBLE, stddev_state_size, stddev_initialize, stddev_update, stddev_combine, stddevpop_finalize));
}

void VarPop::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("var_samp", {SQLType::DOUBLE}, SQLType::DOUBLE, stddev_state_size, stddev_initialize, stddev_update, stddev_combine, varsamp_finalize));
}

void VarSamp::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("var_pop", {SQLType::DOUBLE}, SQLType::DOUBLE, stddev_state_size, stddev_initialize, stddev_update, stddev_combine, varpop_finalize));
}
