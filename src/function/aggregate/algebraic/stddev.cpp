#include "duckdb/function/aggregate/algebraic_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <cmath>

using namespace duckdb;
using namespace std;

struct stddev_state_t {
	uint64_t count;  //  n
	double mean;     //  M1
	double dsquared; //  M2
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

	auto states = (stddev_state_t **)state.GetData();
	auto input_data = (double *)inputs[0].GetData();
	VectorOperations::Exec(state, [&](index_t i, index_t k) {
		if (inputs[0].nullmask[i]) {
			return;
		}

		auto state_ptr = states[i];

		// update running mean and d^2
		state_ptr->count++;
		const double new_value = input_data[i];
		const double mean_differential = (new_value - state_ptr->mean) / state_ptr->count;
		const double new_mean = state_ptr->mean + mean_differential;
		const double dsquared_increment = (new_value - new_mean) * (new_value - state_ptr->mean);
		const double new_dsquared = state_ptr->dsquared + dsquared_increment;

		state_ptr->mean = new_mean;
		state_ptr->dsquared = new_dsquared;
		// see Finalize() method below for final step
	});
}

static void stddev_combine(Vector &state, Vector &combined) {
	// combine streaming stddev states
	auto combined_data = (stddev_state_t **)combined.GetData();
	auto state_data = (stddev_state_t *)state.GetData();

	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto combined_ptr = combined_data[i];
		auto state_ptr = state_data + i;

		if (0 == combined_ptr->count) {
			*combined_ptr = *state_ptr;
		} else if (state_ptr->count) {
			const auto count = combined_ptr->count + state_ptr->count;
			const auto mean = (state_ptr->count * state_ptr->mean + combined_ptr->count * combined_ptr->mean) / count;
			const auto delta = state_ptr->mean - combined_ptr->mean;
			combined_ptr->dsquared = state_ptr->dsquared + combined_ptr->dsquared +
			                         delta * delta * state_ptr->count * combined_ptr->count / count;
			combined_ptr->mean = mean;
			combined_ptr->count = count;
		}
	});
}

static void varsamp_finalize(Vector &state, Vector &result) {
	// compute finalization of streaming stddev of sample
	auto states = (stddev_state_t **)state.GetData();
	auto result_data = (double *)result.GetData();
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto state_ptr = states[i];

		if (state_ptr->count == 0) {
			result.nullmask[i] = true;
			return;
		}
		double res = state_ptr->count > 1 ? (state_ptr->dsquared / (state_ptr->count - 1)) : 0;

		result_data[i] = res;
	});
}

static void varpop_finalize(Vector &state, Vector &result) {
	// compute finalization of streaming stddev of sample
	auto states = (stddev_state_t **)state.GetData();
	auto result_data = (double *)result.GetData();
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto state_ptr = states[i];

		if (state_ptr->count == 0) {
			result.nullmask[i] = true;
			return;
		}
		double res = state_ptr->count > 1 ? (state_ptr->dsquared / state_ptr->count) : 0;

		result_data[i] = res;
	});
}

static void stddevsamp_finalize(Vector &state, Vector &result) {
	// compute finalization of streaming stddev of sample
	auto states = (stddev_state_t **)state.GetData();
	auto result_data = (double *)result.GetData();
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto state_ptr = states[i];

		if (state_ptr->count == 0) {
			result.nullmask[i] = true;
			return;
		}
		double res = state_ptr->count > 1 ? sqrt(state_ptr->dsquared / (state_ptr->count - 1)) : 0;

		result_data[i] = res;
	});
}

static void stddevpop_finalize(Vector &state, Vector &result) {
	// compute finalization of streaming stddev of sample
	auto states = (stddev_state_t **)state.GetData();
	auto result_data = (double *)result.GetData();
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto state_ptr = states[i];

		if (state_ptr->count == 0) {
			result.nullmask[i] = true;
			return;
		}
		double res = state_ptr->count > 1 ? sqrt(state_ptr->dsquared / state_ptr->count) : 0;

		result_data[i] = res;
	});
}

void StdDevSampFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("stddev_samp", {SQLType::DOUBLE}, SQLType::DOUBLE, stddev_state_size,
	                                  stddev_initialize, stddev_update, stddev_combine, stddevsamp_finalize));
}

void StdDevPopFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("stddev_pop", {SQLType::DOUBLE}, SQLType::DOUBLE, stddev_state_size,
	                                  stddev_initialize, stddev_update, stddev_combine, stddevpop_finalize));
}

void VarPopFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("var_samp", {SQLType::DOUBLE}, SQLType::DOUBLE, stddev_state_size,
	                                  stddev_initialize, stddev_update, stddev_combine, varsamp_finalize));
}

void VarSampFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("var_pop", {SQLType::DOUBLE}, SQLType::DOUBLE, stddev_state_size,
	                                  stddev_initialize, stddev_update, stddev_combine, varpop_finalize));
}
