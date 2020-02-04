#include "duckdb/function/aggregate/algebraic_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

struct avg_state_t {
	uint64_t count;
	double sum;
};

static index_t avg_payload_size(TypeId return_type) {
	return sizeof(avg_state_t);
}

static void avg_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, avg_payload_size(return_type));
}

static void avg_update(Vector inputs[], index_t input_count, Vector &state) {
	assert(input_count == 1);
	inputs[0].Normalify();

	auto states = (avg_state_t **)state.GetData();
	auto input_data = (double *)inputs[0].GetData();
	VectorOperations::Exec(state, [&](index_t i, index_t k) {
		if (inputs[0].nullmask[i]) {
			return;
		}

		auto state_ptr = states[i];

		// update count and running sum
		state_ptr->count++;
		const auto new_value = input_data[i];
		state_ptr->sum += new_value;
		// see Finalize() method below for final step
	});
}

static void avg_combine(Vector &state, Vector &combined) {
	// combine streaming avg states
	auto combined_data = (avg_state_t **)combined.GetData();
	auto state_data = (avg_state_t *)state.GetData();

	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto combined_ptr = combined_data[i];
		auto state_ptr = state_data + i;

		if (0 == combined_ptr->count) {
			*combined_ptr = *state_ptr;
		} else if (state_ptr->count) {
			combined_ptr->count += state_ptr->count;
			combined_ptr->sum += state_ptr->sum;
		}
	});
}

static void avg_finalize(Vector &state, Vector &result) {
	// compute finalization of streaming avg
	auto states = (avg_state_t **)state.GetData();
	auto result_data = (double *)result.GetData();
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto state_ptr = states[i];

		if (state_ptr->count == 0) {
			result.nullmask[i] = true;
		} else {
			result_data[i] = state_ptr->sum / state_ptr->count;
		}
	});
}

void AvgFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("avg", {SQLType::DOUBLE}, SQLType::DOUBLE, avg_payload_size, avg_initialize,
	                                  avg_update, avg_combine, avg_finalize));
}
