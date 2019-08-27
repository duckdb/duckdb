#include "function/aggregate/algebraic_functions.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

struct avg_state_t {
    uint64_t    count;
    double      sum;
};

static index_t avg_payload_size(TypeId return_type) {
	return sizeof(avg_state_t);
}

static void avg_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, avg_payload_size(return_type));
}

static void avg_update(Vector inputs[], index_t input_count, Vector &state) {
	assert(input_count == 1);

	VectorOperations::Exec(state, [&](index_t i, index_t k) {
		if (inputs[0].nullmask[i]) {
			return;
		}

		auto state_ptr = (avg_state_t*) ((data_ptr_t *)state.data)[i];

		// update count and running sum
		state_ptr->count++;
		const auto new_value = ((double *)inputs[0].data)[i];
		state_ptr->sum += new_value;
		// see Finalize() method below for final step
	});
}

static void avg_combine(Vector &state1, Vector &state2, Vector &combined) {
	// combine streaming avg states
	VectorOperations::Exec(state1, [&](uint64_t i, uint64_t k) {
		auto combined_ptr = (avg_state_t*) ((data_ptr_t *)combined.data)[i];
		auto state1_ptr = (avg_state_t*) ((data_ptr_t *)state1.data)[i];
		auto state2_ptr = (avg_state_t*) ((data_ptr_t *)state2.data)[i];
		combined_ptr->count = state1_ptr->count + state2_ptr->count;
		combined_ptr->sum = state1_ptr->sum + state2_ptr->sum;
	});
}

static void avg_finalize(Vector &state, Vector &result) {
	// compute finalization of streaming avg
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto state_ptr = (avg_state_t*) ((data_ptr_t *)state.data)[i];

		if (state_ptr->count == 0) {
			result.nullmask[i] = true;
		} else {
			((double *)result.data)[i] = state_ptr->sum / state_ptr->count;
		}
	});
}

void Avg::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("avg", {SQLType::DOUBLE}, SQLType::DOUBLE, avg_payload_size, avg_initialize, avg_update, avg_combine, avg_finalize));
}
