#include "function/aggregate_function/algebraic_functions.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

struct avg_state_t {
    uint64_t    count;
    double      sum;
};

index_t avg_payload_size(TypeId return_type) {
	return sizeof(avg_state_t);
}

void avg_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, avg_payload_size(return_type));
}

SQLType avg_get_return_type(vector<SQLType> &arguments) {
	if (arguments.size() != 1)
		return SQLTypeId::INVALID;
	const auto &input_type = arguments[0];
	switch (input_type.id) {
	case SQLTypeId::SQLNULL:
	case SQLTypeId::TINYINT:
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
	case SQLTypeId::FLOAT:
	case SQLTypeId::DOUBLE:
	case SQLTypeId::DECIMAL:
		return SQLType(SQLTypeId::DECIMAL);
	default:
		return SQLTypeId::INVALID;
	}
}

void avg_update(Vector inputs[], index_t input_count, Vector &state) {
	assert(input_count == 1);
	Vector payload_double;
	if (inputs[0].type != TypeId::DOUBLE) {
		payload_double.Initialize(TypeId::DOUBLE);
		VectorOperations::Cast(inputs[0], payload_double);
	} else {
		payload_double.Reference(inputs[0]);
	}

	VectorOperations::Exec(state, [&](index_t i, index_t k) {
		if (payload_double.nullmask[i]) {
			return;
		}

		auto state_ptr = (avg_state_t*) ((data_ptr_t *)state.data)[i];

		// update count and running sum
		state_ptr->count++;
		const auto new_value = ((double *)payload_double.data)[i];
		state_ptr->sum += new_value;
		// see Finalize() method below for final step
	});
}

void avg_finalize(Vector &state, Vector &result) {
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

} // namespace duckdb
