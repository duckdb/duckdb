#include "function/aggregate_function/algebraic_functions.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include <cmath>

using namespace std;

namespace duckdb {

struct stddev_state_t {
    uint64_t    count;
    double      mean;
    double      dsquared;
};

index_t stddev_state_size(TypeId return_type) {
	return sizeof(stddev_state_t);
}

void stddevsamp_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, stddev_state_size(return_type));
}

SQLType stddev_get_return_type(vector<SQLType> &arguments) {
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

void stddevsamp_update(Vector inputs[], index_t input_count, Vector &state) {
	assert(input_count == 1);
	// Streaming approximate standard deviation using Welford's
	// method, DOI: 10.2307/1266577

	// convert input to floating point if required
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

		auto state_ptr = (stddev_state_t*) ((data_ptr_t *)state.data)[i];

		// update running mean and d^2
		state_ptr->count++;
		const double new_value = ((double *)payload_double.data)[i];
		const double mean_differential = (new_value - state_ptr->mean) / state_ptr->count;
		const double new_mean = state_ptr->mean + mean_differential;
		const double dsquared_increment = (new_value - new_mean) * (new_value - state_ptr->mean);
		const double new_dsquared = state_ptr->dsquared + dsquared_increment;

		state_ptr->mean = new_mean;
		state_ptr->dsquared = new_dsquared;
		// see Finalize() method below for final step
	});
}

void varsamp_finalize(Vector &state, Vector &result) {
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

void varpop_finalize(Vector &state, Vector &result) {
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

void stddevsamp_finalize(Vector &state, Vector &result) {
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

void stddevpop_finalize(Vector &state, Vector &result) {
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

} // namespace duckdb
