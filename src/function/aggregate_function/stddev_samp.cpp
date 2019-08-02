#include "function/aggregate_function/stddev_samp.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include <cmath>

using namespace std;

namespace duckdb {

SQLType stddev_get_return_type(vector<SQLType> &arguments) {
	if (arguments.size() != 1)
		return SQLTypeId::INVALID;
	const auto& input_type = arguments[0];
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

void stddevsamp_update(Vector** inputs, index_t input_count, Vector &result ) {
	assert(input_count == 1 );
	// Streaming approximate standard deviation using Welford's
	// method, DOI: 10.2307/1266577

	// convert input to floating point if required
	Vector payload_double;
	if (inputs[0]->type != TypeId::DOUBLE) {
		payload_double.Initialize(TypeId::DOUBLE);
		VectorOperations::Cast(*inputs[0], payload_double);
	} else {
		payload_double.Reference(*inputs[0]);
	}

	VectorOperations::Exec(result, [&](index_t i, index_t k) {
		if (payload_double.nullmask[i]) {
			return;
		}
		// Layout of payload for STDDEV_SAMP: count(uint64_t), mean
		// (double), dsquared(double)

		auto base_ptr = ((data_ptr_t *)result.data)[i];
		auto count_ptr = (uint64_t *)base_ptr;
		auto mean_ptr = (double *)(base_ptr + sizeof(uint64_t));
		auto dsquared_ptr = (double *)(base_ptr + sizeof(uint64_t) + sizeof(double));

		// update running mean and d^2
		(*count_ptr)++;
		const double new_value = ((double *)payload_double.data)[i];
		const double mean_differential = (new_value - (*mean_ptr)) / (*count_ptr);
		const double new_mean = (*mean_ptr) + mean_differential;
		const double dsquared_increment = (new_value - new_mean) * (new_value - (*mean_ptr));
		const double new_dsquared = (*dsquared_ptr) + dsquared_increment;

		*mean_ptr = new_mean;
		*dsquared_ptr = new_dsquared;
		// see Finalize() method below for final step
	});
}

void stddevsamp_finalize(Vector& payloads, Vector &result) {
	// compute finalization of streaming stddev of sample
	VectorOperations::Exec(payloads, [&](uint64_t i, uint64_t k) {
		auto base_ptr = ((data_ptr_t *)payloads.data)[i];
		auto count_ptr = (uint64_t *)base_ptr;
		auto dsquared_ptr = (double *)(base_ptr + sizeof(uint64_t) + sizeof(double));

		if (*count_ptr == 0) {
			result.nullmask[i] = true;
			return;
		}
		double res = *count_ptr > 1 ? sqrt(*dsquared_ptr / (*count_ptr - 1)) : 0;

		((double *)result.data)[i] = res;
	});
}

} // namespace duckdb
