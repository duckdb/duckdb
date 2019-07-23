//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate_function/distributive.cpp
//
//
//===----------------------------------------------------------------------===//

#include "function/aggregate_function/distributive.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void gather_finalize(Vector& payloads, Vector &result) {
	VectorOperations::Gather::Set(payloads, result);
}

void null_payload_initialize(data_ptr_t payload, TypeId return_type) {
	SetNullValue(payload, return_type);
}

void count_function(Vector inputs[], index_t input_count, Vector &result ) {
	assert(input_count == 1 );
	VectorOperations::Scatter::AddOne(inputs[0], result);
}

void count_simple_function(Vector inputs[], index_t input_count, Value& result) {
	assert(input_count == 1 );
	Value count = VectorOperations::Count(inputs[0]);
	result = result + count;
}

void countstar_function(Vector inputs[], index_t input_count, Vector &result ) {
	assert(input_count == 0 );
	// add one to each address, regardless of if the value is NULL
	Vector one(Value::BIGINT(1));
	VectorOperations::Scatter::Add(one, result);
}

void countstar_simple_function(Vector inputs[], index_t input_count, Value& result) {
	assert(input_count == 1 );
	Value count = Value::BIGINT(inputs[0].count);
	result = result + count;
}

void first_function(Vector inputs[], index_t input_count, Vector &result ) {
	assert(input_count == 1 );
	VectorOperations::Scatter::SetFirst(inputs[0], result);
}

void max_function(Vector inputs[], index_t input_count, Vector &result ) {
	assert(input_count == 1 );
	VectorOperations::Scatter::Max(inputs[0], result);
}

void max_simple_function(Vector inputs[], index_t input_count, Value& result) {
	assert(input_count == 1 );
	Value max = VectorOperations::Max(inputs[0]);
	if (max.is_null) {
		return;
	}
	if (result.is_null || result < max) {
		result = max;
	}
}

void min_function(Vector inputs[], index_t input_count, Vector &result ) {
	assert(input_count == 1 );
	VectorOperations::Scatter::Min(inputs[0], result);
}

void min_simple_function(Vector inputs[], index_t input_count, Value& result) {
	assert(input_count == 1 );
	Value min = VectorOperations::Min(inputs[0]);
	if (min.is_null) {
		return;
	}
	if (result.is_null || result > min) {
		result = min;
	}
}

SQLType stddev_get_return_type(vector<SQLType> &arguments) {
	assert(arguments.size() == 1);
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
		throw BinderException("Unsupported SQLType %s for STDDEV_SAMP aggregate", SQLTypeToString(input_type).c_str());
	}
}
void stddevsamp_function(Vector inputs[], index_t input_count, Vector &result ) {
	assert(input_count == 1 );
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

SQLType sum_get_return_type( vector<SQLType> &arguments ) {
	assert(arguments.size() == 1);
	const auto& input_type = arguments[0];
	switch (input_type.id) {
	case SQLTypeId::SQLNULL:
	case SQLTypeId::TINYINT:
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
		return SQLType(SQLTypeId::BIGINT);
	case SQLTypeId::FLOAT:
		return SQLType(SQLTypeId::FLOAT);
	case SQLTypeId::DOUBLE:
	case SQLTypeId::DECIMAL:
		return SQLType(SQLTypeId::DECIMAL);
	default:
		throw BinderException("Unsupported SQLType %s for SUM aggregate", SQLTypeToString(input_type).c_str());
	}
}

void sum_function(Vector inputs[], index_t input_count, Vector &result ) {
	assert(input_count == 1 );
	VectorOperations::Scatter::Add(inputs[0], result);
}

void sum_simple_function(Vector inputs[], index_t input_count, Value& result) {
	assert(input_count == 1 );
	Value sum = VectorOperations::Sum(inputs[0]);
	if (sum.is_null) {
		return;
	}
	if (result.is_null) {
		result = sum;
	} else {
		result = result + sum;
	}
}

} // namespace duckdb
