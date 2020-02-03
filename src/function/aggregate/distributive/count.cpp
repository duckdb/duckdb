#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {


struct CountOperator {
	template <class STATE_TYPE> static inline void Operation(STATE_TYPE state) {
		(*state)++;
	}
};

static void countstar_update(Vector inputs[], index_t input_count, Vector &addresses) {
	// add one to each address
	auto states = (int64_t**) addresses.GetData();
	VectorOperations::Exec(addresses, [&](index_t i, index_t k) {
		states[i][0]++;
	});
}

static void countstar_simple_update(Vector inputs[], index_t input_count, data_ptr_t state_) {
	// count star: just add the count
	auto state = (int64_t*) state_;
	*state += inputs[0].count;
}

static void count_update(Vector inputs[], index_t input_count, Vector &addresses) {
	auto &input = inputs[0];
	if (input.vector_type == VectorType::CONSTANT_VECTOR) {
		// constant input
		if (!input.nullmask[0]) {
			// constant NULL
			countstar_update(inputs, input_count, addresses);
		}
		return;
	}
	// regular input: first normalize
	input.Normalify();
	// now check nullmask
	if (!input.nullmask.any()) {
		// no null values, perform regular count(*) update
		countstar_update(inputs, input_count, addresses);
		return;
	}
	auto states = (int64_t**) addresses.GetData();
	VectorOperations::Exec(input, [&](index_t i, index_t k) {
		if (!input.nullmask[i]) {
			states[i][0]++;
		}
	});
}

static void count_combine(Vector &state, Vector &combined) {
	VectorOperations::Scatter::Add(state, combined);
}

static void count_simple_update(Vector inputs[], index_t input_count, data_ptr_t state_) {
	auto state = (int64_t*) state_;
	auto &input = inputs[0];
	if (input.vector_type == VectorType::CONSTANT_VECTOR) {
		// constant vector, check if it is a constant NULL
		// if not, add the results
		if (!input.nullmask[0]) {
			*state += input.count;
		}
	} else {
		// other vector type: normalify first
		input.Normalify();
		if (input.nullmask.any()) {
			// NULL values, count the amount of NULL entries
			VectorOperations::Exec(input, [&](index_t i, index_t k) {
				if (!input.nullmask[i]) {
					(*state)++;
				}
			});
		} else {
			// no NULL values, return all
			*state += input.count;
		}
	}
}

AggregateFunction CountFun::GetFunction() {
	return AggregateFunction({SQLType(SQLTypeId::ANY)}, SQLType::BIGINT, get_bigint_type_size,
	                         bigint_payload_initialize, count_update, count_combine, gather_finalize,
	                         count_simple_update);
}

AggregateFunction CountStarFun::GetFunction() {
	return AggregateFunction("count_star", {SQLType(SQLTypeId::ANY)}, SQLType::BIGINT, get_bigint_type_size,
	                         bigint_payload_initialize, countstar_update, count_combine, gather_finalize,
	                         countstar_simple_update);
}

void CountFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunction count_function = CountFun::GetFunction();
	AggregateFunctionSet count("count");
	count.AddFunction(count_function);
	// the count function can also be called without arguments
	count_function.arguments.clear();
	count.AddFunction(count_function);
	set.AddFunction(count);
}

void CountStarFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(CountStarFun::GetFunction());
}

} // namespace duckdb
