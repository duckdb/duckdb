#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;
using namespace duckdb;

static void countstar_update(Vector inputs[], index_t input_count, Vector &result) {
	// add one to each address, regardless of if the value is NULL
	Vector one(Value::BIGINT(1));
	VectorOperations::Scatter::Add(one, result);
}

static void countstar_simple_update(Vector inputs[], index_t input_count, Value &result) {
	assert(input_count == 1);
	Value count = Value::BIGINT(inputs[0].count);
	result = result + count;
}

static void count_update(Vector inputs[], index_t input_count, Vector &result) {
	assert(input_count == 1);
	VectorOperations::Scatter::AddOne(inputs[0], result);
}

static void count_combine(Vector &state, Vector &combined) {
	VectorOperations::Scatter::Add(state, combined);
}

static void count_simple_update(Vector inputs[], index_t input_count, Value &result) {
	assert(input_count == 1);
	Value count = VectorOperations::Count(inputs[0]);
	result = result + count;
}

namespace duckdb {

AggregateFunction CountFun::GetFunction() {
	return AggregateFunction({SQLType(SQLTypeId::ANY)}, SQLType::BIGINT, get_bigint_type_size,
	                         bigint_payload_initialize, count_update, count_combine, gather_finalize,
	                         bigint_simple_initialize, count_simple_update);
}

AggregateFunction CountStarFun::GetFunction() {
	return AggregateFunction("count_star", {SQLType(SQLTypeId::ANY)}, SQLType::BIGINT, get_bigint_type_size,
	                         bigint_payload_initialize, countstar_update, count_combine, gather_finalize,
	                         bigint_simple_initialize, countstar_simple_update);
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
