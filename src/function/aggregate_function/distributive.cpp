//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate_function/distributive.cpp
//
//
//===----------------------------------------------------------------------===//

#include "function/aggregate_function/distributive.hpp"
#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

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
