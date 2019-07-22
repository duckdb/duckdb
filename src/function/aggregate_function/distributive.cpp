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

SQLType sum_get_return_type( vector<SQLType> &arguments ) {
	assert(arguments.size() > 0);
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
