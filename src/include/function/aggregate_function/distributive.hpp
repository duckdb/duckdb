//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate_function/distributive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {

//! The type used for aggregate functions
typedef void (*aggregate_function_t)(Vector inputs[], index_t input_count, Vector &result);
typedef void (*aggregate__simple_function_t)(Vector inputs[], index_t input_count, Value &result);

void sum_function( Vector inputs[], index_t input_count, Vector &result );
void sum_simple_function( Vector inputs[], index_t input_count, Value &result );
SQLType sum_get_return_type(vector<SQLType> &arguments);

class SumFunction {
public:
	static const char*GetName() {
		return "sum";
	}

	static aggregate_function_t GetFunction() {
		return sum_function;
	}

	static aggregate__simple_function_t GetSimpleFunction() {
		return sum_simple_function;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return sum_get_return_type;
	}

};

} // namespace duckdb
