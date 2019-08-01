//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate_function/sum.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function/distributive.hpp"

namespace duckdb {

void sum_update(Vector inputs[], index_t input_count, Vector &result);
void sum_simple_update(Vector inputs[], index_t input_count, Value &result);
SQLType sum_get_return_type(vector<SQLType> &arguments);

class SumFunction : public AggregateInPlaceFunction {
public:
	static const char*GetName() {
		return "sum";
	}

	static aggregate_size_t GetPayloadSizeFunction() {
		return get_return_type_size;
	}

	static aggregate_initialize_t GetInitalizeFunction() {
		return null_payload_initialize;
	}

	static aggregate_update_t GetUpdateFunction() {
		return sum_update;
	}

	static aggregate_simple_initialize_t GetSimpleInitializeFunction() {
		return null_simple_initialize;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return sum_simple_update;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return sum_get_return_type;
	}

	static matches_argument_function_t GetCastArgumentsFunction() {
		return cast_arguments;
	}
};

}
