//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate_function/stringagg.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function/distributive.hpp"

namespace duckdb {

void string_agg_update(Vector inputs[], index_t input_count, Vector &result);
SQLType string_agg_get_return_type(vector<SQLType> &arguments);

class StringAggFunction : public AggregateInPlaceFunction {
public:
	static const char *GetName() {
		return "string_agg";
	}

	static aggregate_size_t GetStateSizeFunction() {
		return get_return_type_size;
	}

	static aggregate_initialize_t GetInitalizeFunction() {
		return null_state_initialize;
	}

	static aggregate_update_t GetUpdateFunction() {
		return string_agg_update;
	}

	static aggregate_simple_initialize_t GetSimpleInitializeFunction() {
		return null_simple_initialize;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return nullptr;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return string_agg_get_return_type;
	}

	static matches_argument_function_t GetCastArgumentsFunction() {
		return cast_arguments;
	}
};

} // namespace duckdb
