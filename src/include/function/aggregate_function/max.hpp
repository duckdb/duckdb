//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate_function/max.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function/distributive.hpp"

namespace duckdb {

void max_update(Vector inputs[], index_t input_count, Vector &result);
void max_simple_update(Vector inputs[], index_t input_count, Value &result);

class MaxFunction : public AggregateSameReturnFunction {
public:
	static const char *GetName() {
		return "max";
	}

	static aggregate_update_t GetUpdateFunction() {
		return max_update;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return max_simple_update;
	}
};

} // namespace duckdb
