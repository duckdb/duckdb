//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate_function/min.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function/distributive.hpp"

namespace duckdb {


void min_update(Vector** inputs, index_t input_count, Vector &result);
void min_simple_update(Vector** inputs, index_t input_count, Value &result);

class MinFunction : public AggregateSameReturnFunction {
public:
	static const char*GetName() {
		return "min";
	}

	static aggregate_update_t GetUpdateFunction() {
		return min_update;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return min_simple_update;
	}
};

}
