//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate_function/first.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function/distributive.hpp"

namespace duckdb {

void first_update(Vector inputs[], index_t input_count, Vector &result);

class FirstFunction : public AggregateSameReturnFunction {
public:
	static const char *GetName() {
		return "first";
	}

	static aggregate_update_t GetUpdateFunction() {
		return first_update;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return nullptr;
	}
};

} // namespace duckdb
