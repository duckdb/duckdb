//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate_function/count_star.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function/distributive.hpp"

namespace duckdb {

void countstar_update(Vector **inputs, index_t input_count, Vector &result);
void countstar_simple_update(Vector **inputs, index_t input_count, Value &result);

class CountStarFunction : public AggregateBigintReturnFunction {
public:
	static const char *GetName() {
		return "count_star";
	}

	static aggregate_update_t GetUpdateFunction() {
		return countstar_update;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return countstar_simple_update;
	}
};

} // namespace duckdb
