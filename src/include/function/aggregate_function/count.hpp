//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate_function/count.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function/distributive.hpp"

namespace duckdb {

void count_update(Vector** inputs, index_t input_count, Vector &result);
void count_simple_update(Vector** inputs, index_t input_count, Value &result);

class CountFunction : public AggregateBigintReturnFunction {
public:
	static const char*GetName() {
		return "count";
	}

	static aggregate_update_t GetUpdateFunction() {
		return count_update;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return count_simple_update;
	}
};

}
