//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate/distributive_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function.hpp"

namespace duckdb {

void gather_finalize(Vector &payloads, Vector &result);

index_t get_bigint_type_size(TypeId return_type);
void bigint_payload_initialize(data_ptr_t payload, TypeId return_type);
Value bigint_simple_initialize();
SQLType get_bigint_return_type(vector<SQLType> &arguments);

index_t get_return_type_size(TypeId return_type);

void null_state_initialize(data_ptr_t state, TypeId return_type);
Value null_simple_initialize();

SQLType get_same_return_type(vector<SQLType> &arguments);

struct CountStar {
	static AggregateFunction GetFunction();
};

struct Count {
	static AggregateFunction GetFunction();
};

struct First {
	static AggregateFunction GetFunction();
};

struct Max {
	static AggregateFunction GetFunction();
};

struct Min {
	static AggregateFunction GetFunction();
};

struct Sum {
	static AggregateFunction GetFunction();
};

struct StringAgg {
	static AggregateFunction GetFunction();
};

}
