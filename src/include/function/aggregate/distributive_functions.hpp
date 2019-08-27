//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate/distributive_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function.hpp"
#include "function/function_set.hpp"

namespace duckdb {

void gather_finalize(Vector &payloads, Vector &result);

index_t get_bigint_type_size(TypeId return_type);
void bigint_payload_initialize(data_ptr_t payload, TypeId return_type);
Value bigint_simple_initialize();

index_t get_return_type_size(TypeId return_type);

void null_state_initialize(data_ptr_t state, TypeId return_type);
Value null_simple_initialize();

struct CountStar {
	static AggregateFunction GetFunction();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct Count {
	static AggregateFunction GetFunction();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct First {
	static AggregateFunction GetFunction(SQLType type);

	static void RegisterFunction(BuiltinFunctions &set);
};

struct Max {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Min {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Sum {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StringAgg {
	static void RegisterFunction(BuiltinFunctions &set);
};

}
