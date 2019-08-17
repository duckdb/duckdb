//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate/algebraic_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function.hpp"

namespace duckdb {

struct Avg {
	static AggregateFunction GetFunction();
};

struct CovarSamp {
	static AggregateFunction GetFunction();
};

struct CovarPop {
	static AggregateFunction GetFunction();
};

struct StdDevSamp {
	static AggregateFunction GetFunction();
};

struct StdDevPop {
	static AggregateFunction GetFunction();
};

struct VarPop {
	static AggregateFunction GetFunction();
};

struct VarSamp {
	static AggregateFunction GetFunction();
};

}
