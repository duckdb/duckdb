#pragma once

#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

struct ImprintStatsFun {
	static ScalarFunction GetFunction();
};

} // namespace duckdb
