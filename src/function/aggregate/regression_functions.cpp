#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/regression_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterRegressiveAggregates() {
	Register<RegrAvgxFun>();
	Register<RegrAvgyFun>();
	Register<RegrCountFun>();
	Register<RegrSlopeFun>();
	Register<RegrR2Fun>();
	Register<RegrSYYFun>();
	Register<RegrSXXFun>();
	Register<RegrSXYFun>();
	Register<RegrInterceptFun>();
}

} // namespace duckdb
