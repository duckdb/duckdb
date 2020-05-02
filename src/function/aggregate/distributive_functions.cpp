#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate_function.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterDistributiveAggregates() {
	Register<BitAndFun>();
	Register<BitOrFun>();
	Register<BitXorFun>();
	Register<CountStarFun>();
	Register<CountFun>();
	Register<FirstFun>();
	Register<MaxFun>();
	Register<MinFun>();
	Register<SumFun>();
	Register<StringAggFun>();
}

} // namespace duckdb
