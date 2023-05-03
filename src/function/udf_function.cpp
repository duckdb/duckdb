#include "duckdb/function/udf_function.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

void UDFWrapper::RegisterFunction(string name, vector<LogicalType> args, LogicalType ret_type,
                                  scalar_function_t udf_function, ClientContext &context, LogicalType varargs) {

	ScalarFunction scalar_function(std::move(name), std::move(args), std::move(ret_type), std::move(udf_function));
	scalar_function.varargs = std::move(varargs);
	scalar_function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	CreateScalarFunctionInfo info(scalar_function);
	info.schema = DEFAULT_SCHEMA;
	context.RegisterFunction(info);
}

void UDFWrapper::RegisterAggrFunction(AggregateFunction aggr_function, ClientContext &context, LogicalType varargs) {
	aggr_function.varargs = std::move(varargs);
	CreateAggregateFunctionInfo info(std::move(aggr_function));
	context.RegisterFunction(info);
}

} // namespace duckdb
