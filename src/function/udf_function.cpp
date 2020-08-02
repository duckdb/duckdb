#include "duckdb/function/udf_function.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

void UDFWrapper::RegisterFunction(string name, vector<SQLType> args, SQLType ret_type,
								  scalar_function_t udf_function, ClientContext &context,
								  SQLType varargs) {

	ScalarFunction scalar_function = ScalarFunction(name, args, ret_type, udf_function);
	scalar_function.varargs = varargs;
	CreateScalarFunctionInfo info(scalar_function);
	context.RegisterFunction(&info);
}

void UDFWrapper::RegisterAggrFunction(AggregateFunction aggr_function, ClientContext &context, SQLType varargs) {
	aggr_function.varargs = varargs;
	CreateAggregateFunctionInfo info(aggr_function);
	context.RegisterFunction(&info);
}

} // namespace duckdb
