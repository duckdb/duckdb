#include "duckdb/function/udf_function.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/main/client_context.hpp"


void UDFWrapper::RegisterFunction(string name, vector<SQLType> args, SQLType ret_type,
							 	 	 	 scalar_function_t udf_function, ClientContext &context) {

	ScalarFunction scalar_function = ScalarFunction(name, args, ret_type, udf_function);
	CreateScalarFunctionInfo info(scalar_function);
	context.RegisterFunction(&info);
}
