#include "duckdb/function/scalar/variant_path_function.hpp"

namespace duckdb {

ScalarFunctionSet VariantPathFunction::CreateFunctionSet(const string &name, const scalar_function_t &function,
                                                         const LogicalType &return_type, const bool path_optional) {
	ScalarFunctionSet fun_set;

	if (path_optional) {
		fun_set.AddFunction(ScalarFunction {
		    name, {LogicalType::VARIANT()}, return_type, function, VariantBindUtils::VariantPathBind, nullptr});
	}

	fun_set.AddFunction(ScalarFunction {name,
	                                    {LogicalType::VARIANT(), LogicalType::VARCHAR},
	                                    return_type,
	                                    function,
	                                    VariantBindUtils::VariantPathBind,
	                                    nullptr});

	fun_set.AddFunction(ScalarFunction {name,
	                                    {LogicalType::VARIANT(), LogicalType::LIST(LogicalType::VARCHAR)},
	                                    LogicalType::LIST(return_type),
	                                    function,
	                                    VariantBindUtils::VariantPathBind,
	                                    nullptr});

	return fun_set;
}

} // namespace duckdb
