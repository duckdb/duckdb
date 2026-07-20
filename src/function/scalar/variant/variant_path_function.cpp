#include "duckdb/function/scalar/variant_path_function.hpp"

namespace duckdb {

ScalarFunctionSet VariantPathFunction::CreateFunctionSet(const Identifier &name, const scalar_function_t &function,
                                                         const LogicalType &return_type, const bool path_optional,
                                                         const init_local_state_t init_state) {
	ScalarFunctionSet fun_set(name);

	if (path_optional) {
		fun_set.AddFunction(ScalarFunction {
		    {LogicalType::VARIANT()}, return_type, function, VariantBindUtils::VariantPathBind, nullptr, init_state});
	}

	fun_set.AddFunction(ScalarFunction {{LogicalType::VARIANT(), LogicalType::VARCHAR},
	                                    return_type,
	                                    function,
	                                    VariantBindUtils::VariantPathBind,
	                                    nullptr,
	                                    init_state});

	fun_set.AddFunction(ScalarFunction {{LogicalType::VARIANT(), LogicalType::LIST(LogicalType::VARCHAR)},
	                                    LogicalType::LIST(return_type),
	                                    function,
	                                    VariantBindUtils::VariantPathBind,
	                                    nullptr,
	                                    init_state});

	return fun_set;
}

} // namespace duckdb
