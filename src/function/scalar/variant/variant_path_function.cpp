#include "duckdb/function/scalar/variant_path_function.hpp"

namespace duckdb {

ScalarFunctionSet VariantPathFunction::CreateFunctionSet(const Identifier &name, const scalar_function_t &function,
                                                         const LogicalType &return_type, const bool path_optional) {
	ScalarFunctionSet fun_set(name);

	if (path_optional) {
		fun_set.AddFunction(ScalarFunction {{{"input_variant", LogicalType::VARIANT()}},
		                                    return_type,
		                                    function,
		                                    VariantBindUtils::VariantPathBind,
		                                    nullptr});
	}

	fun_set.AddFunction(ScalarFunction {{{"input_variant", LogicalType::VARIANT()}, {"path", LogicalType::VARCHAR}},
	                                    return_type,
	                                    function,
	                                    VariantBindUtils::VariantPathBind,
	                                    nullptr});

	fun_set.AddFunction(
	    ScalarFunction {{{"input_variant", LogicalType::VARIANT()}, {"path", LogicalType::LIST(LogicalType::VARCHAR)}},
	                    LogicalType::LIST(return_type),
	                    function,
	                    VariantBindUtils::VariantPathBind,
	                    nullptr});

	return fun_set;
}

} // namespace duckdb
