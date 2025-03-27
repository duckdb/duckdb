#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_list.hpp"
#include "duckdb/function/register_function_list_helper.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

template <class T>
static void FillExtraInfo(const StaticFunctionDefinition &function, T &info) {
	info.internal = true;
	info.alias_of = function.alias_of;
	FillFunctionDescriptions(function, info);
}

static void RegisterFunctionList(Catalog &catalog, CatalogTransaction transaction,
                                 const StaticFunctionDefinition *functions) {
	for (idx_t i = 0; functions[i].name; i++) {
		auto &function = functions[i];
		if (function.get_function || function.get_function_set) {
			// scalar function
			ScalarFunctionSet result;
			if (function.get_function) {
				result.AddFunction(function.get_function());
			} else {
				result = function.get_function_set();
			}
			result.name = function.name;
			CreateScalarFunctionInfo info(result);
			FillExtraInfo(function, info);
			catalog.CreateFunction(transaction, info);
		} else if (function.get_aggregate_function || function.get_aggregate_function_set) {
			// aggregate function
			AggregateFunctionSet result;
			if (function.get_aggregate_function) {
				result.AddFunction(function.get_aggregate_function());
			} else {
				result = function.get_aggregate_function_set();
			}
			result.name = function.name;
			CreateAggregateFunctionInfo info(result);
			FillExtraInfo(function, info);
			catalog.CreateFunction(transaction, info);
		} else {
			throw InternalException("Do not know how to register function of this type");
		}
	}
}

void FunctionList::RegisterFunctions(Catalog &catalog, CatalogTransaction transaction) {
	RegisterFunctionList(catalog, transaction, FunctionList::GetInternalFunctionList());
}

} // namespace duckdb
