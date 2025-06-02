#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_list.hpp"
#include "duckdb/function/register_function_list_helper.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

struct MainRegisterContext {
	MainRegisterContext(Catalog &catalog, CatalogTransaction transaction) : catalog(catalog), transaction(transaction) {
	}

	Catalog &catalog;
	CatalogTransaction transaction;
};

struct MainRegister {
	template <class T>
	static void FillExtraInfo(T &info) {
	}

	template <class T>
	static void RegisterFunction(MainRegisterContext &context, T &info) {
		context.catalog.CreateFunction(context.transaction, info);
	}
};

struct ExtensionRegister {
	template <class T>
	static void FillExtraInfo(T &info) {
		info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	}

	template <class T>
	static void RegisterFunction(DatabaseInstance &db, T &info) {
		ExtensionUtil::RegisterFunction(db, std::move(info));
	}
};

template <class OP, class T>
static void FillExtraInfo(const StaticFunctionDefinition &function, T &info) {
	info.internal = true;
	info.alias_of = function.alias_of;
	FillFunctionDescriptions(function, info);
	OP::FillExtraInfo(info);
}

template <class OP, class REGISTER_CONTEXT>
static void RegisterFunctionList(REGISTER_CONTEXT &context, const StaticFunctionDefinition *functions) {
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
			FillExtraInfo<OP>(function, info);
			OP::RegisterFunction(context, info);
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
			FillExtraInfo<OP>(function, info);
			OP::RegisterFunction(context, info);
		} else {
			throw InternalException("Do not know how to register function of this type");
		}
	}
}

void FunctionList::RegisterExtensionFunctions(DatabaseInstance &db, const StaticFunctionDefinition *functions) {
	RegisterFunctionList<ExtensionRegister>(db, functions);
}

void FunctionList::RegisterFunctions(Catalog &catalog, CatalogTransaction transaction) {
	MainRegisterContext context(catalog, transaction);
	RegisterFunctionList<MainRegister>(context, FunctionList::GetInternalFunctionList());
}

} // namespace duckdb
