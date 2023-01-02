#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"

namespace duckdb {

BuiltinFunctions::BuiltinFunctions(CatalogTransaction transaction, Catalog &catalog)
    : transaction(transaction), catalog(catalog) {
}

BuiltinFunctions::~BuiltinFunctions() {
}

void BuiltinFunctions::AddCollation(string name, ScalarFunction function, bool combinable,
                                    bool not_required_for_equality) {
	CreateCollationInfo info(std::move(name), std::move(function), combinable, not_required_for_equality);
	info.internal = true;
	catalog.CreateCollation(transaction, &info);
}

void BuiltinFunctions::AddFunction(AggregateFunctionSet set) {
	CreateAggregateFunctionInfo info(std::move(set));
	info.internal = true;
	catalog.CreateFunction(transaction, &info);
}

void BuiltinFunctions::AddFunction(AggregateFunction function) {
	CreateAggregateFunctionInfo info(std::move(function));
	info.internal = true;
	catalog.CreateFunction(transaction, &info);
}

void BuiltinFunctions::AddFunction(PragmaFunction function) {
	CreatePragmaFunctionInfo info(std::move(function));
	info.internal = true;
	catalog.CreatePragmaFunction(transaction, &info);
}

void BuiltinFunctions::AddFunction(const string &name, PragmaFunctionSet functions) {
	CreatePragmaFunctionInfo info(name, std::move(functions));
	info.internal = true;
	catalog.CreatePragmaFunction(transaction, &info);
}

void BuiltinFunctions::AddFunction(ScalarFunction function) {
	CreateScalarFunctionInfo info(std::move(function));
	info.internal = true;
	catalog.CreateFunction(transaction, &info);
}

void BuiltinFunctions::AddFunction(const vector<string> &names, ScalarFunction function) { // NOLINT: false positive
	for (auto &name : names) {
		function.name = name;
		AddFunction(function);
	}
}

void BuiltinFunctions::AddFunction(ScalarFunctionSet set) {
	CreateScalarFunctionInfo info(std::move(set));
	info.internal = true;
	catalog.CreateFunction(transaction, &info);
}

void BuiltinFunctions::AddFunction(TableFunction function) {
	CreateTableFunctionInfo info(std::move(function));
	info.internal = true;
	catalog.CreateTableFunction(transaction, &info);
}

void BuiltinFunctions::AddFunction(TableFunctionSet set) {
	CreateTableFunctionInfo info(std::move(set));
	info.internal = true;
	catalog.CreateTableFunction(transaction, &info);
}

void BuiltinFunctions::AddFunction(CopyFunction function) {
	CreateCopyFunctionInfo info(std::move(function));
	info.internal = true;
	catalog.CreateCopyFunction(transaction, &info);
}

} // namespace duckdb
