#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/alter_scalar_function_info.hpp"

namespace duckdb {

ScalarFunctionCatalogEntry::ScalarFunctionCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                                       CreateScalarFunctionInfo &info)
    : FunctionEntry(CatalogType::SCALAR_FUNCTION_ENTRY, catalog, schema, info), functions(info.functions) {
}

unique_ptr<CatalogEntry> ScalarFunctionCatalogEntry::AlterEntry(CatalogTransaction transaction, AlterInfo &info) {
	if (info.type != AlterType::ALTER_SCALAR_FUNCTION) {
		throw InternalException("Attempting to alter ScalarFunctionCatalogEntry with unsupported alter type");
	}
	auto &function_info = info.Cast<AlterScalarFunctionInfo>();
	if (function_info.alter_scalar_function_type != AlterScalarFunctionType::ADD_FUNCTION_OVERLOADS) {
		throw InternalException(
		    "Attempting to alter ScalarFunctionCatalogEntry with unsupported alter scalar function type");
	}
	auto &add_overloads = function_info.Cast<AddScalarFunctionOverloadInfo>();

	ScalarFunctionSet new_set = functions;
	if (!new_set.MergeFunctionSet(add_overloads.new_overloads->functions, true)) {
		throw BinderException(
		    "Failed to add new function overloads to function \"%s\": function overload already exists", name);
	}
	CreateScalarFunctionInfo new_info(std::move(new_set));
	new_info.internal = internal;
	new_info.description =
	    add_overloads.new_overloads->description.empty() ? description : add_overloads.new_overloads->description;
	new_info.parameter_names = add_overloads.new_overloads->parameter_names.empty()
	                               ? parameter_names
	                               : add_overloads.new_overloads->parameter_names;
	new_info.example = add_overloads.new_overloads->example.empty() ? example : add_overloads.new_overloads->example;
	return make_uniq<ScalarFunctionCatalogEntry>(catalog, schema, new_info);
}

} // namespace duckdb
