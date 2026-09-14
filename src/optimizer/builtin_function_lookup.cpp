#include "duckdb/optimizer/builtin_function_lookup.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

//! Built-ins are always looked up fully qualified in the system catalog, so a macro or user-defined function of the
//! same name in the search path can never be selected instead.
QualifiedName BuiltinName(Catalog &catalog, const Identifier &name) {
	return QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), name);
}

} // namespace

shared_ptr<const ScalarFunction> GetBuiltinScalarFunction(ClientContext &context, const Identifier &name,
                                                          const vector<LogicalType> &arguments) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(context, BuiltinName(catalog, name));
	return entry.functions.GetFunctionByArguments(context, arguments);
}

shared_ptr<const AggregateFunction> GetBuiltinAggregateFunction(ClientContext &context, const Identifier &name,
                                                                const vector<LogicalType> &arguments) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(context, BuiltinName(catalog, name));
	return entry.functions.GetFunctionByArguments(context, arguments);
}

shared_ptr<const AggregateFunction> TryGetBuiltinAggregateFunction(ClientContext &context, const Identifier &name,
                                                                   const vector<LogicalType> &arguments) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(context, BuiltinName(catalog, name));

	ErrorData error;
	FunctionBinder function_binder(context);
	auto index = function_binder.BindFunction(entry.functions.name, entry.functions, arguments, error);
	if (!index.IsValid()) {
		return nullptr;
	}
	return entry.functions.GetFunctionByOffset(index.GetIndex());
}

unique_ptr<BoundFunctionExpression> BindBuiltinScalarFunction(ClientContext &context, const Identifier &name,
                                                              vector<unique_ptr<Expression>> children) {
	vector<LogicalType> arguments;
	arguments.reserve(children.size());
	for (auto &child : children) {
		arguments.push_back(child->GetReturnType());
	}
	return GetBuiltinScalarFunction(context, name, arguments)->Bind(context, std::move(children));
}

} // namespace duckdb
