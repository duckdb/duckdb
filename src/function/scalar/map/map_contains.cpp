#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/function/scalar/list/contains_or_position.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/function/scalar/map_functions.hpp"

namespace duckdb {

static void MapContainsFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	const auto count = input.size();

	const auto &map_vec = input.data[0];
	const auto &key_vec = MapVector::GetKeys(map_vec);
	const auto &arg_vec = input.data[1];

	ListSearchOp<bool>(map_vec, key_vec, arg_vec, result, count);
}

static unique_ptr<Expression> BindMapContainsExpression(FunctionBindExpressionInput &input) {
	auto &key_type = input.children[1]->GetReturnType();
	auto &map_key_type = MapType::KeyType(input.children[0]->GetReturnType());
	auto key_collation = key_type.id() == LogicalTypeId::VARCHAR ? StringType::GetCollation(key_type) : string();
	auto map_collation =
	    map_key_type.id() == LogicalTypeId::VARCHAR ? StringType::GetCollation(map_key_type) : string();
	if (key_collation.empty() && map_collation.empty()) {
		return nullptr;
	}

	// Rebind collated map searches through the existing collation-aware list search.
	auto &catalog = Catalog::GetSystemCatalog(input.context);
	auto &map_keys = catalog.GetEntry<ScalarFunctionCatalogEntry>(
	    input.context, QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), "map_keys"));
	auto &list_contains = catalog.GetEntry<ScalarFunctionCatalogEntry>(
	    input.context, QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), "list_contains"));

	FunctionBinder function_binder(input.context);
	ErrorData error;
	vector<unique_ptr<Expression>> map_children;
	map_children.push_back(std::move(input.children[0]));
	auto keys = function_binder.BindScalarFunction(map_keys, std::move(map_children), error);
	if (!keys) {
		error.Throw();
	}

	vector<unique_ptr<Expression>> contains_children;
	contains_children.push_back(std::move(keys));
	contains_children.push_back(std::move(input.children[1]));
	auto result = function_binder.BindScalarFunction(list_contains, std::move(contains_children), error);
	if (!result) {
		error.Throw();
	}
	return result;
}

ScalarFunction MapContainsFun::GetFunction() {
	auto key_type = LogicalType::TEMPLATE("K");
	auto val_type = LogicalType::TEMPLATE("V");

	ScalarFunction fun("map_contains", {LogicalType::MAP(key_type, val_type), key_type}, LogicalType::BOOLEAN,
	                   MapContainsFunction);
	fun.SetBindExpressionCallback(BindMapContainsExpression);
	return fun;
}

} // namespace duckdb
