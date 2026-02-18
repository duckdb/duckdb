#include "duckdb/catalog/catalog.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"

namespace duckdb {

static bool IsValidTypeLookup(optional_ptr<CatalogEntry> entry) {
	if (!entry) {
		return false;
	}
	return entry->Cast<TypeCatalogEntry>().user_type.id() != LogicalTypeId::INVALID;
}

BindResult ExpressionBinder::BindExpression(TypeExpression &type_expr, idx_t depth) {
	auto &type_name = type_expr.GetTypeName();
	auto type_schema = type_expr.GetSchema();
	auto type_catalog = type_expr.GetCatalog();

	QueryErrorContext error_context(type_expr);
	EntryLookupInfo type_lookup(CatalogType::TYPE_ENTRY, type_name, error_context);

	optional_ptr<CatalogEntry> entry = nullptr;

	binder.BindSchemaOrCatalog(context, type_catalog, type_schema);

	// Required for WAL lookup to work
	if (type_catalog.empty() && !DatabaseManager::Get(context).HasDefaultDatabase()) {
		// Look in the system catalog if no catalog was specified
		entry = binder.entry_retriever.GetEntry(SYSTEM_CATALOG, type_schema, type_lookup);
	} else {
		// Try to search from most specific to least specific
		// The search path should already have been set to the correct catalog/schema,
		// in case we are looking for a type in the same schema as a table we are creating

		entry = binder.entry_retriever.GetEntry(type_catalog, type_schema, type_lookup, OnEntryNotFound::RETURN_NULL);

		if (!IsValidTypeLookup(entry)) {
			if (!type_catalog.empty() || !type_schema.empty()) {
				entry = binder.entry_retriever.GetEntry(type_catalog, type_schema, type_lookup,
				                                        OnEntryNotFound::THROW_EXCEPTION);
			}
			entry = binder.entry_retriever.GetEntry(type_catalog, INVALID_SCHEMA, type_lookup,
			                                        OnEntryNotFound::RETURN_NULL);
		}
		if (!IsValidTypeLookup(entry)) {
			entry = binder.entry_retriever.GetEntry(INVALID_CATALOG, INVALID_SCHEMA, type_lookup,
			                                        OnEntryNotFound::RETURN_NULL);
		}
		if (!IsValidTypeLookup(entry)) {
			entry = binder.entry_retriever.GetEntry(SYSTEM_CATALOG, DEFAULT_SCHEMA, type_lookup,
			                                        OnEntryNotFound::THROW_EXCEPTION);
		}
	}

	// By this point we have to have found a type in the catalog
	D_ASSERT(entry != nullptr);
	auto &type_entry = entry->Cast<TypeCatalogEntry>();

	// Now handle type parameters
	auto &unbound_parameters = type_expr.GetChildren();

	if (!type_entry.bind_function) {
		if (!unbound_parameters.empty()) {
			// This type does not support type parameters
			throw BinderException(type_expr, "Type '%s' does not take any type parameters", type_name);
		}

		// Otherwise, return the user type directly!
		auto result_expr = make_uniq<BoundConstantExpression>(Value::TYPE(type_entry.user_type));
		result_expr->query_location = type_expr.GetQueryLocation();
		return BindResult(std::move(result_expr));
	}

	// Bind value parameters
	vector<TypeArgument> bound_parameters;

	for (auto &param : unbound_parameters) {
		// Otherwise, try to fold it to a constant value
		ConstantBinder binder(this->binder, context, StringUtil::Format("Type parameter for type '%s'", type_name));

		auto expr = param->Copy();
		auto bound_expr = binder.Bind(expr);

		if (!bound_expr->IsFoldable()) {
			throw BinderException(type_expr, "Type parameter expression for type '%s' is not a constant", type_name);
		}

		// Shortcut for constant expressions
		if (bound_expr->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &const_expr = bound_expr->Cast<BoundConstantExpression>();
			bound_parameters.emplace_back(param->GetAlias(), const_expr.value);
			continue;
		}

		// Otherwise we need to evaluate the expression
		auto bound_param = ExpressionExecutor::EvaluateScalar(context, *bound_expr);
		bound_parameters.emplace_back(param->GetAlias(), bound_param);
	};

	// Call the bind function
	BindLogicalTypeInput input {context, type_entry.user_type, bound_parameters};
	auto result_type = type_entry.bind_function(input);

	// Return the resulting type!
	auto result_expr = make_uniq<BoundConstantExpression>(Value::TYPE(result_type));
	result_expr->query_location = type_expr.GetQueryLocation();
	return BindResult(std::move(result_expr));
}

} // namespace duckdb
