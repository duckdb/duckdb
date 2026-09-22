#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/planner/operator/logical_pragma.hpp"
#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

unique_ptr<BoundPragmaInfo> Binder::BindPragma(PragmaInfo &info, QueryErrorContext error_context) {
	vector<Value> params;
	// the named arguments are kept in a vector, as for every other function kind, so that they reach overload
	// selection by name and in the order they were written
	vector<pair<Identifier, Value>> named_arguments;

	// resolve the parameters
	ConstantBinder pragma_binder(*this, context, "PRAGMA value");
	for (auto &param : info.parameters) {
		auto bound_value = pragma_binder.Bind(param);
		auto value = ExpressionExecutor::EvaluateScalar(context, *bound_value, true);
		params.push_back(std::move(value));
	}

	for (auto &entry : info.named_parameters) {
		auto bound_value = pragma_binder.Bind(entry.second);
		auto value = ExpressionExecutor::EvaluateScalar(context, *bound_value, true);
		named_arguments.emplace_back(entry.first, std::move(value));
	}

	// bind the pragma function
	auto entry = Catalog::GetEntry<PragmaFunctionCatalogEntry>(
	    context, QualifiedName(Identifier::InvalidCatalog(), Identifier::DefaultSchema(), info.name),
	    OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		// try to find whether a table entry might exist
		auto table_entry = Catalog::GetEntry<TableFunctionCatalogEntry>(
		    context, QualifiedName(Identifier::InvalidCatalog(), Identifier::DefaultSchema(), info.name),
		    OnEntryNotFound::RETURN_NULL);
		if (table_entry) {
			// there is a table entry with the same name, now throw more explicit error message
			throw CatalogException("Pragma Function with name %s does not exist, but a table function with the same "
			                       "name exists, try `CALL %s(...)`",
			                       info.name, info.name);
		}
		// rebind to throw exception
		entry = Catalog::GetEntry<PragmaFunctionCatalogEntry>(
		    context, QualifiedName(Identifier::InvalidCatalog(), Identifier::DefaultSchema(), info.name),
		    OnEntryNotFound::THROW_EXCEPTION);
	}

	FunctionBinder function_binder(*this);
	ErrorData error;
	// selection, casting and named-argument checking all happen in the function binder
	auto bound_idx = function_binder.BindFunction(entry->name, entry->functions, params, named_arguments, error);
	if (!bound_idx.IsValid()) {
		D_ASSERT(error.HasError());
		error.AddQueryLocation(error_context);
		error.Throw();
	}
	auto bound_function = *entry->functions.GetFunctionByOffset(bound_idx.GetIndex());
	// the pragma implementations look their options up by name
	named_parameter_map_t named_parameters;
	for (auto &named_argument : named_arguments) {
		named_parameters.insert(make_pair(named_argument.first, std::move(named_argument.second)));
	}
	return make_uniq<BoundPragmaInfo>(std::move(bound_function), std::move(params), std::move(named_parameters));
}

BoundStatement Binder::Bind(PragmaStatement &stmt) {
	// bind the pragma function
	QueryErrorContext error_context(stmt.stmt_location);
	auto bound_info = BindPragma(*stmt.info, error_context);
	if (!bound_info->function.function) {
		throw BinderException("PRAGMA function does not have a function specified");
	}

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_uniq<LogicalPragma>(std::move(bound_info));

	auto &properties = GetStatementProperties();
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

} // namespace duckdb
