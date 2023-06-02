#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/planner/operator/logical_pragma.hpp"
#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/function_binder.hpp"

namespace duckdb {

BoundStatement Binder::Bind(PragmaStatement &stmt) {
	// bind the pragma function
	auto &entry =
	    Catalog::GetEntry<PragmaFunctionCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, stmt.info->name);
	string error;
	FunctionBinder function_binder(context);
	idx_t bound_idx = function_binder.BindFunction(entry.name, entry.functions, *stmt.info, error);
	if (bound_idx == DConstants::INVALID_INDEX) {
		throw BinderException(FormatError(stmt.stmt_location, error));
	}
	auto bound_function = entry.functions.GetFunctionByOffset(bound_idx);
	if (!bound_function.function) {
		throw BinderException("PRAGMA function does not have a function specified");
	}

	// bind and check named params
	QueryErrorContext error_context(root_statement, stmt.stmt_location);
	BindNamedParameters(bound_function.named_parameters, stmt.info->named_parameters, error_context,
	                    bound_function.name);

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_uniq<LogicalPragma>(bound_function, *stmt.info);
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

} // namespace duckdb
