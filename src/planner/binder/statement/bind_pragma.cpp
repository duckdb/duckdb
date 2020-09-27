#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/planner/operator/logical_pragma.hpp"
#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

BoundStatement Binder::Bind(PragmaStatement &stmt) {
	auto &catalog = Catalog::GetCatalog(context);
	// bind the pragma function
	auto entry = catalog.GetEntry<PragmaFunctionCatalogEntry>(context, DEFAULT_SCHEMA, stmt.info->name, false);
	idx_t bound_idx = Function::BindFunction(entry->name, entry->functions, *stmt.info);
	auto &bound_function = entry->functions[bound_idx];
	if (!bound_function.function) {
		throw BinderException("PRAGMA function does not have a function specified");
	}

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_unique<LogicalPragma>(bound_function, *stmt.info);
	return result;
}

} // namespace duckdb
