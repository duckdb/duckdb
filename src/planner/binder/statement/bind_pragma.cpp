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
	string error;
	idx_t bound_idx = Function::BindFunction(entry->name, entry->functions, *stmt.info, error);
	if (bound_idx == INVALID_INDEX) {
		throw BinderException(FormatError(stmt.stmt_location, error));
	}
	auto &bound_function = entry->functions[bound_idx];
	if (!bound_function.function) {
		throw BinderException("PRAGMA function does not have a function specified");
	}

	QueryErrorContext error_context(root_statement, stmt.stmt_location);
	// bind and check named params
	for (auto &kv : stmt.info->named_parameters) {
		auto entry = bound_function.named_parameters.find(kv.first);
		if (entry == bound_function.named_parameters.end()) {
			// create a list of named parameters for the error
			string named_params;
			for (auto &kv : bound_function.named_parameters) {
				named_params += "    " + kv.first + " " + kv.second.ToString() + "\n";
			}
			if (named_params.empty()) {
				named_params = "Function does not accept any named parameters.";
			} else {
				named_params = "Candidates: " + named_params;
			}
			throw BinderException(error_context.FormatError("Invalid named parameter \"%s\" for function %s\n%s",
			                                                kv.first, bound_function.name, named_params));
		}
		kv.second = kv.second.CastAs(entry->second);
	}

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_unique<LogicalPragma>(bound_function, *stmt.info);
	return result;
}

} // namespace duckdb
