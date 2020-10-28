#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/named_parameter_binder.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/planner/operator/logical_pragma.hpp"
#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

BoundStatement Binder::Bind(PragmaStatement &stmt) {
	// evaluate the input parameters to the function
	// fill the stmt.info.parameters with positional params, then bind the function
	QueryErrorContext error_context(root_statement, stmt.stmt_location);

	vector<LogicalType> arguments;
	vector<Value> parameters;
	unordered_map<string, Value> named_parameters;
	NamedParameterBinder named_param_binder(*this, error_context);
	named_param_binder.EvaluateInputParameters(arguments, parameters, named_parameters, stmt.children, "PRAGMA");
	stmt.info->parameters.insert(stmt.info->parameters.begin(), parameters.begin(), parameters.end());

	auto &catalog = Catalog::GetCatalog(context);
	// bind the pragma function
	auto entry = catalog.GetEntry<PragmaFunctionCatalogEntry>(context, DEFAULT_SCHEMA, stmt.info->name, false);
	string error;
	idx_t bound_idx = Function::BindFunction(entry->name, entry->functions, arguments, error);
	if (bound_idx == INVALID_INDEX) {
		throw BinderException(FormatError(stmt.stmt_location, error));
	}
	auto &bound_function = entry->functions[bound_idx];
	if (!bound_function.function) {
		throw BinderException("PRAGMA function does not have a function specified");
	}

	// now check the named parameters
	named_param_binder.CheckNamedParameters(bound_function.named_parameters, named_parameters, bound_function.name);
	
	// cast the input parameters
	for (idx_t i = 0; i < stmt.info->parameters.size(); i++) {
		auto target_type =
		    i < bound_function.arguments.size() ? bound_function.arguments[i] : bound_function.varargs;
		stmt.info->parameters[i] = stmt.info->parameters[i].CastAs(target_type);
	}

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_unique<LogicalPragma>(bound_function, *stmt.info);
	return result;
}

} // namespace duckdb
