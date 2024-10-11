#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

BoundStatement Binder::Bind(ExtensionStatement &stmt) {
	BoundStatement result;

	// perform the planning of the function
	D_ASSERT(stmt.extension.plan_function);
	auto parse_result =
	    stmt.extension.plan_function(stmt.extension.parser_info.get(), context, std::move(stmt.parse_data));

	auto &properties = GetStatementProperties();
	properties.modified_databases = parse_result.modified_databases;
	properties.requires_valid_transaction = parse_result.requires_valid_transaction;
	properties.return_type = parse_result.return_type;

	// create the plan as a scan of the given table function
	result.plan = BindTableFunction(parse_result.function, std::move(parse_result.parameters));
	D_ASSERT(result.plan->type == LogicalOperatorType::LOGICAL_GET);
	auto &get = result.plan->Cast<LogicalGet>();
	result.names = get.names;
	result.types = get.returned_types;
	get.ClearColumnIds();
	for (idx_t i = 0; i < get.returned_types.size(); i++) {
		get.AddColumnId(i);
	}
	return result;
}

} // namespace duckdb
