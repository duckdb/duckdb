#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

BoundStatement Binder::Bind(ExtensionStatement &stmt) {
	BoundStatement result;

	// perform the planning of the function
	D_ASSERT(stmt.extension.plan_function);
	auto parse_result = stmt.extension.plan_function(stmt.extension.parser_info.get(), context, move(stmt.parse_data));

	properties.read_only = parse_result.read_only;
	properties.requires_valid_transaction = parse_result.requires_valid_transaction;
	properties.return_type = parse_result.return_type;

	// create the plan as a scan of the given table function
	result.plan = BindTableFunction(parse_result.function, move(parse_result.parameters));
	D_ASSERT(result.plan->type == LogicalOperatorType::LOGICAL_GET);
	auto &get = (LogicalGet &)*result.plan;
	result.names = get.names;
	result.types = get.returned_types;
	get.column_ids.clear();
	for (idx_t i = 0; i < get.returned_types.size(); i++) {
		get.column_ids.push_back(i);
	}
	return result;
}

} // namespace duckdb
