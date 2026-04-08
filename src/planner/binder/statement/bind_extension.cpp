#include <string>
#include <unordered_map>
#include <utility>

#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

BoundStatement Binder::Bind(ExtensionStatement &stmt) {
	// perform the planning of the function
	D_ASSERT(stmt.extension.plan_function);
	auto parse_result =
	    stmt.extension.plan_function(stmt.extension.parser_info.get(), context, std::move(stmt.parse_data));

	auto &properties = GetStatementProperties();
	properties.modified_databases = parse_result.modified_databases;
	properties.requires_valid_transaction = parse_result.requires_valid_transaction;
	properties.return_type = parse_result.return_type;

	// create the plan as a scan of the given table function
	auto result = BindTableFunction(parse_result.function, std::move(parse_result.parameters));
	D_ASSERT(result.plan->type == LogicalOperatorType::LOGICAL_GET);
	auto &get = result.plan->Cast<LogicalGet>();
	get.ClearColumnIds();
	for (idx_t i = 0; i < get.returned_types.size(); i++) {
		get.AddColumnId(i);
	}
	return result;
}

} // namespace duckdb
