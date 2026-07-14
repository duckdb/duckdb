#include "duckdb/parser/statement/load_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/update_extensions_statement.hpp"
#include "duckdb/planner/operator/logical_update_extensions.hpp"
#include <algorithm>

namespace duckdb {

BoundStatement Binder::Bind(UpdateExtensionsStatement &stmt) {
	BoundStatement result;

	result.names.emplace_back("extension_name");
	result.types.emplace_back(LogicalType::VARCHAR);
	result.names.emplace_back("repository");
	result.types.emplace_back(LogicalType::VARCHAR);
	result.names.emplace_back("update_result");
	result.types.emplace_back(LogicalType::VARCHAR);
	result.names.emplace_back("previous_version");
	result.types.emplace_back(LogicalType::VARCHAR);
	result.names.emplace_back("current_version");
	result.types.emplace_back(LogicalType::VARCHAR);

	result.plan = make_uniq<LogicalUpdateExtensions>(std::move(stmt.info));

	return result;
}

} // namespace duckdb
