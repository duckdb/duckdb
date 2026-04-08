#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/main/query_parameters.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/planner/bound_statement.hpp"

namespace duckdb {

BoundStatement Binder::Bind(SelectStatement &stmt) {
	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::ALLOW_STREAMING;
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return Bind(*stmt.node);
}

} // namespace duckdb
