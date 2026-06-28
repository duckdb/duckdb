#include "duckdb/common/feature_serve.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/serve_feature_statement.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BoundStatement Binder::Bind(ServeFeatureStatement &stmt) {
	auto serve_sql = BuildServeFeatureSQL(context, stmt.feature_names, stmt.spine_table, stmt.entity_column,
	                                     stmt.as_of_column);

	Parser parser(context.GetParserOptions());
	parser.ParseQuery(serve_sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw InternalException("Generated SERVE FEATURE query did not produce a SELECT statement");
	}

	return Bind(parser.statements[0]->Cast<SelectStatement>());
}

} // namespace duckdb