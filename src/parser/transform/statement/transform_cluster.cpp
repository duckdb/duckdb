#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/cluster_statement.hpp"
#include "nodes/parsenodes.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

unique_ptr<ClusterStatement> Transformer::TransformCluster(duckdb_libpgquery::PGClusterStmt &stmt) {
	auto result = make_uniq<ClusterStatement>();

	result->target = TransformRangeVar(*stmt.targetTable);

	vector<OrderByNode> orders;
	TransformOrderBy(stmt.sortClause, orders);
	if (!orders.empty()) {
		auto order_modifier = make_uniq<OrderModifier>();
		result->modifiers = std::move(orders);
	} else {
		// ORDER BY is required for CLUSTER
		throw ParserException("CLUSTER statement requires an ORDER BY clause");
	}

	return result;
}

} // namespace duckdb
