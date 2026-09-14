#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/logical_operator.hpp"

using namespace duckdb;

static bool ContainsOperator(const LogicalOperator &op, LogicalOperatorType type) {
	if (op.type == type) {
		return true;
	}
	for (auto &child : op.children) {
		if (ContainsOperator(*child, type)) {
			return true;
		}
	}
	return false;
}

TEST_CASE("Comparison extraction keeps inseparable operands in the join predicate", "[planner][join]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	for (const auto &join_type : {"MARK", "RIGHT_SEMI", "RIGHT_ANTI"}) {
		for (const auto &condition : {"a = a+1", "b = b+1", "a+b = 1"}) {
			Parser parser(connection.context->GetParserOptions());
			parser.ParseQuery(string("SELECT * FROM (VALUES (1)) l(a) JOIN BY (TYPE ") + join_type +
			                  ") (VALUES (2)) r(b) ON " + condition);
			Planner planner(*connection.context);
			planner.CreatePlan(std::move(parser.statements[0]));
			REQUIRE(ContainsOperator(*planner.plan, LogicalOperatorType::LOGICAL_ANY_JOIN));
			REQUIRE_FALSE(ContainsOperator(*planner.plan, LogicalOperatorType::LOGICAL_COMPARISON_JOIN));
		}
	}
	connection.Rollback();
}
