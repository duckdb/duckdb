#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/planner.hpp"

using namespace duckdb;

TEST_CASE("Filter deduplication preserves scan pushdown accounting", "[optimizer][filter_combiner]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE filter_input(s VARCHAR, p BOOLEAN, q BOOLEAN)"));
	con.context->RunFunctionInTransaction([&]() {
		Parser parser(*con.context);
		parser.ParseQuery("SELECT * FROM filter_input WHERE s LIKE 'a%b' AND (p OR q)");
		Planner planner(*con.context);
		planner.CreatePlan(std::move(parser.statements[0]));
		Optimizer optimizer(*planner.binder, *con.context);
		optimizer.rewriter.VisitOperator(*planner.plan);
		optional_ptr<LogicalOperator> node = *planner.plan;
		while (node->type != LogicalOperatorType::LOGICAL_FILTER) {
			REQUIRE(node->children.size() == 1);
			node = *node->children[0];
		}
		auto &filter = node->Cast<LogicalFilter>();
		REQUIRE(filter.expressions.size() == 2);
		REQUIRE(filter.expressions[1]->GetExpressionType() == ExpressionType::CONJUNCTION_OR);
		auto &get = filter.children[0]->Cast<LogicalGet>();
		FilterCombiner combiner(*con.context);
		REQUIRE(combiner.AddFilter(filter.expressions[0]->Copy()) == FilterResult::SUCCESS);
		REQUIRE(combiner.AddFilter(filter.expressions[0]->Copy()) == FilterResult::SUCCESS);
		REQUIRE(combiner.AddFilter(filter.expressions[1]->Copy()) == FilterResult::SUCCESS);

		vector<FilterPushdownResult> pushdown_results;
		auto table_filters = combiner.GenerateTableScanFilters(get.GetColumnIds(), pushdown_results);
		REQUIRE(table_filters.HasFilters());
		vector<unique_ptr<Expression>> remaining;
		combiner.GenerateFilters([&](unique_ptr<Expression> expr) { remaining.push_back(std::move(expr)); });
		REQUIRE(remaining.size() == 2);
		REQUIRE(pushdown_results.size() == remaining.size());
		REQUIRE(pushdown_results[0] == FilterPushdownResult::PUSHED_DOWN_PARTIALLY);
		REQUIRE(pushdown_results[1] == FilterPushdownResult::NO_PUSHDOWN);
		REQUIRE(remaining[0]->Equals(*filter.expressions[0]));
		REQUIRE(remaining[1]->Equals(*filter.expressions[1]));
	});
}
