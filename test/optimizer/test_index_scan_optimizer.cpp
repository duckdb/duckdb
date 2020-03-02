#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "expression_helper.hpp"
#include "duckdb/optimizer/index_scan.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/common_subexpression.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Index Scan Optimizer for Integers", "[index-optimizer]") {
	ExpressionHelper helper;
	auto &con = helper.con;
	string int_types[1] = {"integer"}; // {"tinyint", "smallint", "integer", "bigint"};

	for (int idx = 0; idx < 1; idx++) {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i " + int_types[idx] + ")"));
		//! Checking Order Index
		con.Query("CREATE INDEX i_index ON integers(i)");
		// Checking if Optimizer is using index in simple case
		auto tree = helper.ParseLogicalTree("SELECT i FROM integers where i > CAST(10 AS " + int_types[idx] + ")");
		IndexScan index_scan;
		auto plan = index_scan.Optimize(move(tree));
		REQUIRE(plan->children[0]->type == LogicalOperatorType::FILTER);
		REQUIRE(plan->children[0]->children[0]->type == LogicalOperatorType::INDEX_SCAN);
		con.Query("DROP INDEX i_index");
	}
}

// FIXME: These tests make no sense with the current matching system
// Write proper tests after reworking optimization

// TEST_CASE("Test Index Scan Optimizer for Floats", "[index-optimizer-float]") {
//    ExpressionHelper helper;
//    auto &con = helper.con;
//    string int_types[2] = {"real","double"}; // {"tinyint", "smallint", "integer", "bigint"};
//
//    for (int idx = 0; idx < 1; idx++) {
//        REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i " + int_types[idx] + ")"));
//        //! Checking Order Index
//        con.Query("CREATE INDEX i_index ON integers(i)");
//        // Checking if Optimizer is using index in simple case
//        auto tree = helper.ParseLogicalTree("SELECT i FROM integers where i > CAST(10 AS " + int_types[idx] + ")");
//        IndexScan index_scan;
//        auto plan = index_scan.Optimize(move(tree));
//        REQUIRE(plan->children[0]->type == LogicalOperatorType::FILTER);
//        REQUIRE(plan->children[0]->children[0]->type == LogicalOperatorType::INDEX_SCAN);
//        con.Query("DROP INDEX i_index");
//    }
//}
//
// TEST_CASE("Test Index Scan Optimizer for Strings", "[index-optimizer-string]") {
//    ExpressionHelper helper;
//    auto &con = helper.con;
//
//    REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(i  varchar)"));
//    //! Checking Order Index
//    con.Query("CREATE INDEX i_index ON strings(i)");
//    // Checking if Optimizer is using index in simple case
//    auto tree = helper.ParseLogicalTree("SELECT i FROM strings where i >= 'testa' AND i <= 'testz'");
//    IndexScan index_scan;
//    auto plan = index_scan.Optimize(move(tree));
//    REQUIRE(plan->children[0]->type == LogicalOperatorType::FILTER);
//    REQUIRE(plan->children[0]->children[0]->type == LogicalOperatorType::INDEX_SCAN);
//    con.Query("DROP INDEX i_index");
//}
