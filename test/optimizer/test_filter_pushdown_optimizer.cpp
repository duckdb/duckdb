#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/common_subexpression.hpp"
#include "expression_helper.hpp"
#include "test_helpers.hpp"

#include <duckdb/optimizer/filter_pushdown.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include <vector>
using namespace duckdb;
using namespace std;

TEST_CASE("Test Table Filter Push Down", "[filterpushdown-optimizer]") {
	ExpressionHelper helper;
	auto &con = helper.con;
	Binder binder(*con.context);
	Optimizer opt(binder, *con.context);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer, j integer, k integer )"));
	//! Checking if Optimizer push predicates down
	auto tree = helper.ParseLogicalTree("SELECT k FROM integers where i+j > 10 and j = 5 and i = k+1 ");
	FilterPushdown predicatePushdown(opt);
	//! The generated plan should be Projection -> Filter (2) ->Get (1)
	auto plan = predicatePushdown.Rewrite(move(tree));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::FILTER);
	REQUIRE(plan->children[0]->expressions.size() == 2);

	REQUIRE(plan->children[0]->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->children[0]->expressions.size() == 1);

	tree = helper.ParseLogicalTree("SELECT k FROM integers where  j = 5 ");
	//! The generated plan should be Projection -> Get (1)
	plan = predicatePushdown.Rewrite(move(tree));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->expressions.size() == 1);
}

TEST_CASE("Test Table Filter Push Down Multiple Filters", "[filterpushdown-optimizer]") {
	ExpressionHelper helper;
	auto &con = helper.con;
	Binder binder(*con.context);
	Optimizer opt(binder, *con.context);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer, j integer, k integer )"));
	//! Checking if Optimizer push predicates down
	auto tree = helper.ParseLogicalTree("SELECT k FROM integers where j = 5 and i = 10 ");
	FilterPushdown predicatePushdown(opt);
	//! The generated plan should be Projection ->Get (2)
	auto plan = predicatePushdown.Rewrite(move(tree));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->expressions.size() == 2);
}

TEST_CASE("Test Table Filter All Numeric Data Types", "[filterpushdown-optimizer]") {
	vector<string> data_types{"tinyint", "smallint", "integer", "bigint", "numeric", "real", "date"};
	ExpressionHelper helper;
	auto &con = helper.con;
	Binder binder(*con.context);
	Optimizer opt(binder, *con.context);
	for (auto &data_type : data_types) {
		REQUIRE_NO_FAIL(
		    con.Query("CREATE TABLE tablinho(i " + data_type + " , j " + data_type + ", k " + data_type + " )"));
		//! Checking if Optimizer push predicates down
		auto tree = helper.ParseLogicalTree("SELECT k FROM tablinho where j = CAST( 1 AS " + data_type + ")");
        FilterPushdown predicatePushdown(opt);
        //! The generated plan should be Projection ->Get (2)
        auto plan = predicatePushdown.Rewrite(move(tree));
        REQUIRE(plan->children[0]->type == LogicalOperatorType::GET);
        REQUIRE(plan->children[0]->expressions.size() == 1);

        tree = helper.ParseLogicalTree("SELECT k FROM tablinho where j > CAST( 1 AS " + data_type + ")");
        //! The generated plan should be Projection ->Get (2)
        plan = predicatePushdown.Rewrite(move(tree));
        REQUIRE(plan->children[0]->type == LogicalOperatorType::GET);
        REQUIRE(plan->children[0]->expressions.size() == 1);

        tree = helper.ParseLogicalTree("SELECT k FROM tablinho where j >= CAST( 1 AS " + data_type + ")");
        //! The generated plan should be Projection ->Get (2)
        plan = predicatePushdown.Rewrite(move(tree));
        REQUIRE(plan->children[0]->type == LogicalOperatorType::GET);
        REQUIRE(plan->children[0]->expressions.size() == 1);

        tree = helper.ParseLogicalTree("SELECT k FROM tablinho where j < CAST( 1 AS " + data_type + ")");
        //! The generated plan should be Projection ->Get (2)
        plan = predicatePushdown.Rewrite(move(tree));
        REQUIRE(plan->children[0]->type == LogicalOperatorType::GET);
        REQUIRE(plan->children[0]->expressions.size() == 1);

        tree = helper.ParseLogicalTree("SELECT k FROM tablinho where j <= CAST( 1 AS " + data_type + ")");
        //! The generated plan should be Projection ->Get (2)
        plan = predicatePushdown.Rewrite(move(tree));
        REQUIRE(plan->children[0]->type == LogicalOperatorType::GET);
        REQUIRE(plan->children[0]->expressions.size() == 1);
	}
}

TEST_CASE("Test Index vs Pushdown", "[filterpushdown-optimizer]") {
	ExpressionHelper helper;
	auto &con = helper.con;
	Binder binder(*con.context);
	Optimizer opt(binder, *con.context);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer, j integer, k integer )"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));
	//! Checking if Optimizer push predicates down
	auto tree = helper.ParseLogicalTree("SELECT k FROM integers where i = 10 ");
	FilterPushdown predicatePushdown(opt);
	//! The generated plan should be Projection ->Filter -> Get
	auto plan = predicatePushdown.Rewrite(move(tree));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::FILTER);
	REQUIRE(plan->children[0]->children[0]->type == LogicalOperatorType::GET);
}

 TEST_CASE("Test Table Filter Push Down String", "[filterpushdown-optimizer]") {
    ExpressionHelper helper;
    auto &con = helper.con;
    Binder binder(*con.context);
    Optimizer opt(binder,*con.context);
    REQUIRE_NO_FAIL(con.Query("CREATE TABLE tablinho(i varchar )"));
    //! Checking if Optimizer push predicates down
    auto tree = helper.ParseLogicalTree("SELECT i FROM tablinho where i = 'bla' ");
    FilterPushdown predicatePushdown(opt);
    //! The generated plan should be Projection ->Get (1)
    auto plan = predicatePushdown.Rewrite(move(tree));
    REQUIRE(plan->children[0]->type == LogicalOperatorType::GET);
    REQUIRE(plan->children[0]->expressions.size()==1);
}

TEST_CASE("Test Table Filter Push Down Scan Integer", "[filterpushdown-optimizer]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	vector<int> input;
	idx_t input_size = 100000;
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer, j integer)"));
	for (idx_t i = 0; i < input_size; ++i) {
		input.push_back(i);
	}
	for (idx_t i = 0; i < input_size; ++i) {
		REQUIRE_NO_FAIL(
		    con.Query("INSERT INTO integers VALUES(" + to_string(input[i]) + "," + to_string(input[i]) + ")"));
	}

	result = con.Query("SELECT i FROM integers where j = 99000 ");
	REQUIRE(CHECK_COLUMN(result, 0, {99000}));

	result = con.Query("SELECT i FROM integers where j = 99000 and i = 20 ");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
}

TEST_CASE("Test Table Filter Push Down Scan String", "[filterpushdown-optimizer][.]") {
    unique_ptr<QueryResult> result;
    DuckDB db(nullptr);
    Connection con(db);

    vector<string> input {"pedro", "peter", "mark"};
    idx_t input_size = 100000;
    REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(i varchar)"));
    for (auto& value: input){
        for (size_t i = 0; i < input_size; i ++ ){
            con.Query("INSERT INTO strings VALUES('"+value+"')");
        }
    }
    result = con.Query("SELECT count(i) FROM strings where i = 'pedro' ");
    REQUIRE(CHECK_COLUMN(result, 0, {100000}));
    con.Query("INSERT INTO strings VALUES('po')");
    con.Query("INSERT INTO strings VALUES('stefan manegold')");
    con.Query("INSERT INTO strings VALUES('tim k')");
    con.Query("INSERT INTO strings VALUES('tim k')");
    con.Query("update strings set i = 'zorro' where i = 'pedro'");
    result = con.Query("SELECT count(i) FROM strings where i >= 'tim k' ");
    REQUIRE(CHECK_COLUMN(result, 0, {100002}));

}
