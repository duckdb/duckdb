#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/common_subexpression.hpp"
#include "expression_helper.hpp"
#include "test_helpers.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
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
	vector<string> data_types{"tinyint", "smallint", "integer", "bigint", "numeric", "real"};
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
		REQUIRE_NO_FAIL(con.Query("DROP TABLE tablinho"));
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
	Optimizer opt(binder, *con.context);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tablinho(i varchar )"));
	//! Checking if Optimizer push predicates down
	auto tree = helper.ParseLogicalTree("SELECT i FROM tablinho where i = 'bla' ");
	//! The generated plan should be Projection ->Get (1)
	auto plan = opt.Optimize(move(tree));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->expressions.size() == 1);

	tree = helper.ParseLogicalTree("SELECT i FROM tablinho where i = 'bla' ");
	//! The generated plan should be Projection ->Get (1)
	plan = opt.Optimize(move(tree));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->expressions.size() == 1);

	tree = helper.ParseLogicalTree("SELECT i FROM tablinho where i > 'bla' ");
	//! The generated plan should be Projection ->Get (1)
	plan = opt.Optimize(move(tree));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->expressions.size() == 1);

	tree = helper.ParseLogicalTree("SELECT i FROM tablinho where i >= 'bla' ");
	//! The generated plan should be Projection ->Get (1)
	plan = opt.Optimize(move(tree));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->expressions.size() == 1);

	tree = helper.ParseLogicalTree("SELECT i FROM tablinho where i < 'bla' ");
	//! The generated plan should be Projection ->Get (1)
	plan = opt.Optimize(move(tree));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->expressions.size() == 1);

	tree = helper.ParseLogicalTree("SELECT i FROM tablinho where i <= 'bla' ");
	//! The generated plan should be Projection ->Get (1)
	plan = opt.Optimize(move(tree));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->expressions.size() == 1);

	tree = helper.ParseLogicalTree("SELECT i FROM tablinho where i like 'bla%' ");

	//! The generated plan should be Projection ->filter(1)->get(0,2)
	plan = opt.Optimize(move(tree));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::FILTER);
	REQUIRE(plan->children[0]->expressions.size() == 1);
	REQUIRE(plan->children[0]->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->children[0]->expressions.size() == 0);
	REQUIRE(((LogicalGet *)plan->children[0]->children[0].get())->tableFilters.size() == 2);

	tree = helper.ParseLogicalTree("SELECT i FROM tablinho where i like 'bla_bla' ");

	//! The generated plan should be Projection ->filter(1)->get(0,2)
	plan = opt.Optimize(move(tree));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::FILTER);
	REQUIRE(plan->children[0]->expressions.size() == 1);
	REQUIRE(plan->children[0]->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->children[0]->expressions.size() == 0);
	REQUIRE(((LogicalGet *)plan->children[0]->children[0].get())->tableFilters.size() == 2);

	//! The generated plan should be Projection ->filter(1)->get(0,1)
	tree = helper.ParseLogicalTree("SELECT i FROM tablinho where i like 'bla' ");
	plan = opt.Optimize(move(tree));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::FILTER);
	REQUIRE(plan->children[0]->expressions.size() == 1);
	REQUIRE(plan->children[0]->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->children[0]->expressions.size() == 0);
	REQUIRE(((LogicalGet *)plan->children[0]->children[0].get())->tableFilters.size() == 1);

	//! The generated plan should be Projection ->filter(1)->get(0,0)
	tree = helper.ParseLogicalTree("SELECT i FROM tablinho where i like '%bla' ");
	plan = opt.Optimize(move(tree));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::FILTER);
	REQUIRE(plan->children[0]->expressions.size() == 1);
	REQUIRE(plan->children[0]->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->children[0]->expressions.size() == 0);
	REQUIRE(((LogicalGet *)plan->children[0]->children[0].get())->tableFilters.size() == 0);

	//! The generated plan should be Projection ->filter(1)->get(0,0)
	tree = helper.ParseLogicalTree("SELECT i FROM tablinho where i like '_bla' ");
	plan = opt.Optimize(move(tree));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::FILTER);
	REQUIRE(plan->children[0]->expressions.size() == 1);
	REQUIRE(plan->children[0]->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->children[0]->expressions.size() == 0);
	REQUIRE(((LogicalGet *)plan->children[0]->children[0].get())->tableFilters.size() == 0);
}

TEST_CASE("Test Table Filter Push Down Scan", "[filterpushdown-optimizer][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	vector<int> input;
	idx_t input_size = 100;
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer, j integer)"));
	for (idx_t i = 0; i < input_size; ++i) {
		input.push_back(i);
	}
	for (idx_t i = 0; i < input_size; ++i) {
		REQUIRE_NO_FAIL(
		    con.Query("INSERT INTO integers VALUES(" + to_string(input[i]) + "," + to_string(input[i]) + ")"));
	}

	result = con.Query("SELECT j FROM integers where j = 99 ");
	REQUIRE(CHECK_COLUMN(result, 0, {99}));

	result = con.Query("SELECT i FROM integers where j = 99 and i = 99 ");
	REQUIRE(CHECK_COLUMN(result, 0, {99}));

	result = con.Query("SELECT i FROM integers where j = 99 and i = 90 ");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	result = con.Query("SELECT count(i) FROM integers where j > 90 and i < 95 ");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));

	result = con.Query("SELECT count(i) FROM integers where j > 90 and j < 95 ");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
}

TEST_CASE("Test Table Filter Push Down Scan TPCQ6", "[filterpushdown-optimizer]") {
	ExpressionHelper helper;
	auto &con = helper.con;
	Binder binder(*con.context);
	Optimizer opt(binder, *con.context);
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE LINEITEM(L_ORDERKEY INTEGER NOT NULL, L_PARTKEY INTEGER NOT NULL,L_SUPPKEY  INTEGER "
	              "NOT NULL, L_LINENUMBER  INTEGER NOT NULL,L_QUANTITY    DECIMAL(15,2) NOT NULL,L_EXTENDEDPRICE  "
	              "DECIMAL(15,2) NOT NULL,  L_DISCOUNT    DECIMAL(15,2) NOT NULL, L_TAX  DECIMAL(15,2) NOT "
	              "NULL,L_RETURNFLAG  CHAR(1) NOT NULL, L_LINESTATUS  CHAR(1) NOT NULL, L_SHIPDATE    DATE NOT NULL, "
	              "L_COMMITDATE  DATE NOT NULL,L_RECEIPTDATE DATE NOT NULL, L_SHIPINSTRUCT CHAR(25) NOT NULL, "
	              "L_SHIPMODE     CHAR(10) NOT NULL, L_COMMENT      VARCHAR(44) NOT NULL)"));

	//! Checking if Optimizer push predicates down
	auto tree = helper.ParseLogicalTree(
	    "select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate >= '1994-01-01' and "
	    "l_shipdate < '1995-01-01'  and l_discount between 0.05 and 0.07 and l_quantity < 24 ");

	//! The generated plan should be Projection ->Aggregate_and_group_by->Get (5)
	auto plan = opt.Optimize(move(tree));
	REQUIRE(plan->type == LogicalOperatorType::PROJECTION);
	REQUIRE(plan->children[0]->type == LogicalOperatorType::AGGREGATE_AND_GROUP_BY);
	REQUIRE(plan->children[0]->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->children[0]->expressions.size() == 5);
}

TEST_CASE("Test Table Filter Push Down Scan String", "[filterpushdown-optimizer][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	vector<string> input{"pedro", "peter", "mark"};
	idx_t input_size = 100000;
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(i varchar)"));
	for (auto &value : input) {
		for (size_t i = 0; i < input_size; i++) {
			con.Query("INSERT INTO strings VALUES('" + value + "')");
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
