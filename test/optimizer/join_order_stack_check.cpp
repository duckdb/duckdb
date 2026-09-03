#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"

#if defined(__linux__) && !defined(DUCKDB_NO_THREADS) && !defined(DUCKDB_WASM_VERSION)
#include <pthread.h>

#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

namespace {

using namespace duckdb;
using std::exception;
using std::move;
using std::runtime_error;
using std::string;
using std::stringstream;

constexpr size_t TEST_STACK_SIZE = 256 * 1024;

struct ThreadState {
	ThreadState(ClientContext &context_p, duckdb::unique_ptr<LogicalOperator> plan_p)
	    : context(context_p), plan(std::move(plan_p)) {
	}

	ClientContext &context;
	duckdb::unique_ptr<LogicalOperator> plan;
	string error;
	bool optimizer_entered = false;
};

static void ExecuteQuery(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw runtime_error(result->GetError());
	}
}

static string BuildQuery() {
	std::stringstream query;
	query << R"SQL(
WITH latest_config AS (
	SELECT c.*
	FROM configs c
	WHERE (c.store_id, c.mobile_upload_date) IN (
		SELECT store_id, max(mobile_upload_date)
		FROM configs
		WHERE logic_state = 1 AND is_temp = 0
		GROUP BY store_id
	)
),
base AS (
	SELECT s.store_id, s.store_code, s.store_name, c.data_id,
	       c.mobile_upload_date, o.pi4, o.pi5, o.pi6
	FROM stores s
	LEFT JOIN latest_config c ON c.store_id = s.store_id
	LEFT JOIN store_organizations so ON so.store_id = s.store_id
	LEFT JOIN organizations o ON o.org_id = so.org_id
	LEFT JOIN distributor_stores ds ON ds.store_id = s.store_id
	LEFT JOIN stores distributor ON distributor.store_id = ds.dist_id
	WHERE s.store_type_id = 1
	  AND s.state = 1
	  AND o.pi4 = 100
	GROUP BY s.store_id, s.store_code, s.store_name, c.data_id,
	         c.mobile_upload_date, o.pi4, o.pi5, o.pi6
),
total AS (
	SELECT b.store_id,
		(SELECT org_name FROM organizations bo1 WHERE bo1.org_id = b.pi5) AS pi5_name,
		(SELECT org_name FROM organizations bo1 WHERE bo1.org_id = b.pi6) AS pi6_name
)SQL";

	for (idx_t i = 1; i <= 72; i++) {
		query << ",\n\t\t(SELECT max(iv.value) FROM item_values iv WHERE iv.data_id = b.data_id AND iv.item_id = "
		      << i << ") AS item_" << i;
	}

	query << "\n\tFROM base b\n)\nSELECT count(*)\nFROM total";
	return query.str();
}

static void *RunJoinOrderStackCheck(void *arg) {
	auto &state = *static_cast<ThreadState *>(arg);
	try {
		JoinOrderOptimizer optimizer(state.context);
		state.optimizer_entered = true;
		state.plan = optimizer.Optimize(std::move(state.plan));
		state.error = "query unexpectedly succeeded";
	} catch (const exception &ex) {
		state.error = ex.what();
	} catch (...) {
		state.error = "unknown error";
	}
	return nullptr;
}

TEST_CASE("Join-order optimizer checks the native stack", "[optimizer]") {
	DuckDB db(nullptr);
	Connection con(db);

	ExecuteQuery(con, "SET max_expression_depth = 1000");
	ExecuteQuery(con, R"SQL(
CREATE TABLE stores (
	store_id INTEGER,
	store_code VARCHAR,
	store_name VARCHAR,
	store_type_id INTEGER,
	state INTEGER
))SQL");
	ExecuteQuery(con, R"SQL(
CREATE TABLE configs (
	data_id INTEGER,
	store_id INTEGER,
	mobile_upload_date TIMESTAMP,
	logic_state INTEGER,
	is_temp INTEGER
))SQL");
	ExecuteQuery(con, R"SQL(
CREATE TABLE organizations (
	org_id INTEGER,
	org_name VARCHAR,
	pi4 INTEGER,
	pi5 INTEGER,
	pi6 INTEGER
))SQL");
	ExecuteQuery(con, R"SQL(
CREATE TABLE store_organizations (
	id INTEGER,
	store_id INTEGER,
	org_id INTEGER
))SQL");
	ExecuteQuery(con, R"SQL(
CREATE TABLE distributor_stores (
	store_id INTEGER,
	dist_id INTEGER
))SQL");
	ExecuteQuery(con, R"SQL(
CREATE TABLE item_values (
	data_id INTEGER,
	item_id INTEGER,
	value INTEGER
))SQL");

	ExecuteQuery(con, R"SQL(
INSERT INTO stores VALUES
	(1, 'S0001', 'demo store', 1, 1),
	(2, 'D0001', 'distributor', 2, 1)
)SQL");
	ExecuteQuery(con, R"SQL(
INSERT INTO configs VALUES
	(10, 1, TIMESTAMP '2025-01-01 00:00:00', 1, 0),
	(11, 1, TIMESTAMP '2024-01-01 00:00:00', 1, 0)
)SQL");
	ExecuteQuery(con, R"SQL(
INSERT INTO organizations VALUES
	(200, 'main org', 100, 1000, 1001),
	(1000, 'pi5 org', 0, 0, 0),
	(1001, 'pi6 org', 0, 0, 0)
)SQL");
	ExecuteQuery(con, "INSERT INTO store_organizations VALUES (1, 1, 200)");
	ExecuteQuery(con, "INSERT INTO distributor_stores VALUES (1, 2)");
	ExecuteQuery(con, "INSERT INTO item_values SELECT 10, item_id, item_id FROM range(1, 73) AS items(item_id)");

	con.BeginTransaction();
	Parser parser(con.context->GetParserOptions());
	parser.ParseQuery(BuildQuery());
	REQUIRE(parser.statements.size() == 1);
	Planner planner(*con.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	REQUIRE(planner.plan);

	ThreadState state(*con.context, std::move(planner.plan));
	pthread_attr_t attributes;
	REQUIRE(pthread_attr_init(&attributes) == 0);
	REQUIRE(pthread_attr_setstacksize(&attributes, TEST_STACK_SIZE) == 0);

	pthread_t thread;
	const auto create_result = pthread_create(&thread, &attributes, RunJoinOrderStackCheck, &state);
	REQUIRE(pthread_attr_destroy(&attributes) == 0);
	REQUIRE(create_result == 0);
	if (create_result != 0) {
		return;
	}

	REQUIRE(pthread_join(thread, nullptr) == 0);
	REQUIRE(state.optimizer_entered);
	INFO(state.error);
	REQUIRE(state.error.find("Insufficient stack space to process the query") != string::npos);
	con.Rollback();
}

} // namespace
#endif
