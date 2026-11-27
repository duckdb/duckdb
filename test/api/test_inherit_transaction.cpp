#include "catch.hpp"
#include "duckdb.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;

namespace {

static void RequireValue(Connection &con, const string &query, int64_t expected) {
	auto result = con.Query(query);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected)}));
}

//! sql_in_bind(sql): runs `sql` in the binder's transaction and returns the first value it produced as VARCHAR
struct SQLInBindData : public TableFunctionData {
	Value value;
};

struct SQLInBindState : public GlobalTableFunctionState {
	bool done = false;
};

unique_ptr<FunctionData> SQLInBindBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto con = Connection::CreateInTransaction(context);
	auto result = con.Query(StringValue::Get(input.inputs[0]));
	if (result->HasError()) {
		result->ThrowError();
	}
	auto data = make_uniq<SQLInBindData>();
	auto &collection = result->Collection();
	data->value = collection.Count() == 0 ? Value(LogicalType::VARCHAR) : collection.GetValue(0, 0).ToString();
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("value");
	return std::move(data);
}

unique_ptr<GlobalTableFunctionState> SQLInBindInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<SQLInBindState>();
}

void SQLInBindScan(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<SQLInBindState>();
	if (state.done) {
		output.SetChildCardinality(0);
		return;
	}
	state.done = true;
	output.data[0].Append(input.bind_data->Cast<SQLInBindData>().value);
	output.SetChildCardinality(1);
}

//! sql_in_scan(sql): runs `sql` in the scan and returns whether it succeeded, swallowing its error
unique_ptr<FunctionData> SQLInScanBind(ClientContext &, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto data = make_uniq<SQLInBindData>();
	data->value = input.inputs[0];
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("success");
	return std::move(data);
}

void SQLInScanScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<SQLInBindState>();
	if (state.done) {
		output.SetChildCardinality(0);
		return;
	}
	state.done = true;
	auto con = Connection::CreateInTransaction(context);
	auto result = con.Query(StringValue::Get(input.bind_data->Cast<SQLInBindData>().value));
	output.data[0].Append(Value::BOOLEAN(!result->HasError()));
	output.SetChildCardinality(1);
}

void RegisterFunctions(Connection &con) {
	con.BeginTransaction();
	auto &context = *con.context;
	TableFunction in_bind("sql_in_bind", {LogicalType::VARCHAR}, SQLInBindScan, SQLInBindBind, SQLInBindInit);
	CreateTableFunctionInfo bind_info(in_bind);
	Catalog::GetSystemCatalog(context).CreateFunction(context, bind_info);
	TableFunction in_scan("sql_in_scan", {LogicalType::VARCHAR}, SQLInScanScan, SQLInScanBind, SQLInBindInit);
	CreateTableFunctionInfo scan_info(in_scan);
	Catalog::GetSystemCatalog(context).CreateFunction(context, scan_info);
	con.Commit();
}

} // namespace

TEST_CASE("Test a connection that inherits the transaction of another connection", "[api][transaction]") {
	DuckDB db(nullptr);
	Connection con(db);
	Connection other(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (1)"));

	SECTION("changes and temporary objects are visible both ways, and commit with the transaction") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (2)"));
		REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY TABLE tmp AS SELECT 10 AS i"));
		{
			auto child = Connection::CreateInTransaction(*con.context);
			RequireValue(child, "SELECT SUM(i) FROM tbl", 3);
			RequireValue(child, "SELECT SUM(i) FROM tmp", 10);
			REQUIRE_NO_FAIL(child.Query("INSERT INTO tbl VALUES (3)"));
			REQUIRE_NO_FAIL(child.Query("CREATE TABLE tbl2 AS SELECT 100 AS i"));
			RequireValue(con, "SELECT SUM(i) FROM tbl", 6);
			RequireValue(con, "SELECT SUM(i) FROM tbl2", 100);
			RequireValue(other, "SELECT SUM(i) FROM tbl", 1);
			// a connection that inherited from an inheriting connection is in the same transaction
			auto grandchild = Connection::CreateInTransaction(*child.context);
			REQUIRE_NO_FAIL(grandchild.Query("INSERT INTO tbl VALUES (4)"));
			RequireValue(con, "SELECT SUM(i) FROM tbl", 10);
		}
		// closing the connections did not end the transaction
		RequireValue(other, "SELECT SUM(i) FROM tbl", 1);
		REQUIRE_NO_FAIL(con.Query("COMMIT"));
		RequireValue(other, "SELECT SUM(i) FROM tbl", 10);
		RequireValue(other, "SELECT SUM(i) FROM tbl2", 100);
	}
	SECTION("a rollback undoes the changes of the inheriting connection") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		{
			auto child = Connection::CreateInTransaction(*con.context);
			REQUIRE_NO_FAIL(child.Query("INSERT INTO tbl VALUES (2)"));
		}
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
		RequireValue(con, "SELECT SUM(i) FROM tbl", 1);
	}
	SECTION("the inheriting connection cannot start or end a transaction") {
		for (auto &statement : {"COMMIT", "ROLLBACK", "BEGIN TRANSACTION", "JOIN TRANSACTION 'x'"}) {
			REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
			REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (2)"));
			{
				auto child = Connection::CreateInTransaction(*con.context);
				auto result = child.Query(statement);
				REQUIRE_FAIL(result);
				REQUIRE(StringUtil::Contains(result->GetError(), "inherited its transaction"));
				REQUIRE(!child.IsAutoCommit());
				REQUIRE(child.HasActiveTransaction());
				REQUIRE_THROWS(child.Commit());
			}
			// the refused statement failed in the transaction, which invalidated it: the COMMIT rolls back
			REQUIRE_FAIL(con.Query("SELECT 42"));
			REQUIRE_NO_FAIL(con.Query("COMMIT"));
			RequireValue(other, "SELECT SUM(i) FROM tbl", 1);
		}
	}
	SECTION("a failing statement of the inheriting connection invalidates the transaction") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (2)"));
		{
			auto child = Connection::CreateInTransaction(*con.context);
			REQUIRE_FAIL(child.Query("SELECT * FROM nonexistent"));
			REQUIRE_FAIL(child.Query("SELECT 42"));
		}
		REQUIRE_FAIL(con.Query("SELECT 42"));
		// the COMMIT of an invalidated transaction rolls it back
		REQUIRE_NO_FAIL(con.Query("COMMIT"));
		RequireValue(con, "SELECT SUM(i) FROM tbl", 1);
	}
	SECTION("the transaction cannot commit while an inheriting connection is open") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (2)"));
		auto child = Connection::CreateInTransaction(*con.context);
		REQUIRE_NO_FAIL(child.Query("INSERT INTO tbl VALUES (3)"));
		auto result = con.Query("COMMIT");
		REQUIRE_FAIL(result);
		REQUIRE(StringUtil::Contains(result->GetError(), "inherited the transaction is still open"));
		// the transaction was rolled back, the inheriting connection can only be closed
		REQUIRE(con.IsAutoCommit());
		REQUIRE_FAIL(child.Query("SELECT 42"));
		RequireValue(other, "SELECT SUM(i) FROM tbl", 1);
		child = Connection(db);
		RequireValue(con, "SELECT SUM(i) FROM tbl", 1);
		RequireValue(child, "SELECT SUM(i) FROM tbl", 1);
	}
	SECTION("a rollback while an inheriting connection is open") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		auto child = Connection::CreateInTransaction(*con.context);
		REQUIRE_NO_FAIL(child.Query("INSERT INTO tbl VALUES (3)"));
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
		REQUIRE_FAIL(child.Query("SELECT 42"));
		// the connection that left starts new transactions as usual
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (4)"));
		child = Connection(db);
		RequireValue(con, "SELECT SUM(i) FROM tbl", 5);
	}
	SECTION("closing the inheriting connection leaves the transaction, also while one of its results is open") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		unique_ptr<QueryResult> result;
		{
			auto child = Connection::CreateInTransaction(*con.context);
			REQUIRE_NO_FAIL(child.Query("INSERT INTO tbl VALUES (2)"));
			result = child.Submit("SELECT * FROM range(100000)");
			REQUIRE_NO_FAIL(*result);
		}
		REQUIRE_NO_FAIL(con.Query("COMMIT"));
		RequireValue(other, "SELECT SUM(i) FROM tbl", 3);
		result.reset();
	}
	SECTION("a transaction that was shared and joined") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		auto share_id = con.Query("SELECT share_transaction()")->Collection().GetValue(0, 0).ToString();
		REQUIRE_NO_FAIL(other.Query("JOIN TRANSACTION '" + share_id + "'"));
		auto child = Connection::CreateInTransaction(*other.context);
		REQUIRE_NO_FAIL(child.Query("INSERT INTO tbl VALUES (2)"));
		// one participant that did not inherit the transaction is left: it can still commit
		REQUIRE_NO_FAIL(con.Query("COMMIT"));
		REQUIRE_FAIL(other.Query("COMMIT"));
		REQUIRE_FAIL(child.Query("SELECT 42"));
		child = Connection(db);
		RequireValue(con, "SELECT SUM(i) FROM tbl", 1);
	}
	SECTION("errors") {
		// no transaction is active on a connection without a running statement in auto-commit mode
		REQUIRE_THROWS(Connection::CreateInTransaction(*con.context));
		// an aborted transaction cannot be inherited
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_FAIL(con.Query("SELECT * FROM nonexistent"));
		REQUIRE_THROWS(Connection::CreateInTransaction(*con.context));
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
		// a connection cannot inherit its own transaction
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_THROWS(con.context->transaction.InheritTransaction(*con.context));
		auto child = Connection::CreateInTransaction(*con.context);
		REQUIRE_THROWS(child.context->transaction.InheritTransaction(*con.context));
		child = Connection(db);
		REQUIRE_NO_FAIL(con.Query("COMMIT"));
	}
}

TEST_CASE("Test running SQL in a bind callback through an inheriting connection", "[api][transaction]") {
	DuckDB db(nullptr);
	Connection con(db);
	Connection other(db);
	RegisterFunctions(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (1), (2)"));

	SECTION("in auto-commit mode") {
		auto result = con.Query("SELECT * FROM sql_in_bind('SELECT SUM(i) FROM tbl')");
		REQUIRE(CHECK_COLUMN(result, 0, {"3"}));
		// a write in the bind callback commits with the statement
		result = con.Query("SELECT * FROM sql_in_bind('INSERT INTO tbl VALUES (3)')");
		REQUIRE(CHECK_COLUMN(result, 0, {"1"}));
		RequireValue(other, "SELECT SUM(i) FROM tbl", 6);
		// the outer statement sees what the bind callback wrote
		result = con.Query("SELECT (SELECT SUM(i) FROM tbl), * FROM sql_in_bind('INSERT INTO tbl VALUES (4)')");
		REQUIRE(CHECK_COLUMN(result, 0, {10}));
		// a write of the bind callback is rolled back with a statement that fails
		REQUIRE_FAIL(con.Query("SELECT error('fail'), * FROM sql_in_bind('INSERT INTO tbl VALUES (100)')"));
		RequireValue(other, "SELECT SUM(i) FROM tbl", 10);
		// a failing query in the bind callback fails the statement
		REQUIRE_FAIL(con.Query("SELECT * FROM sql_in_bind('SELECT * FROM nonexistent')"));
		REQUIRE_NO_FAIL(con.Query("SELECT 42"));
	}
	SECTION("in an explicit transaction") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (10)"));
		REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY TABLE tmp AS SELECT 5 AS i"));
		auto result = con.Query("SELECT * FROM sql_in_bind('SELECT SUM(i) FROM tbl')");
		REQUIRE(CHECK_COLUMN(result, 0, {"13"}));
		result = con.Query("SELECT * FROM sql_in_bind('SELECT SUM(i) FROM tmp')");
		REQUIRE(CHECK_COLUMN(result, 0, {"5"}));
		result = con.Query("SELECT * FROM sql_in_bind('INSERT INTO tbl VALUES (20)')");
		REQUIRE(CHECK_COLUMN(result, 0, {"1"}));
		RequireValue(con, "SELECT SUM(i) FROM tbl", 33);
		RequireValue(other, "SELECT SUM(i) FROM tbl", 3);
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
		RequireValue(other, "SELECT SUM(i) FROM tbl", 3);
	}
	SECTION("in the scan") {
		REQUIRE_NO_FAIL(con.Query("SET threads=1"));
		auto result = con.Query("SELECT * FROM sql_in_scan('INSERT INTO tbl VALUES (3)')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		RequireValue(other, "SELECT SUM(i) FROM tbl", 6);
		// a statement that failed in the scan invalidated the transaction: the statement cannot commit
		result = con.Query("SELECT * FROM sql_in_scan('INSERT INTO tbl VALUES (''x'')')");
		REQUIRE_FAIL(result);
		REQUIRE(StringUtil::Contains(result->GetError(), "Failed to commit"));
		REQUIRE_NO_FAIL(con.Query("SELECT 42"));
		// in an explicit transaction, the failure aborts the transaction
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (4)"));
		result = con.Query("SELECT * FROM sql_in_scan('SELECT * FROM nonexistent')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
		REQUIRE_FAIL(con.Query("SELECT 42"));
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
		RequireValue(other, "SELECT SUM(i) FROM tbl", 6);
	}
	SECTION("nested in the bind callback of an inheriting connection") {
		auto result = con.Query("SELECT * FROM sql_in_bind('SELECT * FROM sql_in_bind(''SELECT SUM(i) FROM tbl'')')");
		REQUIRE(CHECK_COLUMN(result, 0, {"3"}));
	}
	SECTION("names resolve through the search path of the source") {
		REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA s"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE s.only_in_s(i INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO s.only_in_s VALUES (10), (20)"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE s.tbl(i INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO s.tbl VALUES (100)"));
		REQUIRE_FAIL(con.Query("SELECT * FROM sql_in_bind('SELECT SUM(i) FROM only_in_s')"));
		REQUIRE_NO_FAIL(con.Query("SET schema = 's'"));
		auto result = con.Query("SELECT * FROM sql_in_bind('SELECT SUM(i) FROM only_in_s')");
		REQUIRE(CHECK_COLUMN(result, 0, {"30"}));
		result = con.Query("SELECT * FROM sql_in_bind('SELECT SUM(i) FROM tbl')");
		REQUIRE(CHECK_COLUMN(result, 0, {"100"}));
		result = con.Query("SELECT * FROM sql_in_bind('SELECT current_schema()')");
		REQUIRE(CHECK_COLUMN(result, 0, {"s"}));
		// a connection that did not set the schema still resolves through its own search path
		RequireValue(other, "SELECT SUM(i) FROM tbl", 3);
		REQUIRE_NO_FAIL(con.Query("RESET schema"));
		result = con.Query("SELECT * FROM sql_in_bind('SELECT SUM(i) FROM tbl')");
		REQUIRE(CHECK_COLUMN(result, 0, {"3"}));
	}
}

TEST_CASE("Test that an inheriting connection starts with the search path of its source", "[api][transaction]") {
	DuckDB db(nullptr);
	Connection con(db);
	RegisterFunctions(con);
	REQUIRE_NO_FAIL(con.Query("ATTACH ':memory:' AS other_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA other_db.s"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE other_db.s.tbl AS SELECT 42 AS i"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl AS SELECT 1 AS i"));

	auto result = con.Query("SELECT * FROM sql_in_bind('SELECT current_database() || ''.'' || current_schema()')");
	REQUIRE(CHECK_COLUMN(result, 0, {"memory.main"}));

	REQUIRE_NO_FAIL(con.Query("USE other_db.s"));
	result = con.Query("SELECT * FROM sql_in_bind('SELECT current_database() || ''.'' || current_schema()')");
	REQUIRE(CHECK_COLUMN(result, 0, {"other_db.s"}));
	result = con.Query("SELECT * FROM sql_in_bind('SELECT i FROM tbl')");
	REQUIRE(CHECK_COLUMN(result, 0, {"42"}));
	// unqualified names created by the inheriting connection land in the default schema of the source
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM sql_in_bind('CREATE TABLE created AS SELECT 7 AS i')"));
	RequireValue(con, "SELECT i::BIGINT FROM other_db.s.created", 7);

	// changing the search path of the inheriting connection leaves the source alone
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM sql_in_bind('USE memory')"));
	result = con.Query("SELECT current_database() || '.' || current_schema()");
	REQUIRE(CHECK_COLUMN(result, 0, {"other_db.s"}));

	REQUIRE_NO_FAIL(con.Query("SET search_path = 'memory.main'"));
	result = con.Query("SELECT * FROM sql_in_bind('SELECT current_setting(''search_path'')')");
	REQUIRE(CHECK_COLUMN(result, 0, {"memory.main"}));
	result = con.Query("SELECT * FROM sql_in_bind('SELECT i FROM tbl')");
	REQUIRE(CHECK_COLUMN(result, 0, {"1"}));
}
