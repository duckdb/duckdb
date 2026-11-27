#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"

#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Stable C++ API tests: Context::ConnectInTransaction, running SQL from a
// bind callback in the transaction of the statement being bound.
// ---------------------------------------------------------------------------

namespace {

using namespace duckdb::cxx;

std::vector<int64_t> CollectBigints(QueryResult result) {
	std::vector<int64_t> out;
	while (auto chunk = result.FetchChunk()) {
		auto view = chunk.GetVector(0).GetView();
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			REQUIRE(view.IsValid(i));
			out.push_back(view.Data<int64_t>()[view.SelAt(i)]);
		}
	}
	return out;
}

// sql_in_bind(sql): runs `sql` (which produces BIGINTs) in the bind callback and returns the first value.
struct SQLInBindData {
	int64_t value = 0;
};
struct SQLInBindGlobal {
	bool done = false;
};

void SQLInBindBind(TableFunction::BindInput &input) {
	auto &context = input.GetContext();
	auto sql = std::string(input.GetConstantArgument(0).Get<varchar_t>().view());
	auto conn = context.ConnectInTransaction();
	auto values = CollectBigints(conn.Execute(sql));
	input.AddResultColumn("value", context.GetFactory().ParseType("BIGINT"));
	input.SetBindData<SQLInBindData>(SQLInBindData {values.empty() ? -1 : values[0]});
}

void SQLInBindInitGlobal(TableFunction::InitGlobalInput &input) {
	input.SetGlobalState<SQLInBindGlobal>(SQLInBindGlobal {});
}

void SQLInBindExec(TableFunction::ExecInput &input) {
	auto &global = input.GetGlobalState<SQLInBindGlobal>();
	auto chunk = input.GetOutputChunk();
	auto vec = chunk.GetVector(0);
	if (global.done) {
		vec.SetSize(0);
		return;
	}
	global.done = true;
	vec.GetDataMutable<int64_t>()[0] = input.GetBindData<SQLInBindData>().value;
	vec.SetSize(1);
}

// tx_in_bind(sql): runs a transaction statement in the bind callback, returns the error code it failed with.
void TxInBindBind(TableFunction::BindInput &input) {
	auto &context = input.GetContext();
	auto sql = std::string(input.GetConstantArgument(0).Get<varchar_t>().view());
	auto conn = context.ConnectInTransaction();
	int64_t code = 0;
	try {
		conn.Execute(sql).Drain();
	} catch (Exception &ex) {
		code = ex.GetCode();
	}
	input.AddResultColumn("value", context.GetFactory().ParseType("BIGINT"));
	input.SetBindData<SQLInBindData>(SQLInBindData {code});
}

void Register(Connection &conn, const std::string &name, TableFunction::BindCallback bind) {
	auto &factory = conn.GetFactory();
	auto function = TableFunction::Create(factory);
	function.SetName(name);
	function.GetSignature().AddParameter("sql", factory.ParseType("VARCHAR"));
	function.SetBindCallback(bind).SetInitGlobalCallback(SQLInBindInitGlobal).SetExecCallback(SQLInBindExec);
	conn.Register(function);
}

} // namespace

TEST_CASE("Stable C++API: a connection in the transaction of a bind callback", "[cpp_api]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();
	auto other = db.Connect();
	Register(conn, "sql_in_bind", SQLInBindBind);
	Register(conn, "tx_in_bind", TxInBindBind);
	conn.Execute("CREATE TABLE tbl AS SELECT 1::BIGINT AS i UNION ALL SELECT 2").Drain();

	SECTION("auto-commit") {
		REQUIRE(CollectBigints(conn.Execute("SELECT * FROM sql_in_bind('SELECT SUM(i) FROM tbl')")) ==
		        std::vector<int64_t> {3});
		// a write of the bind callback commits with the statement
		REQUIRE(CollectBigints(conn.Execute("SELECT * FROM sql_in_bind('INSERT INTO tbl VALUES (3)')")) ==
		        std::vector<int64_t> {1});
		REQUIRE(CollectBigints(other.Execute("SELECT SUM(i) FROM tbl")) == std::vector<int64_t> {6});
		// a failing query fails the statement being bound
		REQUIRE_THROWS_AS(conn.Execute("SELECT * FROM sql_in_bind('SELECT * FROM nonexistent')").Drain(), Exception);
		REQUIRE(CollectBigints(conn.Execute("SELECT 42::BIGINT")) == std::vector<int64_t> {42});
	}
	SECTION("explicit transaction: uncommitted changes and temporary tables are visible") {
		conn.Execute("BEGIN TRANSACTION").Drain();
		conn.Execute("INSERT INTO tbl VALUES (10)").Drain();
		conn.Execute("CREATE TEMPORARY TABLE tmp AS SELECT 5::BIGINT AS i").Drain();
		REQUIRE(CollectBigints(conn.Execute("SELECT * FROM sql_in_bind('SELECT SUM(i) FROM tbl')")) ==
		        std::vector<int64_t> {13});
		REQUIRE(CollectBigints(conn.Execute("SELECT * FROM sql_in_bind('SELECT SUM(i) FROM tmp')")) ==
		        std::vector<int64_t> {5});
		REQUIRE(CollectBigints(conn.Execute("SELECT * FROM sql_in_bind('INSERT INTO tbl VALUES (20)')")) ==
		        std::vector<int64_t> {1});
		REQUIRE(CollectBigints(conn.Execute("SELECT SUM(i) FROM tbl")) == std::vector<int64_t> {33});
		REQUIRE(CollectBigints(other.Execute("SELECT SUM(i) FROM tbl")) == std::vector<int64_t> {3});
		conn.Execute("ROLLBACK").Drain();
		REQUIRE(CollectBigints(conn.Execute("SELECT SUM(i) FROM tbl")) == std::vector<int64_t> {3});
	}
}

TEST_CASE("Stable C++API: transaction statements in a connection in the transaction of a callback", "[cpp_api]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();
	Register(conn, "tx_in_bind", TxInBindBind);
	for (auto &statement : {"COMMIT", "ROLLBACK", "BEGIN TRANSACTION"}) {
		// the refused statement fails, which invalidates the transaction of the statement being bound
		REQUIRE_THROWS_MATCHES(conn.Execute(std::string("SELECT * FROM tx_in_bind('") + statement + "')").Drain(),
		                       Exception, HasErrorCode(DUCKDB_V2_ERROR_DATABASE_TRANSACTION));
	}
}
