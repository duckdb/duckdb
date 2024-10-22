#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

static void CreateSimpleTable(Connection &con) {
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a (i TINYINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (11), (12), (13)"));
}

static void ModifySimpleTable(Connection &con) {
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (14)"));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM a where i=12"));
}

static void CheckSimpleQueryPrepareExecute(Connection &con) {
	auto statements = con.ExtractStatements("SELECT COUNT(*) FROM a WHERE i=?");
	REQUIRE(statements.size() == 1);
	duckdb::vector<Value> values = {Value(12)};
	auto result = con.PrepareAndExecute(std::move(statements[0]), values, true);
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

static void CheckCatalogErrorQuery(Connection &con) {
	duckdb::vector<Value> values = {Value(12)};
	auto result = con.PrepareAndExecute("SELECT COUNT(*) FROM b WHERE i=?", values, true);
	D_ASSERT(result->HasError() && result->GetErrorType() == ExceptionType::CATALOG);
}

static void CheckConversionErrorQuery(Connection &con) {
	// Check query with invalid prepared value
	duckdb::vector<Value> values = {Value("fawakaaniffoo")};
	auto result = con.PrepareAndExecute("SELECT COUNT(*) FROM a WHERE i=?", values, true);
	D_ASSERT(result->HasError() && result->GetErrorType() == ExceptionType::CONVERSION);
}

static void CheckSimpleQueryPrepareExecuteAfterModification(Connection &con) {
	duckdb::vector<Value> values = {Value(14)};
	auto result = con.PrepareAndExecute("SELECT COUNT(*) FROM a WHERE i=?", values, true);
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("PrepareExecute happy path", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	CreateSimpleTable(con);
	CheckSimpleQueryPrepareExecute(con);
	CheckSimpleQueryPrepareExecute(con);
}

TEST_CASE("PrepareExecute catalog error", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	CreateSimpleTable(con);

	CheckCatalogErrorQuery(con);

	// Verify things are still sane
	CheckSimpleQueryPrepareExecute(con);
}

TEST_CASE("PrepareExecute invalid value type error", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	CreateSimpleTable(con);

	CheckConversionErrorQuery(con);

	// Verify things are still sane
	CheckSimpleQueryPrepareExecute(con);
}

TEST_CASE("PrepareExecute with transactions", "[api]") {
	DuckDB db(nullptr);
	Connection con1(db);
	Connection con2(db);
	duckdb::vector<Value> empty_values = {};
	con1.EnableQueryVerification();

	CreateSimpleTable(con1);

	CheckConversionErrorQuery(con1);

	// Begin a transaction in the PrepareAndExecute
	auto result1 = con1.PrepareAndExecute("BEGIN TRANSACTION", empty_values, false);
	REQUIRE(!result1->HasError());
	CheckSimpleQueryPrepareExecute(con1);

	// Modify table on other connection, leaving transaction open
	con2.BeginTransaction();
	ModifySimpleTable(con2);
	CheckSimpleQueryPrepareExecuteAfterModification(con2);

	// con1 sees nothing: both transactions are open
	CheckSimpleQueryPrepareExecute(con1);

	con2.Commit();

	// con1 still sees nothing: its transaction was started before con2's
	CheckSimpleQueryPrepareExecute(con1);

	// con 1 commits
	auto result2 = con1.PrepareAndExecute("COMMIT", empty_values, true);
	REQUIRE(!result2->HasError());

	// now con1 should see changes from con2
	CheckSimpleQueryPrepareExecuteAfterModification(con1);
	CheckSimpleQueryPrepareExecuteAfterModification(con2);
}
