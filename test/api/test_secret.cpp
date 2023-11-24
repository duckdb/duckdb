#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

TEST_CASE("Test secrets in transactions", "[api]") {
	DuckDB db(nullptr);
	Connection con1(db), con2(db), con3(db);

	if (!db.ExtensionIsLoaded("httpfs")) {
		return;
	}

	// Running single threaded
	REQUIRE_NO_FAIL(con1.Query("PRAGMA threads=1"));

	// Disable permanent secrets to prevent accidentally loading them from ~/.duckdb
	REQUIRE_NO_FAIL(con1.Query("SET allow_permanent_secrets=0;"));

	// Start transaction 1
	REQUIRE_NO_FAIL(con1.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con1.Query("CREATE SECRET s1 (TYPE S3)"));

	// Start transaction 2
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("CREATE SECRET s2 (TYPE S3)"));

	// Ensure both transactions are consistent
 	auto res1 = con1.Query("SELECT name FROM duckdb_secrets();");
 	auto res2 = con2.Query("SELECT name FROM duckdb_secrets();");
	REQUIRE(res1->RowCount() == 1);
	REQUIRE(res2->RowCount() == 1);
	REQUIRE(res1->GetValue(0,0).ToString() == "s1");
	REQUIRE(res2->GetValue(0,0).ToString() == "s2");

	// Commit transaction 1
	REQUIRE_NO_FAIL(con1.Query("COMMIT"));

	// Transaction 2 still only sees own secret
	res2 = con2.Query("SELECT name FROM duckdb_secrets();");
	REQUIRE(res2->RowCount() == 1);
	REQUIRE(res2->GetValue(0,0).ToString() == "s2");

	// New transaction will see only committed secret
	auto res3 = con3.Query("SELECT name FROM duckdb_secrets();");
	REQUIRE(res1->RowCount() == 1);
	REQUIRE(res1->GetValue(0,0).ToString() == "s1");

	// Commit transaction 2
	REQUIRE_NO_FAIL(con2.Query("COMMIT"));

	// Now both are visible
	res3 = con3.Query("SELECT name FROM duckdb_secrets() ORDER BY name;");
	REQUIRE(res3->RowCount() == 2);
	REQUIRE(res3->GetValue(0,0).ToString() == "s1");
	REQUIRE(res3->GetValue(0,1).ToString() == "s2");

	// Test dropping secret
	REQUIRE_NO_FAIL(con1.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con1.Query("DROP SECRET s1"));

	// Still visible to new transaction
	res3 = con3.Query("SELECT name FROM duckdb_secrets() ORDER BY name;");
	REQUIRE(res3->RowCount() == 2);
	REQUIRE(res3->GetValue(0,0).ToString() == "s1");
	REQUIRE(res3->GetValue(0,1).ToString() == "s2");

	// Commit DROP
	REQUIRE_NO_FAIL(con1.Query("COMMIT"));

	// Now only 1 secret remains
	res3 = con3.Query("SELECT name FROM duckdb_secrets();");
	REQUIRE(res3->RowCount() == 1);
	REQUIRE(res3->GetValue(0,0).ToString() == "s2");
}
