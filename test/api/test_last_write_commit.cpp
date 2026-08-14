#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

namespace {

struct CommitCounters {
	transaction_t any;
	transaction_t write;
};

CommitCounters ReadCounters(Connection &con, const string &table_name) {
	con.BeginTransaction();
	auto &table_entry = Catalog::GetEntry<TableCatalogEntry>(*con.context, INVALID_CATALOG, DEFAULT_SCHEMA, table_name);
	auto &transaction_manager = DuckTransactionManager::Get(table_entry.GetStorage().GetAttached());
	CommitCounters counters {transaction_manager.GetLastCommit(), transaction_manager.GetLastWriteCommit()};
	con.Commit();
	return counters;
}

} // namespace

TEST_CASE("Test GetLastWriteCommit is not advanced by read-only transactions", "[api][transaction]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	auto before = ReadCounters(con, "integers");

	for (idx_t i = 0; i < 3; i++) {
		REQUIRE_NO_FAIL(con.Query("SELECT COUNT(*) FROM integers"));
	}

	auto after = ReadCounters(con, "integers");

	// Read-only statements are committed, so the general commit counter moves.
	// Asserting this keeps the check below meaningful.
	REQUIRE(after.any > before.any);
	// ... but nothing was modified, so the write commit counter must not move.
	REQUIRE(after.write == before.write);
}

TEST_CASE("Test GetLastWriteCommit is advanced by every kind of modification", "[api][transaction]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));

	// Append (transaction-local storage).
	auto before_append = ReadCounters(con, "integers");
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));
	auto after_append = ReadCounters(con, "integers");
	REQUIRE(after_append.write > before_append.write);

	// In-place update (undo buffer, UndoFlags::UPDATE_TUPLE). The row count is
	// unchanged and no rows move, so this is the case a row-count based signal
	// would miss.
	auto before_update = ReadCounters(con, "integers");
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i = i + 100 WHERE i = 2"));
	auto after_update = ReadCounters(con, "integers");
	REQUIRE(after_update.write > before_update.write);

	// Delete (undo buffer, UndoFlags::DELETE_TUPLE).
	auto before_delete = ReadCounters(con, "integers");
	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i = 1"));
	auto after_delete = ReadCounters(con, "integers");
	REQUIRE(after_delete.write > before_delete.write);

	// Catalog change.
	auto before_ddl = ReadCounters(con, "integers");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers2(i INTEGER)"));
	auto after_ddl = ReadCounters(con, "integers");
	REQUIRE(after_ddl.write > before_ddl.write);
}

TEST_CASE("Test GetLastWriteCommit is not advanced by a rolled back transaction", "[api][transaction]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1)"));

	auto before = ReadCounters(con, "integers");

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (2)"));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	auto after = ReadCounters(con, "integers");
	REQUIRE(after.write == before.write);

	auto result = con.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(1)}));
}
