#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

namespace {

transaction_t ReadLastCommit(Connection &con, const string &table_name) {
	con.BeginTransaction();
	// QualifiedName overload, not the (catalog, schema, name) one: that form is
	// deprecated on main in favour of folding the qualification into a
	// QualifiedName. The deprecated form also compiles here, but a new test
	// should not add a deprecation warning to the build.
	auto &table_entry = Catalog::GetEntry<TableCatalogEntry>(
	    *con.context, QualifiedName(INVALID_CATALOG, DEFAULT_SCHEMA, table_name));
	auto &transaction_manager = DuckTransactionManager::Get(table_entry.GetStorage().GetAttached());
	auto last_commit = transaction_manager.GetLastCommit();
	con.Commit();
	return last_commit;
}

} // namespace

TEST_CASE("Test GetLastCommit is not advanced by read-only transactions", "[api][transaction]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	auto before = ReadLastCommit(con, "integers");

	for (idx_t i = 0; i < 3; i++) {
		REQUIRE_NO_FAIL(con.Query("SELECT COUNT(*) FROM integers"));
	}

	auto after = ReadLastCommit(con, "integers");
	REQUIRE(after == before);
}

TEST_CASE("Test GetLastCommit is advanced by every kind of modification", "[api][transaction]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));

	// Append (transaction-local storage).
	auto before_append = ReadLastCommit(con, "integers");
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));
	auto after_append = ReadLastCommit(con, "integers");
	REQUIRE(after_append > before_append);

	// In-place update (undo buffer, UndoFlags::UPDATE_TUPLE). The row count is
	// unchanged and no rows move, so this is the case a row-count based signal
	// would miss.
	auto before_update = ReadLastCommit(con, "integers");
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i = i + 100 WHERE i = 2"));
	auto after_update = ReadLastCommit(con, "integers");
	REQUIRE(after_update > before_update);

	// Delete (undo buffer, UndoFlags::DELETE_TUPLE).
	auto before_delete = ReadLastCommit(con, "integers");
	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i = 1"));
	auto after_delete = ReadLastCommit(con, "integers");
	REQUIRE(after_delete > before_delete);

	// Catalog change.
	auto before_ddl = ReadLastCommit(con, "integers");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers2(i INTEGER)"));
	auto after_ddl = ReadLastCommit(con, "integers");
	REQUIRE(after_ddl > before_ddl);
}

TEST_CASE("Test GetLastCommit is not advanced by a rolled back transaction", "[api][transaction]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1)"));

	auto before = ReadLastCommit(con, "integers");

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (2)"));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	auto after = ReadLastCommit(con, "integers");
	REQUIRE(after == before);

	auto result = con.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(1)}));
}
