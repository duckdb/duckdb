#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/table_index_list.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_operator.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/iterator.hpp"
#include "duckdb/common/optional_ptr.hpp"

using namespace duckdb;

namespace {
	ART &GetARTIndex(Connection &con, const string &table_name, const string &index_name) {
		optional_ptr<ART> art_ptr;
		con.context->RunFunctionInTransaction([&]() {
			auto &catalog = Catalog::GetCatalog(*con.context, "testdb");
			auto &table_entry = catalog.GetEntry<TableCatalogEntry>(*con.context, "main", table_name);
			auto &duck_table = table_entry.Cast<DuckTableEntry>();
			auto &storage = duck_table.GetStorage();
			auto &data_table_info = storage.GetDataTableInfo();
			auto &indexes = data_table_info->GetIndexes();

			indexes.Scan([&](Index &index) {
				if (index.GetIndexName() == index_name) {
					REQUIRE(index.IsBound());
					art_ptr = &index.Cast<ART>();
					return true;
				}
				return false;
			});
		});
		REQUIRE(art_ptr);
		return *art_ptr;
	}
}

// This test inspects the ART tree to check that index WAL operations are properly replayed after
// restarting the database. This was not being explicitly triggered in any CI or SQL tests, so it is
// necessary to have a C++ test that explicitly traverses the ART tree to verify it.

// This test includes both physical and generated columns to test that the buffering and column_id mappings
// work -- it includes a "regular case" with an index on the first two physical columns, and an index on
// physical columns that are interleaved between  generated columns to test that the buffered mappings work as
// intended.
TEST_CASE("Test ART index with WAL replay - generated columns and interleaved inserts/deletes",
          "[wal][art-wal-replay]") {
	duckdb::unique_ptr<QueryResult> result;
	auto db_path = TestCreatePath("art_wal_gen_test.db");
	DeleteDatabase(db_path);
	{
		DuckDB db(nullptr);
		Connection con(db);

		REQUIRE_NO_FAIL(con.Query("ATTACH '" + db_path + "' AS testdb"));
		REQUIRE_NO_FAIL(con.Query("USE testdb"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA wal_autocheckpoint='1TB'"));

		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(a INT, b INT, c AS (2*a), d VARCHAR, e AS (b + 2), f VARCHAR)"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX idx_ab ON tbl(a, b)"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX idx_df ON tbl(d, f)"));

		REQUIRE_NO_FAIL(
		    con.Query("INSERT INTO tbl SELECT range, range * 2, 'val_' || range, 'tag_' || range FROM range(100)"));

		REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE a % 5 = 0"));

		result = con.Query("SELECT COUNT(*) FROM tbl");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(80)}));

		REQUIRE_NO_FAIL(con.Query("USE memory"));
		REQUIRE_NO_FAIL(con.Query("DETACH testdb"));
	}

	{
		// Reattach database and verify that WAL index replays work.
		DuckDB db(nullptr);
		Connection con(db);

		REQUIRE_NO_FAIL(con.Query("ATTACH '" + db_path + "' AS testdb"));
		REQUIRE_NO_FAIL(con.Query("USE testdb"));

		result = con.Query("SELECT * FROM tbl WHERE a = 1");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));

		result = con.Query("SELECT COUNT(*) FROM tbl");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(80)}));

		auto &idx_ab = GetARTIndex(con, "tbl", "idx_ab");
		auto &idx_df = GetARTIndex(con, "tbl", "idx_df");

		ArenaAllocator arena_ab(BufferAllocator::Get(idx_ab.db));
		ArenaAllocator arena_df(BufferAllocator::Get(idx_df.db));

		// Verify idx_ab: all mod 5 deleted, everything else exists
		for (int i = 0; i < 100; i++) {
			Value val_a(i);
			Value val_b(i * 2);
			auto key = ARTKey::CreateARTKey<int32_t>(arena_ab, val_a);
			auto key_b = ARTKey::CreateARTKey<int32_t>(arena_ab, val_b);
			key.Concat(arena_ab, key_b);
			auto leaf = ARTOperator::Lookup(idx_ab, idx_ab.tree, key, 0);

			if (i % 5 == 0) {
				REQUIRE(!leaf);
			} else {
				REQUIRE(leaf);
			}
		}

		// Verify idx_df: all mod5 are deleted, everything else exists.
		for (int i = 0; i < 100; i++) {
			string str_d = "val_" + to_string(i);
			string str_f = "tag_" + to_string(i);
			string_t str_t_d(str_d.c_str(), str_d.length());
			string_t str_t_f(str_f.c_str(), str_f.length());

			auto key_d = ARTKey::CreateARTKey<string_t>(arena_df, str_t_d);
			auto key_f = ARTKey::CreateARTKey<string_t>(arena_df, str_t_f);
			key_d.Concat(arena_df, key_f);
			auto leaf = ARTOperator::Lookup(idx_df, idx_df.tree, key_d, 0);

			if (i % 5 == 0) {
				REQUIRE(!leaf);
			} else {
				REQUIRE(leaf);
			}
		}
	}
	DeleteDatabase(db_path);
}
