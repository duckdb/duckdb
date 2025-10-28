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
#include <algorithm>

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
} // namespace

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
		for (idx_t i = 0; i < 100; i++) {
			Value val_a(static_cast<int32_t>(i));
			Value val_b(static_cast<int32_t>(i * 2));
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
		for (idx_t i = 0; i < 100; i++) {
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

// Test nested leaf with restart operations: create multiple row IDs under same key, delete some, verify others exist
TEST_CASE("Test ART index with WAL replay - nested leaf row ID lookups", "[wal][art-wal-replay]") {
	duckdb::unique_ptr<QueryResult> result;
	auto db_path = TestCreatePath("art_wal_nested_test.db");
	DeleteDatabase(db_path);
	const int INSERT_COUNT = 50;
	const int DELETE_COUNT = 10;

	{
		DuckDB db(nullptr);
		Connection con(db);

		REQUIRE_NO_FAIL(con.Query("ATTACH '" + db_path + "' AS testdb"));
		REQUIRE_NO_FAIL(con.Query("USE testdb"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA wal_autocheckpoint='1TB'"));

		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(a INT)"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX idx_a ON tbl(a)"));

		// Insert some extra values to trigger index binding later on.
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (1), (2), (3), (100), (200)"));

		// This creates a nested leaf for the value 42.
		for (idx_t i = 0; i < INSERT_COUNT; i++) {
			REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (42)"));
		}

		// The rowid's for 42 should start from 5.
		// Delete every 5th rowid for 42. (5, 10, ..., 45)
		for (idx_t i = 0; i < DELETE_COUNT; i++) {
			row_t rowid_to_delete = 5 + (i * 5);
			REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE a = 42 AND rowid = " + to_string(rowid_to_delete)));
		}

		result = con.Query("SELECT COUNT(*) FROM tbl WHERE a = 42");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(static_cast<int64_t>(INSERT_COUNT - DELETE_COUNT))}));

		REQUIRE_NO_FAIL(con.Query("USE memory"));
		REQUIRE_NO_FAIL(con.Query("DETACH testdb"));
	}

	{
		DuckDB db(nullptr);
		Connection con(db);

		REQUIRE_NO_FAIL(con.Query("ATTACH '" + db_path + "' AS testdb"));
		REQUIRE_NO_FAIL(con.Query("USE testdb"));

		result = con.Query("SELECT * FROM tbl WHERE a = 42 LIMIT 1");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));

		result = con.Query("SELECT COUNT(*) FROM tbl WHERE a = 42");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(static_cast<int64_t>(INSERT_COUNT - DELETE_COUNT))}));

		auto &idx_a = GetARTIndex(con, "tbl", "idx_a");
		ArenaAllocator arena(BufferAllocator::Get(idx_a.db));

		Value val_42(42);
		auto key = ARTKey::CreateARTKey<int32_t>(arena, val_42);
		auto leaf = ARTOperator::Lookup(idx_a, idx_a.tree, key, 0);
		REQUIRE(leaf);

		result = con.Query("SELECT rowid FROM tbl WHERE a = 42 ORDER BY rowid");
		std::vector<row_t> remaining_rowids;
		while (auto chunk = result->Fetch()) {
			for (idx_t i = 0; i < chunk->size(); i++) {
				auto rowid_val = chunk->GetValue(0, i).GetValue<int64_t>();
				remaining_rowids.push_back(static_cast<row_t>(rowid_val));
			}
		}

		for (idx_t i = 0; i < DELETE_COUNT; i++) {
			row_t deleted_rowid = static_cast<row_t>(5 + (i * 5));
			auto rowid_key = ARTKey::CreateARTKey<row_t>(arena, deleted_rowid);
			bool found = ARTOperator::LookupInLeaf(idx_a, *leaf, rowid_key);
			REQUIRE(!found);
		}

		for (auto rowid : remaining_rowids) {
			auto check_key = ARTKey::CreateARTKey<row_t>(arena, rowid);
			bool exists = ARTOperator::LookupInLeaf(idx_a, *leaf, check_key);
			REQUIRE(exists);
		}
	}
	DeleteDatabase(db_path);
}

// Similar to the first test, but do a tighter interleaving between inserts and deletes.
TEST_CASE("Test ART index with WAL replay - interleaved inserts and deletes with generated columns",
          "[wal][art-wal-replay]") {
	duckdb::unique_ptr<QueryResult> result;
	auto db_path = TestCreatePath("art_wal_interleaved_test.db");
	DeleteDatabase(db_path);
	std::vector<int> values;
	std::vector<int> deleted_values;

	{
		constexpr int OPERATION_COUNT = 100;
		DuckDB db(nullptr);
		Connection con(db);

		REQUIRE_NO_FAIL(con.Query("ATTACH '" + db_path + "' AS testdb"));
		REQUIRE_NO_FAIL(con.Query("USE testdb"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA wal_autocheckpoint='1TB'"));

		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(a INT, b INT, c AS (2*a), d VARCHAR, e AS (b + 2), f VARCHAR)"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX idx_ab ON tbl(a, b)"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX idx_df ON tbl(d, f)"));

		for (idx_t i = 0; i < OPERATION_COUNT; i++) {
			idx_t val_a = (i + 1) * 10;
			idx_t val_b = val_a * 2;
			string val_d = "val_" + to_string(val_a);
			string val_f = "tag_" + to_string(val_a);

			if (i % 3 == 0) {
				// Insert and delete (0, 3, 6, 9, ...)
				REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl(a, b, d, f) VALUES (" + to_string(val_a) + ", " +
				                          to_string(val_b) + ", '" + val_d + "', '" + val_f + "')"));
				REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE a = " + to_string(val_a)));
				deleted_values.push_back(static_cast<int>(val_a));
			} else {
				// Insert and keep (pattern: 1, 2, 4, 5, 7, 8, ...)
				REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl(a, b, d, f) VALUES (" + to_string(val_a) + ", " +
				                          to_string(val_b) + ", '" + val_d + "', '" + val_f + "')"));
				values.push_back(static_cast<int>(val_a));
			}
		}

		result = con.Query("SELECT COUNT(*) FROM tbl");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(static_cast<int64_t>(kept_values.size()))}));

		REQUIRE_NO_FAIL(con.Query("USE memory"));
		REQUIRE_NO_FAIL(con.Query("DETACH testdb"));
	}

	{
		// Reattach and verify interleaved operations were correctly replayed
		DuckDB db(nullptr);
		Connection con(db);

		REQUIRE_NO_FAIL(con.Query("ATTACH '" + db_path + "' AS testdb"));
		REQUIRE_NO_FAIL(con.Query("USE testdb"));

		if (!values.empty()) {
			result = con.Query("SELECT * FROM tbl WHERE a = " + to_string(values[0]));
			REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(values[0])}));
		}

		result = con.Query("SELECT COUNT(*) FROM tbl");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(static_cast<int64_t>(values.size()))}));

		auto &idx_ab = GetARTIndex(con, "tbl", "idx_ab");
		auto &idx_df = GetARTIndex(con, "tbl", "idx_df");

		ArenaAllocator arena_ab(BufferAllocator::Get(idx_ab.db));
		ArenaAllocator arena_df(BufferAllocator::Get(idx_df.db));

		// Verify idx_ab
		for (auto val_a : values) {
			Value val_a_obj(static_cast<int32_t>(val_a));
			Value val_b_obj(static_cast<int32_t>(val_a * 2));
			auto key = ARTKey::CreateARTKey<int32_t>(arena_ab, val_a_obj);
			auto key_b = ARTKey::CreateARTKey<int32_t>(arena_ab, val_b_obj);
			key.Concat(arena_ab, key_b);
			auto leaf = ARTOperator::Lookup(idx_ab, idx_ab.tree, key, 0);
			REQUIRE(leaf);
		}

		for (auto val_a : deleted_values) {
			Value val_a_obj(static_cast<int32_t>(val_a));
			Value val_b_obj(static_cast<int32_t>(val_a * 2));
			auto key = ARTKey::CreateARTKey<int32_t>(arena_ab, val_a_obj);
			auto key_b = ARTKey::CreateARTKey<int32_t>(arena_ab, val_b_obj);
			key.Concat(arena_ab, key_b);
			auto leaf = ARTOperator::Lookup(idx_ab, idx_ab.tree, key, 0);
			REQUIRE(!leaf);
		}

		// Verify idx_df
		for (auto val_a : values) {
			string str_d = "val_" + to_string(val_a);
			string str_f = "tag_" + to_string(val_a);
			string_t str_t_d(str_d.c_str(), str_d.length());
			string_t str_t_f(str_f.c_str(), str_f.length());

			auto key_d = ARTKey::CreateARTKey<string_t>(arena_df, str_t_d);
			auto key_f = ARTKey::CreateARTKey<string_t>(arena_df, str_t_f);
			key_d.Concat(arena_df, key_f);
			auto leaf = ARTOperator::Lookup(idx_df, idx_df.tree, key_d, 0);
			REQUIRE(leaf);
		}

		for (auto val_a : deleted_values) {
			string str_d = "val_" + to_string(val_a);
			string str_f = "tag_" + to_string(val_a);
			string_t str_t_d(str_d.c_str(), str_d.length());
			string_t str_t_f(str_f.c_str(), str_f.length());

			auto key_d = ARTKey::CreateARTKey<string_t>(arena_df, str_t_d);
			auto key_f = ARTKey::CreateARTKey<string_t>(arena_df, str_t_f);
			key_d.Concat(arena_df, key_f);
			auto leaf = ARTOperator::Lookup(idx_df, idx_df.tree, key_d, 0);
			REQUIRE(!leaf);
		}
	}
	DeleteDatabase(db_path);
}
