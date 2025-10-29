#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "test_helpers.hpp"
#include <fstream>

using namespace duckdb;

static bool FileExists(const string &path) {
	std::ifstream file(path);
	return file.good();
}

static string ReadFile(const string &path) {
	std::ifstream file(path);
	if (!file.is_open()) {
		throw std::runtime_error("Could not open file: " + path);
	}
	std::stringstream buffer;
	buffer << file.rdbuf();
	return buffer.str();
}

static void WriteFile(const string &path, const string &contents) {
	std::ofstream file(path);
	file << contents;
}

static BoundIndex &GetIndexFromTable(Connection &con, const string &table_name, const string &index_name) {
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context, "");
	auto &table_entry = catalog.GetEntry<TableCatalogEntry>(context, "", "main", table_name);
	auto &duck_table = table_entry.Cast<DuckTableEntry>();
	auto &storage = duck_table.GetStorage();
	auto &indexes = storage.GetDataTableInfo()->GetIndexes();
	auto bound_index = indexes.Find(index_name);
	
	if (!bound_index) {
		throw std::runtime_error("Index not found: " + index_name);
	}
	return *bound_index;
}

static void CompareWithReferenceFile(const string &actual, const string &reference_file) {
	if (!FileExists(reference_file)) {
		WriteFile(reference_file, actual);
	} else {
		string expected = ReadFile(reference_file);
		REQUIRE(actual == expected);
	}
}

TEST_CASE("Test BoundIndex Verify and ToString with duplicate keys", "[index][index-verify-string]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE dup_table(id INTEGER, value VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX dup_index ON dup_table(id)"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO dup_table VALUES (1, 'a'), (1, 'b'), (1, 'c'), (1, 'd')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO dup_table VALUES (2, 'w'), (2, 'x'), (2, 'y'), (2, 'z')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO dup_table VALUES (3, 'p'), (3, 'q'), (3, 'r')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO dup_table VALUES (5, 'm'), (5, 'n')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO dup_table VALUES (7, 'e'), (7, 'f'), (7, 'g')"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	
	auto &bound_index = GetIndexFromTable(con, "dup_table", "dup_index");
	bound_index.Verify();

	string index_string = bound_index.ToString();
	CompareWithReferenceFile(index_string, "test/sql/index/art/pretty_printer/expected_duplicate_keys.txt");
	
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
}

TEST_CASE("Test BoundIndex Verify and ToString with deep tree (ASCII mode)", "[index][index-verify-string]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE deep_table(name VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX deep_index ON deep_table(name)"));

	REQUIRE_NO_FAIL(con.Query(R"(
		INSERT INTO deep_table VALUES 
		('apple'), ('application'), ('applicator'), ('apply'), ('apricot'),
		('banana'), ('band'), ('bandana'), ('banner'), ('banquet'),
		('car'), ('card'), ('cardinal'), ('cargo'), ('carpet'),
		('dog'), ('door'), ('doorbell'), ('doorway'), ('dot')
	)"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	
	auto &bound_index = GetIndexFromTable(con, "deep_table", "deep_index");
	bound_index.Verify();

	string index_string = bound_index.ToString(true);
	CompareWithReferenceFile(index_string, "test/sql/index/art/pretty_printer/expected_deep_tree_ascii.txt");
	
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
}

TEST_CASE("Test BoundIndex Verify and ToString with VARCHAR duplicates (ASCII + Gate)", "[index][index-verify-string]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE varchar_dup_table(sentence VARCHAR, id INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX varchar_dup_index ON varchar_dup_table(sentence)"));

	REQUIRE_NO_FAIL(con.Query(R"(
		INSERT INTO varchar_dup_table VALUES 
		('The quick brown fox jumps over the lazy dog', 1),
		('The quick brown fox jumps over the lazy dog', 2),
		('The quick brown fox jumps over the lazy dog', 3),
		('The quick brown fox jumps over the lazy cat', 4),
		('The quick brown fox jumps over the lazy cat', 5),
		('The quick brown fox jumps over the fence', 6),
		('The quick brown fox jumps over the fence', 7),
		('The quick brown fox jumps over the moon', 8),
		('The quick brown fox runs away quickly', 9),
		('The quick brown fox runs away quickly', 10),
		('The quick brown fox', 11),
		('The quick brown fox', 12),
		('The quick brown fox', 13),
		('The quick brown squirrel climbs the tree', 14),
		('The quick brown squirrel climbs the tree', 15),
		('The slow red turtle walks carefully through the garden', 16),
		('The slow red turtle walks carefully through the garden', 17),
		('The slow red turtle walks carefully through the park', 18),
		('The slow red turtle walks slowly forward', 19),
		('The slow red turtle', 20),
		('A completely different sentence starts here today', 21),
		('A completely different sentence starts here today', 22),
		('A completely different sentence starts here tomorrow', 23),
		('A completely different sentence ends there', 24),
		('Beautiful mountains tower majestically over the valley below', 25),
		('Beautiful mountains tower majestically over the valley below', 26),
		('Beautiful mountains tower majestically over the valley below', 27),
		('Beautiful mountains tower above everything else', 28),
		('Beautiful mountains', 29),
		('Zebras and giraffes roam freely across the African savanna', 30),
		('Zebras and giraffes roam freely across the African savanna', 31),
		('Zebras and giraffes roam freely across the plains together', 32),
		('Zebras and giraffes roam the countryside', 33),
		('Zebras run incredibly fast through the grasslands', 34),
		('Zebras run incredibly fast through the grasslands', 35),
		('Zebras run incredibly fast', 36),
		('Zebras run incredibly fast', 37),
		('Zebras run incredibly fast', 38)
	)"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	
	auto &bound_index = GetIndexFromTable(con, "varchar_dup_table", "varchar_dup_index");
	bound_index.Verify();

	string index_string = bound_index.ToString(true);
	CompareWithReferenceFile(index_string, "test/sql/index/art/pretty_printer/expected_varchar_duplicates_ascii.txt");
	
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
}
