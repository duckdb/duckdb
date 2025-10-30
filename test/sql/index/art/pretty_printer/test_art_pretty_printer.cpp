#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "test_helpers.hpp"
#include <fstream>

using namespace duckdb;

namespace {

bool FileExists(const string &path) {
	std::ifstream file(path);
	return file.good();
}

string ReadFile(const string &path) {
	std::ifstream file(path);
	if (!file.is_open()) {
		throw std::runtime_error("Could not open file: " + path);
	}
	std::stringstream buffer;
	buffer << file.rdbuf();
	return buffer.str();
}

void WriteFile(const string &path, const string &contents) {
	std::ofstream file(path);
	file << contents;
}

ART &GetARTIndex(Connection &con, const string &table_name, const string &index_name) {
	optional_ptr<ART> art_ptr;
	con.context->RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*con.context, "");
		auto &table_entry = catalog.GetEntry<TableCatalogEntry>(*con.context, "", "main", table_name);
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

void CompareWithReferenceFile(const string &actual, const string &reference_file) {
	if (!FileExists(reference_file)) {
		WriteFile(reference_file, actual);
	} else {
		string expected = ReadFile(reference_file);
		REQUIRE(actual == expected);
	}
}
} // namespace

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

	auto &art = GetARTIndex(con, "dup_table", "dup_index");
	BoundIndex &bound_index = art;

	bound_index.Verify();
	string index_string = bound_index.ToString();
	CompareWithReferenceFile(index_string, "test/sql/index/art/pretty_printer/expected_duplicate_keys.txt");
}

TEST_CASE("Test BoundIndex Verify and ToString (ASCII mode)", "[index][index-verify-string]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE prefix_table(name VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX prefix_index ON prefix_table(name)"));

	REQUIRE_NO_FAIL(con.Query(R"(
		INSERT INTO prefix_table VALUES 
		('apple'), ('application'), ('applicator'), ('apply'), ('apricot'),
		('banana'), ('band'), ('bandana'), ('banner'), ('banquet'),
		('car'), ('card'), ('cardinal'), ('cargo'), ('carpet'),
		('dog'), ('door'), ('doorbell'), ('doorway'), ('dot')
	)"));

	auto &art = GetARTIndex(con, "prefix_table", "prefix_index");
	BoundIndex &bound_index = art;

	bound_index.Verify();
	string index_string = bound_index.ToString(true);
	CompareWithReferenceFile(index_string, "test/sql/index/art/pretty_printer/expected_prefix_compression_ascii.txt");
}

TEST_CASE("Test BoundIndex Verify and ToString with VARCHAR duplicates (ASCII + Gate)",
          "[index][index-verify-string]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE varchar_dup_table(sentence VARCHAR, id INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX varchar_dup_index ON varchar_dup_table(sentence)"));

	REQUIRE_NO_FAIL(con.Query(R"(
		INSERT INTO varchar_dup_table VALUES 
		('aaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbccccccccccccccccccccccccdddddddddddddddddddddddd', 1),
		('aaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbccccccccccccccccccccccccdddddddddddddddddddddddd', 2),
		('aaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbccccccccccccccccccccccccdddddddddddddddddddddddd', 3),
		('aaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccceeeeeeeeeeeeeeeeeeeeeeee', 4),
		('aaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccceeeeeeeeeeeeeeeeeeeeeeee', 5),
		('aaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbccccccccccccccccccccccccffffffffffffffffffff', 6),
		('aaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccc', 7),
		('aaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccc', 8),
		('aaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccc', 9),
		('aaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbb', 10),
		('aaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbb', 11),
		('aaaaaaaaaaaaaaaaaaaaaaaagggggggggggggggggggggggghhhhhhhhhhhhhhhhhhhhhhhiiiiiiiiiiiiiiiiiiii', 12),
		('aaaaaaaaaaaaaaaaaaaaaaaagggggggggggggggggggggggghhhhhhhhhhhhhhhhhhhhhhhiiiiiiiiiiiiiiiiiiii', 13),
		('aaaaaaaaaaaaaaaaaaaaaaaagggggggggggggggggggggggghhhhhhhhhhhhhhhhhhhhhhhiiiiiiiiiiiiiiiiiiii', 14),
		('aaaaaaaaaaaaaaaaaaaaaaaagggggggggggggggggggggggghhhhhhhhhhhhhhhhhhhhhhhiiiiiiiiiiiiiiiiiiii', 15),
		('aaaaaaaaaaaaaaaaaaaaaaaagggggggggggggggggggggggghhhhhhhhhhhhhhhhhhhhhhhiiiiiiiiiiiiiiiiiiii', 16),
		('aaaaaaaaaaaaaaaaaaaaaaaagggggggggggggggggggggggghhhhhhhhhhhhhhhhhhhhhhhiiiiiiiiiiiiiiiiijjjj', 17),
		('aaaaaaaaaaaaaaaaaaaaaaaagggggggggggggggggggggggghhhhhhhhhhhhhhhhhhhhhhhiiiiiiiiiiiiiiiiijjjj', 18),
		('aaaaaaaaaaaaaaaaaaaaaaaagggggggggggggggggggggggghhhhhhhhhhhhhhhhhhhhhhhkkkkkkkkkkkkkkkkkkkk', 19),
		('aaaaaaaaaaaaaaaaaaaaaaaagggggggggggggggggggggggghhhhhhhhhhhhhhhhhhhhhhhkkkkkkkkkkkkkkkkkkkk', 20),
		('ppppppppppppppppppppppppqqqqqqqqqqqqqqqqqqqqqqqqrrrrrrrrrrrrrrrrrrrrrrrrssssssssssssssssssssssss', 21),
		('ppppppppppppppppppppppppqqqqqqqqqqqqqqqqqqqqqqqqrrrrrrrrrrrrrrrrrrrrrrrrssssssssssssssssssssssss', 22),
		('ppppppppppppppppppppppppqqqqqqqqqqqqqqqqqqqqqqqqrrrrrrrrrrrrrrrrrrrrrrrrssssssssssssssssssssssss', 23),
		('ppppppppppppppppppppppppqqqqqqqqqqqqqqqqqqqqqqqqrrrrrrrrrrrrrrrrrrrrrrrrssssssssssssssssssssssss', 24),
		('ppppppppppppppppppppppppqqqqqqqqqqqqqqqqqqqqqqqqrrrrrrrrrrrrrrrrrrrrrrrrtttttttttttttttttttttttt', 25),
		('ppppppppppppppppppppppppqqqqqqqqqqqqqqqqqqqqqqqqrrrrrrrrrrrrrrrrrrrrrrrrtttttttttttttttttttttttt', 26),
		('ppppppppppppppppppppppppqqqqqqqqqqqqqqqqqqqqqqqqrrrrrrrrrrrrrrrrrrrrrrrr', 27),
		('ppppppppppppppppppppppppqqqqqqqqqqqqqqqqqqqqqqqqrrrrrrrrrrrrrrrrrrrrrrrr', 28),
		('ppppppppppppppppppppppppqqqqqqqqqqqqqqqqqqqqqqqqrrrrrrrrrrrrrrrrrrrrrrrr', 29),
		('xxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyyzzzzzzzzzzzzzzzzzzzzzzzz', 30),
		('xxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyyzzzzzzzzzzzzzzzzzzzzzzzz', 31),
		('xxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyyzzzzzzzzzzzzzzzzzzzzzzzz', 32),
		('xxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyyzzzzzzzzzzzzzzzzzzzzzzzz', 33),
		('xxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyyzzzzzzzzzzzzzzzzzzzzzzzz', 34),
		('xxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyyzzzzzzzzzzzzzzzzzzzzzzzzAAAAAAAAAAAAAAAAAAAAAAAA', 35),
		('xxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyyzzzzzzzzzzzzzzzzzzzzzzzzAAAAAAAAAAAAAAAAAAAAAAAA', 36),
		('xxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyyzzzzzzzzzzzzzzzzzzzzzzzzAAAAAAAAAAAAAAAAAAAAAAAA', 37),
		('xxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyyzzzzzzzzzzzzzzzzzzzzzzzzBBBBBBBBBBBBBBBBBBBBBBBB', 38),
		('xxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyyzzzzzzzzzzzzzzzzzzzzzzzzBBBBBBBBBBBBBBBBBBBBBBBB', 39),
		('xxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyyzzzzzzzzzzzzzzzzzzzzzzzzBBBBBBBBBBBBBBBBBBBBBBBB', 40)
	)"));

	auto &art = GetARTIndex(con, "varchar_dup_table", "varchar_dup_index");
	BoundIndex &bound_index = art;

	bound_index.Verify();
	string index_string = bound_index.ToString(true);
	CompareWithReferenceFile(index_string, "test/sql/index/art/pretty_printer/expected_varchar_duplicates_ascii.txt");
}

TEST_CASE("Test BoundIndex Verify and ToString with Node16 and Node4 structure", "[index][index-verify-string]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE node_structure_table(text VARCHAR, id INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX node_structure_index ON node_structure_table(text)"));

	REQUIRE_NO_FAIL(con.Query(R"(
		INSERT INTO node_structure_table VALUES 
		('Aaa', 1), ('Aaa', 2), ('Aaa', 3),
		('Abb', 4), ('Abb', 5),
		('Acc', 6), ('Acc', 7), ('Acc', 8),
		('Add', 9),
		('Bxx', 10), ('Bxx', 11), ('Bxx', 12), ('Bxx', 13),
		('Byy', 14), ('Byy', 15),
		('Bzz', 16),
		('Cpp', 17), ('Cpp', 18),
		('Cqq', 19), ('Cqq', 20), ('Cqq', 21),
		('Crr', 22),
		('Dmmm', 23), ('Dmmm', 24), ('Dmmm', 25),
		('Dnnn', 26), ('Dnnn', 27),
		('Dooo', 28),
		('Eppp', 29),
		('Efff', 30), ('Efff', 31),
		('Eggg', 32), ('Eggg', 33), ('Eggg', 34)
	)"));

	auto &art = GetARTIndex(con, "node_structure_table", "node_structure_index");
	BoundIndex &bound_index = art;

	bound_index.Verify();
	string index_string = bound_index.ToString(true);
	CompareWithReferenceFile(index_string, "test/sql/index/art/pretty_printer/expected_node16_node4_ascii.txt");
}
