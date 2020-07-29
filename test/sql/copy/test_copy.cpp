#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/date.hpp"
#include "test_csv_header.hpp"
#include "test_helpers.hpp"

#include <fstream>

using namespace duckdb;
using namespace std;

TEST_CASE("Test copy statement with unicode delimiter/quote/escape", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	con.AddComment("generate CSV file with unicode (> one-byte) delimiter/quote/escape");
	ofstream from_csv_file1(fs.JoinPath(csv_path, "multi_char.csv"));
	from_csv_file1 << 0 << "🦆ˮdu˧˧🦆ckˮ🦆ˮd˧ˮ˧ˮu🦆ckˮ🦆duck" << endl;
	from_csv_file1 << 1 << "🦆ˮdou˧ˮbleˮ🦆🦆duck" << endl;
	from_csv_file1 << 2 << "🦆🦆🦆" << endl;
	from_csv_file1 << 3 << "🦆duck inv˧asion🦆🦆" << endl;
	from_csv_file1.close();

	con.AddComment("generate CSV file with unicode (> one-byte) delimiter/quote/escape that exceeds the buffer size a few times");
	ofstream from_csv_file2(fs.JoinPath(csv_path, "multi_char_buffer_exhausted.csv"));
	int64_t sum = 0;
	for (int i = 0; i < 16384; i++) {
		if (i % 2 == 0) {
			from_csv_file2 << i << "🦆ˮ🦆dˮ🦆ˮd˧ˮ🦆ˮ🦆d˧" << endl;
		} else {
			from_csv_file2 << i << "🦆ˮ˧ˮ˧ˮ˧ˮˮ🦆˧˧🦆	test test	🦆" << endl;
		}
		sum += i;
	}
	from_csv_file2.close();

	con.AddComment("generate CSV file with one-byte delimiter/quote/escape");
	ofstream from_csv_file3(fs.JoinPath(csv_path, "one_byte_char.csv"));
	for (int i = 0; i < 3; i++) {
		from_csv_file3 << i << ",'du''ck','''''du,ck',duck" << endl;
	}
	from_csv_file3.close();

	con.AddComment("generate CSV file with unterminated quotes");
	ofstream from_csv_file4(fs.JoinPath(csv_path, "unterminated_quotes.csv"));
	for (int i = 0; i < 3; i++) {
		from_csv_file4 << i << ",duck,\"duck" << endl;
	}
	from_csv_file4.close();

	con.AddComment("generate CSV file with quotes that start midway in the value");
	ofstream from_csv_file5(fs.JoinPath(csv_path, "unterminated_quotes_2.csv"));
	for (int i = 0; i < 3; i++) {
		from_csv_file5 << i << ",du\"ck,duck" << endl;
	}
	from_csv_file5.close();

	con.AddComment("generate a CSV file with a very long string exceeding the buffer midway in an escape sequence (delimiter and");
	con.AddComment("escape share substrings)");
	ofstream from_csv_file6(fs.JoinPath(csv_path, "shared_substrings.csv"));
	string big_string_a(16370, 'a');
	from_csv_file6 << big_string_a << "AAA\"aaaaaaaaAAB\"\"" << endl;
	from_csv_file6.close();

	con.AddComment("create three tables for testing");
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE test_unicode_1 (col_a INTEGER, col_b VARCHAR(10), col_c VARCHAR(10), col_d VARCHAR(10));"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE test_unicode_2 (col_a INTEGER, col_b VARCHAR(10), col_c VARCHAR(10), col_d VARCHAR(10));"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE test_unicode_3 (col_a INTEGER, col_b VARCHAR(10), col_c VARCHAR(10), col_d VARCHAR(10));"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test_unicode_4 (col_a VARCHAR, col_b VARCHAR);"));

	con.AddComment("throw error if unterminated quotes are detected");
	REQUIRE_FAIL(con.Query("COPY test_unicode_1 FROM '" + fs.JoinPath(csv_path, "unterminated_quotes.csv") + "';"));
	REQUIRE_FAIL(con.Query("COPY test_unicode_1 FROM '" + fs.JoinPath(csv_path, "unterminated_quotes_2.csv") + "';"));

	con.AddComment("test COPY ... FROM ...");

	con.AddComment("test unicode delimiter/quote/escape");
	result = con.Query("COPY test_unicode_1 FROM '" + fs.JoinPath(csv_path, "multi_char.csv") +
	                   "' (DELIMITER '🦆', QUOTE 'ˮ', ESCAPE '˧');");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	result = con.Query("SELECT * FROM test_unicode_1 ORDER BY 1 LIMIT 4;");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"du˧🦆ck", "douˮble", Value(), "duck inv˧asion"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"dˮˮu🦆ck", Value(), Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {"duck", "duck", Value(), Value()}));

	con.AddComment("test unicode delimiter/quote/escape that exceeds the buffer size a few times");
	result = con.Query("COPY test_unicode_2 FROM '" + fs.JoinPath(csv_path, "multi_char_buffer_exhausted.csv") +
	                   "' (DELIMITER '🦆', QUOTE 'ˮ', ESCAPE '˧');");
	REQUIRE(CHECK_COLUMN(result, 0, {16384}));
	result = con.Query("SELECT * FROM test_unicode_2 ORDER BY 1 LIMIT 4;");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"🦆d", "ˮˮˮ", "🦆d", "ˮˮˮ"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"dˮ🦆", "˧˧", "dˮ🦆", "˧˧"}));
	REQUIRE(CHECK_COLUMN(result, 3, {"d˧", "	test test	", "d˧", "	test test	"}));
	result = con.Query("SELECT SUM(col_a) FROM test_unicode_2;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum)}));

	con.AddComment("test one-byte delimiter/quote/escape");
	result = con.Query("COPY test_unicode_3 FROM '" + fs.JoinPath(csv_path, "one_byte_char.csv") + "' (QUOTE '''');");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	result = con.Query("SELECT * FROM test_unicode_3 ORDER BY 1 LIMIT 3;");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {"du'ck", "du'ck", "du'ck"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"''du,ck", "''du,ck", "''du,ck"}));
	REQUIRE(CHECK_COLUMN(result, 3, {"duck", "duck", "duck"}));

	con.AddComment("test correct shared substring behavior at buffer borders");
	result = con.Query("COPY test_unicode_4 FROM '" + fs.JoinPath(csv_path, "shared_substrings.csv") +
	                   "' (DELIMITER 'AAA', ESCAPE 'AAB');");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT * FROM test_unicode_4;");
	REQUIRE(CHECK_COLUMN(result, 0, {big_string_a}));
	REQUIRE(CHECK_COLUMN(result, 1, {"aaaaaaaa\""}));

	con.AddComment("quote and escape must not be empty");
	REQUIRE_FAIL(con.Query("COPY test_unicode_1 FROM '" + fs.JoinPath(csv_path, "one_byte_char.csv") +
	                       "' (DELIMITER '🦆', QUOTE '');"));
	REQUIRE_FAIL(con.Query("COPY test_unicode_1 FROM '" + fs.JoinPath(csv_path, "one_byte_char.csv") +
	                       "' (DELIMITER '🦆', ESCAPE '');"));

	con.AddComment("test same string for delimiter and quote");
	REQUIRE_FAIL(con.Query("COPY test_unicode_1 FROM '" + fs.JoinPath(csv_path, "one_byte_char.csv") +
	                       "' (DELIMITER '🦆', QUOTE '🦆');"));

	con.AddComment("escape and quote cannot be substrings of each other");
	REQUIRE_FAIL(con.Query("COPY test_unicode_1 FROM '" + fs.JoinPath(csv_path, "one_byte_char.csv") +
	                       "' (ESCAPE 'du', QUOTE 'duck');"));
	REQUIRE_FAIL(con.Query("COPY test_unicode_1 FROM '" + fs.JoinPath(csv_path, "one_byte_char.csv") +
	                       "' (ESCAPE 'duck', QUOTE 'du');"));

	con.AddComment("delimiter and quote cannot be substrings of each other");
	REQUIRE_FAIL(con.Query("COPY test_unicode_1 FROM '" + fs.JoinPath(csv_path, "one_byte_char.csv") +
	                       "' (DELIMITER 'du', QUOTE 'duck');"));
	REQUIRE_FAIL(con.Query("COPY test_unicode_1 FROM '" + fs.JoinPath(csv_path, "one_byte_char.csv") +
	                       "' (DELIMITER 'duck', QUOTE 'du');"));

	con.AddComment("delimiter and escape cannot be substrings of each other");
	REQUIRE_FAIL(con.Query("COPY test_unicode_1 FROM '" + fs.JoinPath(csv_path, "one_byte_char.csv") +
	                       "' (DELIMITER 'AA', ESCAPE 'AAAA');"));
	REQUIRE_FAIL(con.Query("COPY test_unicode_1 FROM '" + fs.JoinPath(csv_path, "one_byte_char.csv") +
	                       "' (DELIMITER 'AAAA', ESCAPE 'AA');"));

	con.AddComment("COPY ... TO ...");

	con.AddComment("test unicode delimiter/quote/escape");
	result = con.Query("COPY test_unicode_1 TO '" + fs.JoinPath(csv_path, "test_unicode_1.csv") +
	                   "' (DELIMITER '🦆', QUOTE 'ˮ', ESCAPE '˧');");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM test_unicode_1;"));
	result = con.Query("COPY test_unicode_1 FROM '" + fs.JoinPath(csv_path, "test_unicode_1.csv") +
	                   "' (DELIMITER '🦆', QUOTE 'ˮ', ESCAPE '˧');");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	result = con.Query("SELECT * FROM test_unicode_1 ORDER BY 1 LIMIT 4;");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"du˧🦆ck", "douˮble", Value(), "duck inv˧asion"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"dˮˮu🦆ck", Value(), Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {"duck", "duck", Value(), Value()}));

	con.AddComment("test unicode delimiter/quote/escape");
	result = con.Query("COPY test_unicode_2 TO '" + fs.JoinPath(csv_path, "test_unicode_2.csv") +
	                   "' (DELIMITER '🦆', QUOTE 'ˮ', ESCAPE '˧');");
	REQUIRE(CHECK_COLUMN(result, 0, {16384}));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM test_unicode_2;"));
	result = con.Query("COPY test_unicode_2 FROM '" + fs.JoinPath(csv_path, "test_unicode_2.csv") +
	                   "' (DELIMITER '🦆', QUOTE 'ˮ', ESCAPE '˧');");
	REQUIRE(CHECK_COLUMN(result, 0, {16384}));
	result = con.Query("SELECT * FROM test_unicode_2 ORDER BY 1 LIMIT 4;");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"🦆d", "ˮˮˮ", "🦆d", "ˮˮˮ"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"dˮ🦆", "˧˧", "dˮ🦆", "˧˧"}));
	REQUIRE(CHECK_COLUMN(result, 3, {"d˧", "	test test	", "d˧", "	test test	"}));
	result = con.Query("SELECT SUM(col_a) FROM test_unicode_2;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum)}));

	con.AddComment("test one-byte delimiter/quote/escape");
	result = con.Query("COPY test_unicode_3 TO '" + fs.JoinPath(csv_path, "test_unicode_3.csv") + "' (QUOTE '''');");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM test_unicode_3;"));
	result = con.Query("COPY test_unicode_3 FROM '" + fs.JoinPath(csv_path, "test_unicode_3.csv") + "' (QUOTE '''');");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	result = con.Query("SELECT * FROM test_unicode_3 ORDER BY 1 LIMIT 3;");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {"du'ck", "du'ck", "du'ck"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"''du,ck", "''du,ck", "''du,ck"}));
	REQUIRE(CHECK_COLUMN(result, 3, {"duck", "duck", "duck"}));
}

TEST_CASE("Test copy statement with default values", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	con.AddComment("create a file only consisting of integers");
	ofstream from_csv_file(fs.JoinPath(csv_path, "test.csv"));
	int64_t expected_sum_a = 0;
	int64_t expected_sum_c = 0;
	for (int i = 0; i < 5000; i++) {
		from_csv_file << i << endl;

		expected_sum_a += i;
		expected_sum_c += i + 7;
	}
	from_csv_file.close();

	con.AddComment("load CSV file into a table");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR DEFAULT('hello'), c INTEGER DEFAULT(3+4));"));
	result = con.Query("COPY test (a) FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));
	result = con.Query("COPY test (c) FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));

	result =
	    con.Query("SELECT COUNT(a), COUNT(b), COUNT(c), MIN(LENGTH(b)), MAX(LENGTH(b)), SUM(a), SUM(c) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));
	REQUIRE(CHECK_COLUMN(result, 1, {10000}));
	REQUIRE(CHECK_COLUMN(result, 2, {10000}));
	REQUIRE(CHECK_COLUMN(result, 3, {5}));
	REQUIRE(CHECK_COLUMN(result, 4, {5}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value::BIGINT(expected_sum_a)}));
	REQUIRE(CHECK_COLUMN(result, 6, {Value::BIGINT(expected_sum_c)}));
}

TEST_CASE("Test copy statement with long lines", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	con.AddComment("generate a CSV file with a very long string");
	ofstream from_csv_file(fs.JoinPath(csv_path, "test.csv"));
	string big_string_a(100000, 'a');
	string big_string_b(200000, 'b');
	from_csv_file << 10 << "," << big_string_a << "," << 20 << endl;
	from_csv_file << 20 << "," << big_string_b << "," << 30 << endl;
	from_csv_file.close();

	con.AddComment("load CSV file into a table");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR, c INTEGER);"));
	result = con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	result = con.Query("SELECT LENGTH(b) FROM test ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {100000, 200000}));

	result = con.Query("SELECT SUM(a), SUM(c) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {30}));
	REQUIRE(CHECK_COLUMN(result, 1, {50}));
}

TEST_CASE("Test copy statement with quotes and newlines", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	con.AddComment("generate a CSV file with newlines enclosed by quotes");
	ofstream from_csv_file(fs.JoinPath(csv_path, "test.csv"));
	from_csv_file << "\"hello\\nworld\",\"5\"" << endl;
	from_csv_file << "\"what,\\n brings, you here\\n, today\",\"6\"" << endl;
	from_csv_file.close();

	con.AddComment("load CSV file into a table");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR, b INTEGER);"));
	result = con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	result = con.Query("SELECT SUM(b) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {11}));

	result = con.Query("SELECT a FROM test ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello\\nworld", "what,\\n brings, you here\\n, today"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	con.AddComment("quotes in the middle of a quoted string cause an exception if they are not escaped");
	from_csv_file.open(fs.JoinPath(csv_path, "test.csv"));
	from_csv_file << "\"hello\\n\"w\"o\"rld\",\"5\"" << endl;
	from_csv_file << "\"what,\\n brings, you here\\n, today\",\"6\"" << endl;
	from_csv_file.close();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR, b INTEGER);"));
	REQUIRE_FAIL(con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';"));

	con.AddComment("now the same quotes are escaped");
	from_csv_file.open(fs.JoinPath(csv_path, "test.csv"));
	from_csv_file << "\"hello\\n\"\"w\"\"o\"\"rld\",\"5\"" << endl;
	from_csv_file << "\"what,\\n brings, you here\\n, today\",\"6\"" << endl;
	from_csv_file.close();

	result = con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	result = con.Query("SELECT SUM(b) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {11}));
	result = con.Query("SELECT a,b FROM test ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello\\n\"w\"o\"rld", "what,\\n brings, you here\\n, today"}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 6}));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	con.AddComment("not escaped escape string in quotes throws an exception");
	from_csv_file.open(fs.JoinPath(csv_path, "test.csv"));
	from_csv_file << "\"\\\"escaped\\\",\"5\"" << endl;
	from_csv_file << "yea,6" << endl;
	from_csv_file.close();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR, b INTEGER);"));
	REQUIRE_FAIL(con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "' (ESCAPE '\\');"));
}

TEST_CASE("Test copy statement with many empty lines", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	con.AddComment("generate CSV file with a very long string");
	ofstream from_csv_file(fs.JoinPath(csv_path, "test.csv"));
	from_csv_file << "1\n";
	for (idx_t i = 0; i < 19999; i++) {
		from_csv_file << "\n";
	}
	from_csv_file.close();

	con.AddComment("load CSV file into a table");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
	result = con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {20000}));

	result = con.Query("SELECT SUM(a) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Test different line endings", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	con.AddComment("generate CSV file with different line endings");
	ofstream from_csv_file(fs.JoinPath(csv_path, "test.csv"));
	from_csv_file << 10 << ","
	              << "hello"
	              << "," << 20 << "\r\n";
	from_csv_file << 20 << ","
	              << "world"
	              << "," << 30 << '\n';
	from_csv_file << 30 << ","
	              << "test"
	              << "," << 30 << '\r';
	from_csv_file.close();

	con.AddComment("load CSV file into a table");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR, c INTEGER);"));
	result = con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	result = con.Query("SELECT LENGTH(b) FROM test ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 5, 4}));

	result = con.Query("SELECT SUM(a), SUM(c) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {60}));
	REQUIRE(CHECK_COLUMN(result, 1, {80}));
}

TEST_CASE("Test Windows Newlines with a long file", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	idx_t line_count = 20000;
	int64_t sum_a = 0, sum_c = 0;

	con.AddComment("generate a CSV file with many strings");
	ofstream from_csv_file(fs.JoinPath(csv_path, "test.csv"));
	for (idx_t i = 0; i < line_count; i++) {
		from_csv_file << i << ","
		              << "hello"
		              << "," << i + 2 << "\r\n";

		sum_a += i;
		sum_c += i + 2;
	}
	from_csv_file.close();

	con.AddComment("load CSV file into a table");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR, c INTEGER);"));
	result = con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(line_count)}));

	result = con.Query("SELECT SUM(a), MIN(LENGTH(b)), MAX(LENGTH(b)), SUM(LENGTH(b)), SUM(c) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum_a)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(5)}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::BIGINT(5)}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value::BIGINT(5 * line_count)}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value::BIGINT(sum_c)}));

	REQUIRE_NO_FAIL(con.Query("DELETE FROM test;"));
	con.AddComment("now do the same with a multi-byte quote that is not actually used");
	result = con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "' QUOTE 'BLABLABLA';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(line_count)}));

	result = con.Query("SELECT SUM(a), MIN(LENGTH(b)), MAX(LENGTH(b)), SUM(LENGTH(b)), SUM(c) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum_a)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(5)}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::BIGINT(5)}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value::BIGINT(5 * line_count)}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value::BIGINT(sum_c)}));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	con.AddComment("generate a csv file with one value and many empty values");
	ofstream from_csv_file_empty(fs.JoinPath(csv_path, "test2.csv"));
	from_csv_file_empty << 1 << "\r\n";
	for (idx_t i = 0; i < line_count - 1; i++) {
		from_csv_file_empty << "\r\n";
	}
	from_csv_file_empty.close();

	con.AddComment("load CSV file into a table");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
	result = con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test2.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(line_count)}));

	result = con.Query("SELECT SUM(a) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(1)}));
}

TEST_CASE("Test lines that exceed the maximum line size", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	con.AddComment("generate CSV file with 20 MB string");
	ofstream from_csv_file(fs.JoinPath(csv_path, "test.csv"));
	string big_string(2048576, 'a');
	from_csv_file << 10 << "," << big_string << "," << 20 << endl;
	from_csv_file.close();

	con.AddComment("value is too big for loading");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR, c INTEGER);"));
	REQUIRE_FAIL(con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';"));
}

TEST_CASE("Test copy from/to on-time dataset", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto ontime_csv = fs.JoinPath(csv_path, "ontime.csv");
	WriteBinary(ontime_csv, ontime_sample, sizeof(ontime_sample));

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE ontime(year SMALLINT, quarter SMALLINT, month SMALLINT, dayofmonth SMALLINT, dayofweek SMALLINT, "
	    "flightdate DATE, uniquecarrier CHAR(7), airlineid DECIMAL(8,2), carrier CHAR(2), tailnum VARCHAR(50), "
	    "flightnum VARCHAR(10), originairportid INTEGER, originairportseqid INTEGER, origincitymarketid INTEGER, "
	    "origin CHAR(5), origincityname VARCHAR(100), originstate CHAR(2), originstatefips VARCHAR(10), "
	    "originstatename VARCHAR(100), originwac DECIMAL(8,2), destairportid INTEGER, destairportseqid INTEGER, "
	    "destcitymarketid INTEGER, dest CHAR(5), destcityname VARCHAR(100), deststate CHAR(2), deststatefips "
	    "VARCHAR(10), deststatename VARCHAR(100), destwac DECIMAL(8,2), crsdeptime DECIMAL(8,2), deptime DECIMAL(8,2), "
	    "depdelay DECIMAL(8,2), depdelayminutes DECIMAL(8,2), depdel15 DECIMAL(8,2), departuredelaygroups "
	    "DECIMAL(8,2), deptimeblk VARCHAR(20), taxiout DECIMAL(8,2), wheelsoff DECIMAL(8,2), wheelson DECIMAL(8,2), "
	    "taxiin DECIMAL(8,2), crsarrtime DECIMAL(8,2), arrtime DECIMAL(8,2), arrdelay DECIMAL(8,2), arrdelayminutes "
	    "DECIMAL(8,2), arrdel15 DECIMAL(8,2), arrivaldelaygroups DECIMAL(8,2), arrtimeblk VARCHAR(20), cancelled "
	    "SMALLINT, cancellationcode CHAR(1), diverted SMALLINT, crselapsedtime DECIMAL(8,2), actualelapsedtime "
	    "DECIMAL(8,2), airtime DECIMAL(8,2), flights DECIMAL(8,2), distance DECIMAL(8,2), distancegroup SMALLINT, "
	    "carrierdelay DECIMAL(8,2), weatherdelay DECIMAL(8,2), nasdelay DECIMAL(8,2), securitydelay DECIMAL(8,2), "
	    "lateaircraftdelay DECIMAL(8,2), firstdeptime VARCHAR(10), totaladdgtime VARCHAR(10), longestaddgtime "
	    "VARCHAR(10), divairportlandings VARCHAR(10), divreacheddest VARCHAR(10), divactualelapsedtime VARCHAR(10), "
	    "divarrdelay VARCHAR(10), divdistance VARCHAR(10), div1airport VARCHAR(10), div1aiportid INTEGER, "
	    "div1airportseqid INTEGER, div1wheelson VARCHAR(10), div1totalgtime VARCHAR(10), div1longestgtime VARCHAR(10), "
	    "div1wheelsoff VARCHAR(10), div1tailnum VARCHAR(10), div2airport VARCHAR(10), div2airportid INTEGER, "
	    "div2airportseqid INTEGER, div2wheelson VARCHAR(10), div2totalgtime VARCHAR(10), div2longestgtime VARCHAR(10), "
	    "div2wheelsoff VARCHAR(10), div2tailnum VARCHAR(10), div3airport VARCHAR(10), div3airportid INTEGER, "
	    "div3airportseqid INTEGER, div3wheelson VARCHAR(10), div3totalgtime VARCHAR(10), div3longestgtime VARCHAR(10), "
	    "div3wheelsoff VARCHAR(10), div3tailnum VARCHAR(10), div4airport VARCHAR(10), div4airportid INTEGER, "
	    "div4airportseqid INTEGER, div4wheelson VARCHAR(10), div4totalgtime VARCHAR(10), div4longestgtime VARCHAR(10), "
	    "div4wheelsoff VARCHAR(10), div4tailnum VARCHAR(10), div5airport VARCHAR(10), div5airportid INTEGER, "
	    "div5airportseqid INTEGER, div5wheelson VARCHAR(10), div5totalgtime VARCHAR(10), div5longestgtime VARCHAR(10), "
	    "div5wheelsoff VARCHAR(10), div5tailnum VARCHAR(10));"));

	result = con.Query("COPY ontime FROM '" + ontime_csv + "' DELIMITER ',' HEADER;");
	REQUIRE(CHECK_COLUMN(result, 0, {9}));

	result = con.Query("SELECT year, uniquecarrier, origin, origincityname, div5longestgtime FROM ontime;");
	REQUIRE(CHECK_COLUMN(result, 0, {1988, 1988, 1988, 1988, 1988, 1988, 1988, 1988, 1988}));
	REQUIRE(CHECK_COLUMN(result, 1, {"AA", "AA", "AA", "AA", "AA", "AA", "AA", "AA", "AA"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"JFK", "JFK", "JFK", "JFK", "JFK", "JFK", "JFK", "JFK", "JFK"}));
	REQUIRE(CHECK_COLUMN(result, 3,
	                     {"New York, NY", "New York, NY", "New York, NY", "New York, NY", "New York, NY",
	                      "New York, NY", "New York, NY", "New York, NY", "New York, NY"}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value(), Value(), Value(), Value(), Value(), Value(), Value(), Value(), Value()}));

	result = con.Query("COPY ontime TO '" + ontime_csv + "' DELIMITER ',' HEADER;");
	REQUIRE(CHECK_COLUMN(result, 0, {9}));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM ontime;"));
	result = con.Query("COPY ontime FROM '" + ontime_csv + "' DELIMITER ',' HEADER;");
	REQUIRE(CHECK_COLUMN(result, 0, {9}));

	result = con.Query("SELECT year, uniquecarrier, origin, origincityname, div5longestgtime FROM ontime;");
	REQUIRE(CHECK_COLUMN(result, 0, {1988, 1988, 1988, 1988, 1988, 1988, 1988, 1988, 1988}));
	REQUIRE(CHECK_COLUMN(result, 1, {"AA", "AA", "AA", "AA", "AA", "AA", "AA", "AA", "AA"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"JFK", "JFK", "JFK", "JFK", "JFK", "JFK", "JFK", "JFK", "JFK"}));
	REQUIRE(CHECK_COLUMN(result, 3,
	                     {"New York, NY", "New York, NY", "New York, NY", "New York, NY", "New York, NY",
	                      "New York, NY", "New York, NY", "New York, NY", "New York, NY"}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value(), Value(), Value(), Value(), Value(), Value(), Value(), Value(), Value()}));
}

TEST_CASE("Test copy from/to lineitem csv", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto lineitem_csv = fs.JoinPath(csv_path, "lineitem.csv");
	WriteBinary(lineitem_csv, lineitem_sample, sizeof(lineitem_sample));

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE lineitem(l_orderkey INT NOT NULL, l_partkey INT NOT NULL, l_suppkey INT NOT NULL, l_linenumber "
	    "INT NOT NULL, l_quantity INTEGER NOT NULL, l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) "
	    "NOT NULL, l_tax DECIMAL(15,2) NOT NULL, l_returnflag VARCHAR(1) NOT NULL, l_linestatus VARCHAR(1) NOT NULL, "
	    "l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_shipinstruct VARCHAR(25) "
	    "NOT NULL, l_shipmode VARCHAR(10) NOT NULL, l_comment VARCHAR(44) NOT NULL);"));
	result = con.Query("COPY lineitem FROM '" + lineitem_csv + "' DELIMITER '|'");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));

	result = con.Query("SELECT l_partkey, l_comment FROM lineitem WHERE l_orderkey=1 ORDER BY l_linenumber;");
	REQUIRE(CHECK_COLUMN(result, 0, {15519, 6731, 6370, 214, 2403, 1564}));
	REQUIRE(
	    CHECK_COLUMN(result, 1,
	                 {"egular courts above the", "ly final dependencies: slyly bold ", "riously. regular, express dep",
	                  "lites. fluffily even de", " pending foxes. slyly re", "arefully slyly ex"}));

	con.AddComment("test COPY ... TO ... with HEADER");
	result = con.Query("COPY lineitem TO '" + lineitem_csv + "' (DELIMITER ' ', HEADER);");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));

	con.AddComment("clear the table");
	REQUIRE_NO_FAIL(con.Query("DELETE FROM lineitem;"));
	result = con.Query("SELECT * FROM lineitem;");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	con.AddComment("now copy back into the table");
	result = con.Query("COPY lineitem FROM '" + lineitem_csv + "' DELIMITER ' ' HEADER;");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));

	result = con.Query("SELECT l_partkey, l_comment FROM lineitem WHERE l_orderkey=1 ORDER BY l_linenumber;");
	REQUIRE(CHECK_COLUMN(result, 0, {15519, 6731, 6370, 214, 2403, 1564}));
	REQUIRE(
	    CHECK_COLUMN(result, 1,
	                 {"egular courts above the", "ly final dependencies: slyly bold ", "riously. regular, express dep",
	                  "lites. fluffily even de", " pending foxes. slyly re", "arefully slyly ex"}));
}

TEST_CASE("Test copy from web_page csv", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto webpage_csv = fs.JoinPath(csv_path, "web_page.csv");
	WriteBinary(webpage_csv, web_page, sizeof(web_page));

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE web_page(wp_web_page_sk integer not null, wp_web_page_id char(16) not null, wp_rec_start_date "
	    "date, wp_rec_end_date date, wp_creation_date_sk integer, wp_access_date_sk integer, wp_autogen_flag char(1), "
	    "wp_customer_sk integer, wp_url varchar(100), wp_type char(50), wp_char_count integer, wp_link_count integer, "
	    "wp_image_count integer, wp_max_ad_count integer, primary key (wp_web_page_sk));"));

	result = con.Query("COPY web_page FROM '" + webpage_csv + "' DELIMITER '|';");
	REQUIRE(CHECK_COLUMN(result, 0, {60}));

	result = con.Query("SELECT * FROM web_page ORDER BY wp_web_page_sk LIMIT 3;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"AAAAAAAABAAAAAAA", "AAAAAAAACAAAAAAA", "AAAAAAAACAAAAAAA"}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::DATE(1997, 9, 3), Value::DATE(1997, 9, 3), Value::DATE(2000, 9, 3)}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value(), Value::DATE(2000, 9, 2), Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {2450810, 2450814, 2450814}));
	REQUIRE(CHECK_COLUMN(result, 5, {2452620, 2452580, 2452611}));
	REQUIRE(CHECK_COLUMN(result, 6, {"Y", "N", "N"}));
	REQUIRE(CHECK_COLUMN(result, 7, {98539, Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 8, {"http://www.foo.com", "http://www.foo.com", "http://www.foo.com"}));
	REQUIRE(CHECK_COLUMN(result, 9, {"welcome", "protected", "feedback"}));
	REQUIRE(CHECK_COLUMN(result, 10, {2531, 1564, 1564}));
	REQUIRE(CHECK_COLUMN(result, 11, {8, 4, 4}));
	REQUIRE(CHECK_COLUMN(result, 12, {3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 13, {4, 1, 4}));
}

TEST_CASE("Test copy from greek-utf8 csv", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto csv_file = fs.JoinPath(csv_path, "greek_utf8.csv");
	WriteBinary(csv_file, greek_utf8, sizeof(greek_utf8));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE greek_utf8(i INTEGER, j VARCHAR, k INTEGER);"));

	result = con.Query("COPY greek_utf8 FROM '" + csv_file + "' DELIMITER '|';");
	REQUIRE(CHECK_COLUMN(result, 0, {8}));

	result = con.Query("SELECT * FROM greek_utf8 ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1689, 1690, 41561, 45804, 51981, 171067, 182773, 607808}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {"\x30\x30\x69\\047\x6d", "\x30\x30\x69\\047\x76", "\x32\x30\x31\x35\xe2\x80\x8e",
	                      "\x32\x31\xcf\x80", "\x32\x34\x68\x6f\x75\x72\x73\xe2\x80\xac",
	                      "\x61\x72\x64\x65\xcc\x80\x63\x68", "\x61\xef\xac\x81",
	                      "\x70\x6f\x76\x65\x72\x74\x79\xe2\x80\xaa"}));
	REQUIRE(CHECK_COLUMN(result, 2, {2, 2, 1, 1, 1, 2, 1, 1}));
}

TEST_CASE("Test copy from ncvoter csv", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto ncvoter_csv = fs.JoinPath(csv_path, "ncvoter.csv");
	WriteBinary(ncvoter_csv, ncvoter, sizeof(ncvoter));

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE IF NOT EXISTS ncvoters(county_id INTEGER, county_desc STRING, voter_reg_num STRING,status_cd "
	    "STRING, voter_status_desc STRING, reason_cd STRING, voter_status_reason_desc STRING, absent_ind STRING, "
	    "name_prefx_cd STRING,last_name STRING, first_name STRING, midl_name STRING, name_sufx_cd STRING, "
	    "full_name_rep STRING,full_name_mail STRING, house_num STRING, half_code STRING, street_dir STRING, "
	    "street_name STRING, street_type_cd STRING, street_sufx_cd STRING, unit_designator STRING, unit_num STRING, "
	    "res_city_desc STRING,state_cd STRING, zip_code STRING, res_street_address STRING, res_city_state_zip STRING, "
	    "mail_addr1 STRING, mail_addr2 STRING, mail_addr3 STRING, mail_addr4 STRING, mail_city STRING, mail_state "
	    "STRING, mail_zipcode STRING, mail_city_state_zip STRING, area_cd STRING, phone_num STRING, full_phone_number "
	    "STRING, drivers_lic STRING, race_code STRING, race_desc STRING, ethnic_code STRING, ethnic_desc STRING, "
	    "party_cd STRING, party_desc STRING, sex_code STRING, sex STRING, birth_age STRING, birth_place STRING, "
	    "registr_dt STRING, precinct_abbrv STRING, precinct_desc STRING,municipality_abbrv STRING, municipality_desc "
	    "STRING, ward_abbrv STRING, ward_desc STRING, cong_dist_abbrv STRING, cong_dist_desc STRING, super_court_abbrv "
	    "STRING, super_court_desc STRING, judic_dist_abbrv STRING, judic_dist_desc STRING, nc_senate_abbrv STRING, "
	    "nc_senate_desc STRING, nc_house_abbrv STRING, nc_house_desc STRING,county_commiss_abbrv STRING, "
	    "county_commiss_desc STRING, township_abbrv STRING, township_desc STRING,school_dist_abbrv STRING, "
	    "school_dist_desc STRING, fire_dist_abbrv STRING, fire_dist_desc STRING, water_dist_abbrv STRING, "
	    "water_dist_desc STRING, sewer_dist_abbrv STRING, sewer_dist_desc STRING, sanit_dist_abbrv STRING, "
	    "sanit_dist_desc STRING, rescue_dist_abbrv STRING, rescue_dist_desc STRING, munic_dist_abbrv STRING, "
	    "munic_dist_desc STRING, dist_1_abbrv STRING, dist_1_desc STRING, dist_2_abbrv STRING, dist_2_desc STRING, "
	    "confidential_ind STRING, age STRING, ncid STRING, vtd_abbrv STRING, vtd_desc STRING);"));
	result = con.Query("COPY ncvoters FROM '" + ncvoter_csv + "' DELIMITER '\t';");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));

	result = con.Query("SELECT county_id, county_desc, vtd_desc, name_prefx_cd FROM ncvoters;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 1, 1, 1, 1, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {"ALAMANCE", "ALAMANCE", "ALAMANCE", "ALAMANCE", "ALAMANCE", "ALAMANCE", "ALAMANCE",
	                      "ALAMANCE", "ALAMANCE", "ALAMANCE"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"09S", "09S", "03W", "09S", "1210", "035", "124", "06E", "035", "064"}));
	REQUIRE(CHECK_COLUMN(result, 3,
	                     {Value(), Value(), Value(), Value(), Value(), Value(), Value(), Value(), Value(), Value()}));
}

TEST_CASE("Test date copy", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE date_test(d date);"));

	auto csv_path = GetCSVPath();
	auto date_csv = fs.JoinPath(csv_path, "date.csv");
	WriteCSV(date_csv, "2019-06-05\n");

	result = con.Query("COPY date_test FROM '" + date_csv + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT cast(d as string) FROM date_test;");
	REQUIRE(CHECK_COLUMN(result, 0, {"2019-06-05"}));
}

TEST_CASE("Test cranlogs broken gzip copy and temp table", "[copy][.]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto cranlogs_csv = fs.JoinPath(csv_path, "cranlogs.csv.gz");
	WriteBinary(cranlogs_csv, tmp2013_06_15, sizeof(tmp2013_06_15));

	REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY TABLE cranlogs (date date,time string,size int,r_version string,r_arch "
	                          "string,r_os string,package string,version string,country string,ip_id int)"));

	result = con.Query("COPY cranlogs FROM '" + cranlogs_csv + "' DELIMITER ',' HEADER;");
	REQUIRE(CHECK_COLUMN(result, 0, {37459}));
}

TEST_CASE("Test imdb escapes", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto imdb_movie_info = fs.JoinPath(csv_path, "imdb_movie_info.csv");
	WriteBinary(imdb_movie_info, imdb_movie_info_escaped, sizeof(imdb_movie_info_escaped));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE movie_info (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, "
	                          "info_type_id integer NOT NULL, info text NOT NULL, note text);"));

	result = con.Query("COPY movie_info FROM '" + imdb_movie_info + "' DELIMITER ',' ESCAPE '\\';");
	REQUIRE(result->success);
	REQUIRE(CHECK_COLUMN(result, 0, {201}));

	con.AddComment("TODO: actually check results");
	result = con.Query("SELECT * FROM movie_info;");
}

TEST_CASE("Test read CSV function with lineitem", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto lineitem_csv = fs.JoinPath(csv_path, "lineitem.csv");
	WriteBinary(lineitem_csv, lineitem_sample, sizeof(lineitem_sample));

	con.AddComment("create a view using the read_csv function");
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW lineitem AS SELECT * FROM read_csv('" + lineitem_csv +
	    "', '|', STRUCT_PACK(l_orderkey := 'INT', l_partkey := 'INT', l_suppkey := 'INT', l_linenumber := 'INT', "
	    "l_quantity := 'INTEGER', l_extendedprice := 'DOUBLE', l_discount := 'DOUBLE', l_tax := 'DOUBLE', l_returnflag "
	    ":= 'VARCHAR', l_linestatus := 'VARCHAR', l_shipdate := 'DATE', l_commitdate := 'DATE', l_receiptdate := "
	    "'DATE', l_shipinstruct := 'VARCHAR', l_shipmode := 'VARCHAR', l_comment := 'VARCHAR'));"));

	con.AddComment("each of these will read the CSV again through the view");
	result = con.Query("SELECT COUNT(*) FROM lineitem");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));

	result = con.Query("SELECT l_partkey, l_comment FROM lineitem WHERE l_orderkey=1 ORDER BY l_linenumber;");
	REQUIRE(CHECK_COLUMN(result, 0, {15519, 6731, 6370, 214, 2403, 1564}));
	REQUIRE(
	    CHECK_COLUMN(result, 1,
	                 {"egular courts above the", "ly final dependencies: slyly bold ", "riously. regular, express dep",
	                  "lites. fluffily even de", " pending foxes. slyly re", "arefully slyly ex"}));

	con.AddComment("test incorrect usage of read_csv function");
	con.AddComment("wrong argument type");
	REQUIRE_FAIL(con.Query("SELECT * FROM read_csv('" + lineitem_csv + "', '|', STRUCT_PACK(l_orderkey := 5))"));
}

TEST_CASE("Test CSV with UTF8 NFC Normalization", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto nfc_csv = fs.JoinPath(csv_path, "nfc.csv");

	const char *nfc_content = "\xc3\xbc\n\x75\xcc\x88";

	WriteBinary(nfc_csv, (const uint8_t *)nfc_content, strlen(nfc_content));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE nfcstrings (s STRING);"));
	REQUIRE_NO_FAIL(con.Query("COPY nfcstrings FROM '" + nfc_csv + "';"));

	result = con.Query("SELECT COUNT(*) FROM nfcstrings WHERE s = '\xc3\xbc'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(2)}));
}

TEST_CASE("Test CSV with Unicode NFC Normalization test suite", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.AddComment("http://www.unicode.org/Public/UCD/latest/ucd/NormalizationTest.txt");

	auto csv_path = GetCSVPath();
	auto nfc_csv = fs.JoinPath(csv_path, "nfc_test_suite.csv");

	WriteBinary(nfc_csv, nfc_normalization, sizeof(nfc_normalization));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE nfcstrings (source STRING, nfc STRING, nfd STRING);"));
	REQUIRE_NO_FAIL(con.Query("COPY nfcstrings FROM '" + nfc_csv + "' DELIMITER '|';"));

	result = con.Query("SELECT COUNT(*) FROM nfcstrings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(18819)}));

	result = con.Query("SELECT COUNT(*) FROM nfcstrings WHERE source=nfc");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(18819)}));

	result = con.Query("SELECT COUNT(*) FROM nfcstrings WHERE nfc=nfd");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(18819)}));
}

TEST_CASE("Test CSV reading/writing from relations", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;

	con.AddComment("write a bunch of values to a CSV");
	auto csv_file = TestCreatePath("relationtest.csv");

	con.Values("(1), (2), (3)", {"i"})->WriteCSV(csv_file);

	con.AddComment("now scan the CSV file");
	auto csv_scan = con.ReadCSV(csv_file, {"i INTEGER"});
	result = csv_scan->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	REQUIRE_THROWS(con.ReadCSV(csv_file, {"i INTEGER); SELECT 42;--"}));
}

TEST_CASE("Test BLOB with COPY INTO", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.AddComment("Creating a blob buffer with almost ALL ASCII chars");
	uint8_t num_chars = 256 - 5; con.AddComment("skipping: '\0', '\n', '\15', ',', '\32'");
	unique_ptr<char[]> blob_chars(new char[num_chars + 1]);
	char ch = '\0';
	idx_t buf_idx = 0;
	for(idx_t i = 0; i < 255; ++i, ++ch) {
		con.AddComment("skip chars: '\0', new line, shift in, comma, and crtl+Z");
		if(ch == '\0' || ch == '\n' || ch == '\15' || ch == ',' || ch == '\32') {
			continue;
		}
		blob_chars[buf_idx] = ch;
    	++buf_idx;
	}
	blob_chars[num_chars] = '\0';

	con.AddComment("Wrinting BLOB values to a csv file");
	string blob_file_path = TestCreatePath("blob_file.csv");
    ofstream ofs_blob_file(blob_file_path, std::ofstream::out | std::ofstream::app);
    con.AddComment("Insert all ASCII chars from 1 to 255, skipping '\0', '\n', '\15', and ',' chars");
   	ofs_blob_file << blob_chars.get();
	ofs_blob_file.close();

	con.AddComment("COPY INTO");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BYTEA);"));
    result = con.Query("COPY blobs FROM '" + blob_file_path + "';");
    REQUIRE(CHECK_COLUMN(result, 0, {1}));

    con.AddComment("Testing if the system load/store correctly the bytes");
	string blob_str(blob_chars.get(), num_chars);
	result = con.Query("SELECT b FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB(blob_str)}));

	blob_chars.reset();
	TestDeleteFile(blob_file_path);
}
