#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Single UNIQUE constraint", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER UNIQUE, j INTEGER)"));

	// insert unique values
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3, 4), (2, 5)"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5}));

	// insert a duplicate value as part of a chain of values, this should fail
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (6, 6), (3, 4);"));

	// unique constraints accept NULL values, unlike PRIMARY KEY columns
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (NULL, 6), (NULL, 7)"));

	// but if we try to replace them like this it's going to fail
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=77 WHERE i IS NULL"));

	result = con.Query("SELECT * FROM integers ORDER BY i, j");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {6, 7, 5, 4}));

	// we can replace them like this though
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=77 WHERE i IS NULL AND j=6"));

	result = con.Query("SELECT * FROM integers ORDER BY i, j");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 2, 3, 77}));
	REQUIRE(CHECK_COLUMN(result, 1, {7, 5, 4, 6}));

	for (idx_t i = 0; i < 10; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (NULL, 6), (NULL, 7)"));
	}
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (NULL, 6), (3, 7)"));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
}

TEST_CASE("UNIQUE constraint on temporary tables", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY TABLE integers(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE UNIQUE INDEX uidx ON integers (i) "));

	// insert unique values
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3, 4), (2, 5)"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5}));

	// insert a duplicate value as part of a chain of values, this should fail
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (6, 6), (3, 4);"));

	// unique constraints accept NULL values, unlike PRIMARY KEY columns
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (NULL, 6), (NULL, 7)"));

	// but if we try to replace them like this it's going to fail
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=77 WHERE i IS NULL"));

	result = con.Query("SELECT * FROM integers ORDER BY i, j");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {6, 7, 5, 4}));

	// we can replace them like this though
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=77 WHERE i IS NULL AND j=6"));

	result = con.Query("SELECT * FROM integers ORDER BY i, j");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 2, 3, 77}));
	REQUIRE(CHECK_COLUMN(result, 1, {7, 5, 4, 6}));

	for (idx_t i = 0; i < 10; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (NULL, 6), (NULL, 7)"));
	}
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES  (3, 4)"));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
}

TEST_CASE("NULL values and a multi-column UNIQUE constraint", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY TABLE integers(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE UNIQUE INDEX uidx ON integers (i,j) "));

	// insert unique values
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3, 4), (2, 5)"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5}));

	// insert a duplicate value as part of a chain of values, this should fail
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (6, 6), (3, 4);"));

	// unique constraints accept NULL values, unlike PRIMARY KEY columns
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (NULL, 6), (NULL, 6), (NULL, 7)"));

	// but if we try to replace them like this it's going to fail
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=77 WHERE i IS NULL"));

	result = con.Query("SELECT * FROM integers ORDER BY i, j");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {6, 6, 7, 5, 4}));

	// we can replace them like this though
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=77 WHERE i IS NULL AND j=7"));

	result = con.Query("SELECT * FROM integers ORDER BY i, j");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), 2, 3, 77}));
	REQUIRE(CHECK_COLUMN(result, 1, {6, 6, 5, 4, 7}));

	for (idx_t i = 0; i < 10; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (NULL, 6), (NULL, 7)"));
	}
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES  (3, 4)"));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
}

TEST_CASE("UNIQUE constraint on temporary tables with Strings", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY TABLE integers(i INTEGER, j VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("CREATE UNIQUE INDEX \"uidx\" ON \"integers\" (\"j\") "));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3, '4'), (2, '5')"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {"4", "5"}));

	// insert a duplicate value as part of a chain of values, this should fail
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (6, '6'), (3, '4');"));

	// unique constraints accept NULL values, unlike PRIMARY KEY columns
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (6,NULL), (7,NULL)"));

	// but if we try to replace them like this it's going to fail
	REQUIRE_FAIL(con.Query("UPDATE integers SET j='77' WHERE j IS NULL"));

	result = con.Query("SELECT * FROM integers ORDER BY i, j");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 6, 7}));
	REQUIRE(CHECK_COLUMN(result, 1, {"5", "4", Value(), Value()}));

	// we can replace them like this though
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET j='7777777777777777777777777777' WHERE j IS NULL AND i=6"));
	for (idx_t i = 0; i < 10; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (6,NULL), (7,NULL)"));
	}
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES  (3, '4')"));
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES  (3, '4')"));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
}

TEST_CASE("UNIQUE constraint on temporary tables with duplicate data", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY TABLE integers(i INTEGER, j VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3, '4'), (2, '4')"));

	REQUIRE_FAIL(con.Query("CREATE UNIQUE INDEX uidx ON integers (j) "));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
}

TEST_CASE("Multiple constraints", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER UNIQUE)"));

	// no constraints are violated
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 1), (2, 2)"));
	// only the second UNIQUE constraint is violated
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (3, 3), (4, 1)"));
	// no constraints are violated
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3, 3), (4, 4)"));
	// insert many values with a unique constraint violation at the end
	REQUIRE_FAIL(con.Query(
	    "INSERT INTO integers VALUES  (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13), "
	    "(14, 14), (15, 15), (16, 16), (17, 17), (18, 18), (19, 19), (20, 20), (21, 21), (22, 22), (23, 23), (24, 24), "
	    "(25, 25), (26, 26), (27, 27), (28, 28), (29, 29), (30, 30), (31, 31), (32, 32), (33, 33), (34, 34), (35, 35), "
	    "(36, 36), (37, 37), (38, 38), (39, 39), (40, 40), (41, 41), (42, 42), (43, 43), (44, 44), (45, 45), (46, 46), "
	    "(47, 47), (48, 48), (49, 49), (50, 50), (51, 51), (52, 52), (53, 53), (54, 54), (55, 55), (56, 56), (57, 57), "
	    "(58, 58), (59, 59), (60, 60), (61, 61), (62, 62), (63, 63), (64, 64), (65, 65), (66, 66), (67, 67), (68, 68), "
	    "(69, 69), (70, 70), (71, 71), (72, 72), (73, 73), (74, 74), (75, 75), (76, 76), (77, 77), (78, 78), (79, 79), "
	    "(80, 80), (81, 81), (82, 82), (83, 83), (84, 84), (85, 85), (86, 86), (87, 87), (88, 88), (89, 89), (90, 90), "
	    "(91, 91), (92, 92), (93, 93), (94, 94), (95, 95), (96, 96), (97, 97), (98, 98), (99, 99), (5, 5), (NULL, "
	    "NULL), (NULL, NULL), (NULL, NULL), (NULL, NULL)"));

	result = con.Query("SELECT * FROM integers WHERE i > 0 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 4}));

	// attempt to append values that were inserted before (but failed)
	// should work now
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (5, 5), (6, 6)"));

	result = con.Query("SELECT * FROM integers WHERE i > 0 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 6}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 4, 5, 6}));

	// now attempt conflicting updates
	// conflict on PRIMARY KEY
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=4, j=100 WHERE i=1"));
	// conflict on UNIQUE INDEX
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=100, j=4 WHERE j=1"));
	// we can insert the old tuple normally
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (100, 100)"));

	result = con.Query("SELECT * FROM integers WHERE i > 0 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 6, 100}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 4, 5, 6, 100}));
}
