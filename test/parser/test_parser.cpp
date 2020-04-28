#include "duckdb.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "catch.hpp"

using namespace duckdb;

TEST_CASE("Test parser", "[parser]") {
	Parser parser;

	SECTION("Query with several statements") {
		parser.ParseQuery("CREATE TABLE nums (num INTEGER);"
		                  "BEGIN TRANSACTION;"
		                  "    INSERT INTO nums VALUES(1);"
		                  "    INSERT INTO nums VALUES(2);"
		                  "    INSERT INTO nums VALUES(3);"
		                  "    INSERT INTO nums VALUES(4);"
		                  "COMMIT;");

		REQUIRE(parser.statements.size() == 7);
		REQUIRE(parser.statements[0]->type == StatementType::CREATE_STATEMENT);
		REQUIRE(parser.statements[1]->type == StatementType::TRANSACTION_STATEMENT);
		REQUIRE(parser.statements[2]->type == StatementType::INSERT_STATEMENT);
		REQUIRE(parser.statements[3]->type == StatementType::INSERT_STATEMENT);
		REQUIRE(parser.statements[4]->type == StatementType::INSERT_STATEMENT);
		REQUIRE(parser.statements[5]->type == StatementType::INSERT_STATEMENT);
		REQUIRE(parser.statements[6]->type == StatementType::TRANSACTION_STATEMENT);
	}

	SECTION("Wrong query") {
		REQUIRE_THROWS(parser.ParseQuery("TABLE"));
	}

	SECTION("Empty query") {
		parser.ParseQuery("");
		REQUIRE(parser.statements.size() == 0);
	}

	SECTION("Pragma query") {
		parser.ParseQuery("PRAGMA table_info('nums');");
		REQUIRE(parser.statements.size() == 1);
	}

	SECTION("Wrong pragma query") {
		parser.ParseQuery("PRAGMA table_info;");
		REQUIRE(parser.statements.size() == 1);
	}
}
