#include "utils.h"

using namespace odbc_col_attribute_test;

void odbc_col_attribute_test::DeleteExpectedMap(std::map<SQLLEN, ExpectedResult *> &expected) {
	for (auto &it : expected) {
		delete it.second;
	}
}

void odbc_col_attribute_test::CheckString(SQLHANDLE handle, const std::string &expected, SQLSMALLINT field_identifier) {
	SQLCHAR buffer[64];
	EXECUTE_AND_CHECK("SQLColAttribute", SQLColAttribute, handle, 1, field_identifier, buffer, sizeof(buffer), nullptr,
	                  nullptr);
	REQUIRE(ConvertToString(buffer) == expected);
}

void odbc_col_attribute_test::CheckInteger(SQLHANDLE handle, SQLLEN expected, SQLSMALLINT field_identifier) {
	SQLLEN number;
	EXECUTE_AND_CHECK("SQLColAttribute", SQLColAttribute, handle, 1, field_identifier, nullptr, 0, nullptr, &number);
	REQUIRE(number == expected);
}

void odbc_col_attribute_test::ExpectError(SQLHANDLE handle, SQLSMALLINT field_identifier) {
	SQLRETURN ret = SQLColAttribute(handle, 1, field_identifier, nullptr, sizeof(nullptr), nullptr, nullptr);
	REQUIRE(ret == SQL_ERROR);
}

void odbc_col_attribute_test::TestAllFields(SQLHANDLE hstmt, std::map<SQLLEN, ExpectedResult *> expected) {
	// SQL_DESC_AUTO_UNIQUE_VALUE
	SQLLEN n;
	SQLRETURN ret = SQLColAttribute(hstmt, 1, SQL_DESC_AUTO_UNIQUE_VALUE, nullptr, 0, nullptr, &n);
	REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
	REQUIRE(n == SQL_FALSE);

	// SQL_DESC_BASE_TABLE_NAME
	ExpectError(hstmt, SQL_DESC_BASE_TABLE_NAME);

	// SQL_DESC_CASE_SENSITIVE
	// [expected int: SQL_FALSE, char: SQL_TRUE]
	CheckInteger(hstmt, expected[SQL_DESC_CASE_SENSITIVE]->n, SQL_DESC_CASE_SENSITIVE);

	// SQL_DESC_CATALOG_NAME
	// [expected: int: "system", char: "system"]
	CheckString(hstmt, expected[SQL_DESC_CATALOG_NAME]->s, SQL_DESC_CATALOG_NAME);

	// SQL_DESC_CONCISE_TYPE
	// [expected: int: SQL_INTEGER, char: SQL_VARCHAR]
	CheckInteger(hstmt, expected[SQL_DESC_CONCISE_TYPE]->n, SQL_DESC_CONCISE_TYPE);

	// SQL_DESC_COUNT
	// [expected: int: 2, char: 2]
	CheckInteger(hstmt, expected[SQL_DESC_COUNT]->n, SQL_DESC_COUNT);

	// SQL_DESC_DISPLAY_SIZE
	// [expected: int 11, char 256]
	CheckInteger(hstmt, expected[SQL_DESC_DISPLAY_SIZE]->n, SQL_DESC_DISPLAY_SIZE);

	// SQL_DESC_FIXED_PREC_SCALE
	// [expected: int: SQL_FALSE, char: SQL_FALSE]
	CheckInteger(hstmt, expected[SQL_DESC_FIXED_PREC_SCALE]->n, SQL_DESC_FIXED_PREC_SCALE);

	// SQL_DESC_LENGTH
	// [expected: int: 10, char: -1]
	CheckInteger(hstmt, expected[SQL_DESC_LENGTH]->n, SQL_DESC_LENGTH);

	// SQL_DESC_LITERAL_PREFIX
	// [expected: int: "NULL", char: "''''"]
	CheckString(hstmt, expected[SQL_DESC_LITERAL_PREFIX]->s, SQL_DESC_LITERAL_PREFIX);

	// SQL_DESC_LITERAL_SUFFIX
	// [expected: int: "NULL", char: "''''"]
	CheckString(hstmt, expected[SQL_DESC_LITERAL_SUFFIX]->s, SQL_DESC_LITERAL_SUFFIX);

	// SQL_DESC_LOCAL_TYPE_NAME
	// [expected: int: "", char: ""]
	CheckString(hstmt, expected[SQL_DESC_LOCAL_TYPE_NAME]->s, SQL_DESC_LOCAL_TYPE_NAME);

	// SQL_DESC_NULLABLE
	// [expected: int: SQL_NULLABLE, char: SQL_NULLABLE]
	CheckInteger(hstmt, expected[SQL_DESC_NULLABLE]->n, SQL_DESC_NULLABLE);

	// SQL_DESC_NUM_PREC_RADIX
	// [expected: int: 2, char: 0]
	CheckInteger(hstmt, expected[SQL_DESC_NUM_PREC_RADIX]->n, SQL_DESC_NUM_PREC_RADIX);

	// SQL_DESC_PRECISION
	// [expected: int: 10, char: -1]
	CheckInteger(hstmt, expected[SQL_DESC_PRECISION]->n, SQL_DESC_PRECISION);

	// SQL_NO_TOTAL -> Returns SQL_SUCCESS_WITH_INFO
	if (expected[SQL_COLUMN_SCALE]->n == SQL_NO_TOTAL) {
		// SQL_COLUMN_SCALE
		// [expected: int: 0, char: SQL_NO_TOTAL]
		ret = SQLColAttribute(hstmt, 1, SQL_COLUMN_SCALE, nullptr, 0, nullptr, &n);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		REQUIRE(n == expected[SQL_COLUMN_SCALE]->n);

		// SQL_DESC_SCALE
		// [expected: int: 0, char: SQL_NO_TOTAL]
		ret = SQLColAttribute(hstmt, 1, SQL_DESC_SCALE, nullptr, 0, nullptr, &n);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		REQUIRE(n == expected[SQL_DESC_SCALE]->n);
	} else {
		CheckInteger(hstmt, expected[SQL_COLUMN_SCALE]->n, SQL_COLUMN_SCALE);
		CheckInteger(hstmt, expected[SQL_DESC_SCALE]->n, SQL_DESC_SCALE);
	}
	// SQL_DESC_SCHEMA_NAME
	// [expected: int: "", char: ""]
	CheckString(hstmt, expected[SQL_DESC_SCHEMA_NAME]->s, SQL_DESC_SCHEMA_NAME);

	// SQL_DESC_SEARCHABLE
	// [expected: int: SQL_PRED_BASIC, char: SQL_SEARCHABLE]
	CheckInteger(hstmt, expected[SQL_DESC_SEARCHABLE]->n, SQL_DESC_SEARCHABLE);

	// SQL_DESC_TYPE
	// [expected: int: SQL_INTEGER, char: SQL_VARCHAR]
	CheckInteger(hstmt, expected[SQL_DESC_TYPE]->n, SQL_DESC_TYPE);

	// SQL_DESC_UNNAMED
	// [expected: int: SQL_NAMED, char: SQL_NAMED]
	CheckInteger(hstmt, expected[SQL_DESC_UNNAMED]->n, SQL_DESC_UNNAMED);

	// SQL_DESC_UNSIGNED
	// [expected: int: SQL_FALSE, char: SQL_TRUE]
	CheckInteger(hstmt, expected[SQL_DESC_UNSIGNED]->n, SQL_DESC_UNSIGNED);

	// SQL_DESC_UPDATABLE
	// [expected: int: SQL_ATTR_READONLY, char: SQL_ATTR_READONLY]
	CheckInteger(hstmt, expected[SQL_DESC_UPDATABLE]->n, SQL_DESC_UPDATABLE);

	DeleteExpectedMap(expected);
}
