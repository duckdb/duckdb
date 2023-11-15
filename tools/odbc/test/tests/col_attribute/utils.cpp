#include "utils.h"

using namespace odbc_col_attribute_test;

void odbc_col_attribute_test::CheckString(SQLHANDLE handle, const std::string &expected, SQLSMALLINT field_identifier) {
	SQLCHAR buffer[64];
	EXECUTE_AND_CHECK("SQLColAttribute", SQLColAttribute, handle, 1, field_identifier, buffer, sizeof(buffer), nullptr,
	                  nullptr);
	REQUIRE(ConvertToString(buffer) == expected);
}

void odbc_col_attribute_test::CheckInteger(SQLHANDLE handle, SQLLEN expected, SQLSMALLINT field_identifier) {
	SQLLEN number;
	std::string state;
	std::string message;
	// EXECUTE_AND_CHECK("SQLColAttribute", SQLColAttribute, handle, 1, field_identifier, nullptr, 0, nullptr, &number);
	SQLRETURN ret = SQLColAttribute(handle, 1, field_identifier, nullptr, 0, nullptr, &number);
	if (ret != SQL_SUCCESS) {
		ACCESS_DIAGNOSTIC(state, message, handle, SQL_HANDLE_STMT);
	}
	INFO(message);
	REQUIRE(number == expected);
}

void odbc_col_attribute_test::ExpectError(SQLHANDLE handle, SQLSMALLINT field_identifier) {
	SQLRETURN ret = SQLColAttribute(handle, 1, field_identifier, nullptr, sizeof(nullptr), nullptr, nullptr);
	REQUIRE(ret == SQL_ERROR);
}

static const std::map<int, std::string> sql_lookup {{SQL_DESC_CASE_SENSITIVE, "SQL_DESC_CASE_SENSITIVE"},
                                                    {SQL_DESC_CATALOG_NAME, "SQL_DESC_CATALOG_NAME"},
                                                    {SQL_DESC_CONCISE_TYPE, "SQL_DESC_CONCISE_TYPE"},
                                                    {SQL_DESC_COUNT, "SQL_DESC_COUNT"},
                                                    {SQL_DESC_DISPLAY_SIZE, "SQL_DESC_DISPLAY_SIZE"},
                                                    {SQL_DESC_FIXED_PREC_SCALE, "SQL_DESC_FIXED_PREC_SCALE"},
                                                    {SQL_DESC_LENGTH, "SQL_DESC_LENGTH"},
                                                    {SQL_DESC_LITERAL_PREFIX, "SQL_DESC_LITERAL_PREFIX"},
                                                    {SQL_DESC_LITERAL_SUFFIX, "SQL_DESC_LITERAL_SUFFIX"},
                                                    {SQL_DESC_LOCAL_TYPE_NAME, "SQL_DESC_LOCAL_TYPE_NAME"},
                                                    {SQL_DESC_NULLABLE, "SQL_DESC_NULLABLE"},
                                                    {SQL_DESC_NUM_PREC_RADIX, "SQL_DESC_NUM_PREC_RADIX"},
                                                    {SQL_DESC_PRECISION, "SQL_DESC_PRECISION"},
                                                    {SQL_DESC_SCALE, "SQL_DESC_SCALE"},
                                                    {SQL_DESC_SCHEMA_NAME, "SQL_DESC_SCHEMA_NAME"},
                                                    {SQL_DESC_SEARCHABLE, "SQL_DESC_SEARCHABLE"},
                                                    {SQL_DESC_TYPE, "SQL_DESC_TYPE"},
                                                    {SQL_DESC_TYPE_NAME, "SQL_DESC_TYPE_NAME"},
                                                    {SQL_DESC_UNNAMED, "SQL_DESC_UNNAMED"},
                                                    {SQL_DESC_UNSIGNED, "SQL_DESC_UNSIGNED"},
                                                    {SQL_COLUMN_SCALE, "SQL_COLUMN_SCALE"},
                                                    {SQL_COLUMN_LENGTH, "SQL_COLUMN_LENGTH"},
                                                    {SQL_COLUMN_PRECISION, "SQL_COLUMN_PRECISION"},
                                                    {SQL_DESC_UPDATABLE, "SQL_DESC_UPDATABLE"}};

void odbc_col_attribute_test::TestAllFields(SQLHANDLE hstmt, std::map<SQLLEN, ExpectedResult> expected) {
	SQLLEN n;
	SQLRETURN ret = SQLColAttribute(hstmt, 1, SQL_DESC_AUTO_UNIQUE_VALUE, nullptr, 0, nullptr, &n);
	REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
	REQUIRE(n == SQL_FALSE);

	for (auto it = expected.begin(); it != expected.end(); ++it) {
		INFO("value(" << it->first << ") --> " << sql_lookup.at(it->first));
		if (it->second.is_int) {
			if (it->second.n == SQL_NO_TOTAL && (it->first == SQL_COLUMN_SCALE || it->first == SQL_DESC_SCALE)) {
				// SQL_NO_TOTAL -> Returns SQL_SUCCESS_WITH_INFO
				ret = SQLColAttribute(hstmt, 1, it->first, nullptr, 0, nullptr, &n);
				INFO("SQLColAttribute " << ret);
				REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
				REQUIRE(n == SQL_NO_TOTAL);
				continue;
			}
			CheckInteger(hstmt, it->second.n, it->first);
		} else {
			CheckString(hstmt, it->second.s, it->first);
		}
	}
}
