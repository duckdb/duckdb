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
	EXECUTE_AND_CHECK("SQLColAttribute", SQLColAttribute, handle, 1, field_identifier, nullptr, 0, nullptr, &number);
	REQUIRE(number == expected);
}

void odbc_col_attribute_test::ExpectError(SQLHANDLE handle, SQLSMALLINT field_identifier) {
	SQLRETURN ret = SQLColAttribute(handle, 1, field_identifier, nullptr, sizeof(nullptr), nullptr, nullptr);
	REQUIRE(ret == SQL_ERROR);
}

void odbc_col_attribute_test::TestAllFields(SQLHANDLE hstmt, std::map<SQLLEN, ExpectedResult> expected) {
	SQLLEN n;
	SQLRETURN ret = SQLColAttribute(hstmt, 1, SQL_DESC_AUTO_UNIQUE_VALUE, nullptr, 0, nullptr, &n);
	REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
	REQUIRE(n == SQL_FALSE);

	for (auto it = expected.begin(); it != expected.end(); ++it) {
		if (it->second.is_int) {
			if (it->second.n == SQL_NO_TOTAL && (it->first == SQL_COLUMN_SCALE || it->first == SQL_DESC_SCALE)) {
				// SQL_NO_TOTAL -> Returns SQL_SUCCESS_WITH_INFO
				ret = SQLColAttribute(hstmt, 1, it->first, nullptr, 0, nullptr, &n);
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
