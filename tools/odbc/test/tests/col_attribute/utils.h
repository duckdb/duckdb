#ifndef DUCKDB_UTILS_H
#define DUCKDB_UTILS_H

#include "../../common.h"

using namespace odbc_test;

namespace odbc_col_attribute_test {
class ExpectedResult {
public:
	std::string s;
	SQLLEN n;
	bool is_int;

	ExpectedResult() {};

	explicit ExpectedResult(SQLLEN n_n) : n(n_n), is_int(true) {};

	explicit ExpectedResult(const std::string &n_s) : s(n_s), is_int(false) {};

	ExpectedResult(const ExpectedResult &other) {
		*this = other;
	}
};

/*
 * @brief Executes SQLColAttribute, checks the result, and compares it to the expected string
 */
void CheckString(SQLHANDLE handle, const std::string &expected, SQLSMALLINT field_identifier);

/*
 * @brief Executes SQLColAttribute, checks the result, and compares it to the expected integer
 */
void CheckInteger(SQLHANDLE handle, SQLLEN expected, SQLSMALLINT field_identifier);

/*
 * @brief Executes SQLColAttribute and checks that it returns SQL_ERROR
 */
void ExpectError(SQLHANDLE handle, SQLSMALLINT field_identifier);

/*
 * @brief Executes SQLColAttribute for all fields and checks the result
 */
void TestAllFields(SQLHANDLE hstmt, std::map<SQLLEN, ExpectedResult> expected);

} // namespace odbc_col_attribute_test

#endif // DUCKDB_UTILS_H
