#include "../common.h"

using namespace odbc_test;

static unsigned char HexToInt(char c) {
	if (c >= '0' && c <= '9') {
		return static_cast<unsigned char>(c - '0');
	} else if (c >= 'a' && c <= 'f') {
		return static_cast<unsigned char>(c - 'a' + 10);
	} else if (c >= 'A' && c <= 'F') {
		return static_cast<unsigned char>(c - 'A' + 10);
	} else {
		FAIL("invalid hex-encoded numeric value");
		return 0;
	}
}

static void BuildNumericStruct(SQL_NUMERIC_STRUCT *numeric, unsigned char sign, const char *hexval,
                               unsigned char precision, unsigned char scale) {
	memset(numeric, 0, sizeof(SQL_NUMERIC_STRUCT));
	numeric->sign = sign;
	numeric->precision = precision;
	numeric->scale = scale;

	// Convert hexval to binary
	int len = 0;
	while (*hexval) {
		if (*hexval == ' ') {
			hexval++;
			continue;
		}
		if (len >= SQL_MAX_NUMERIC_LEN) {
			FAIL("hex-encoded numeric value too long");
		}
		numeric->val[len] = HexToInt(*hexval) << 4 | HexToInt(*(hexval + 1));
		hexval += 2;
		len++;
	}
}

static void TestNumericParams(HSTMT &hstmt, unsigned char sign, const char *hexval, unsigned char precision,
                              unsigned char scale, const std::string &expected) {
	SQL_NUMERIC_STRUCT numeric;
	BuildNumericStruct(&numeric, sign, hexval, precision, scale);

	SQLLEN numeric_len = sizeof(numeric);
	EXECUTE_AND_CHECK("SQLBindParameter (numeric)", SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_NUMERIC,
	                  SQL_NUMERIC, precision, scale, &numeric, numeric_len, &numeric_len);

	EXECUTE_AND_CHECK("SQLExecute", SQLExecute, hstmt);

	EXECUTE_AND_CHECK("SQLFetch", SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, expected);
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
}

static void TestNumericResult(HSTMT &hstmt, const char *num_str, const std::string &expected_result,
                              unsigned char expected_precision = 18, unsigned char expected_scale = 3,
                              unsigned int precision = 18, unsigned int scale = 3) {
	SQL_NUMERIC_STRUCT numeric;

	std::string query = "SELECT '" + std::string(num_str) + "'::numeric(" + std::to_string(precision) + "," +
	                    std::to_string(scale) + ")";
	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt, ConvertToSQLCHAR(query), SQL_NTS);

	EXECUTE_AND_CHECK("SQLFetch", SQLFetch, hstmt);
	EXECUTE_AND_CHECK("SQLGetData", SQLGetData, hstmt, 1, SQL_C_NUMERIC, &numeric, sizeof(numeric), nullptr);
	REQUIRE(numeric.precision == expected_precision);
	REQUIRE(numeric.scale == expected_scale);
	REQUIRE(numeric.sign == 1);
	REQUIRE(ConvertHexToString(numeric.val, expected_result.length()) == expected_result);
}

TEST_CASE("Test numeric limits and conversion", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Test 25.212 with default precision and scale
	EXECUTE_AND_CHECK("SQLPrepare (?::numeric)", SQLPrepare, hstmt, ConvertToSQLCHAR("SELECT ?::numeric"), SQL_NTS);
	TestNumericParams(hstmt, 1, "7C62", 5, 3, "25.212");

	// Test 0 (negative and positive) with precision 1 and scale 0
	EXECUTE_AND_CHECK("SQLPrepare (?::numeric(1,0))", SQLPrepare, hstmt, ConvertToSQLCHAR("SELECT ?::numeric(1,0)"),
	                  SQL_NTS);
	TestNumericParams(hstmt, 1, "00", 1, 0, "0");
	TestNumericParams(hstmt, 0, "00", 1, 0, "0");

	// Test 7.70 with precision 3 and scale 2
	EXECUTE_AND_CHECK("SQLPrepare (?::numeric(3,2))", SQLPrepare, hstmt, ConvertToSQLCHAR("SELECT ?::numeric(3,2)"),
	                  SQL_NTS);
	TestNumericParams(hstmt, 1, "0203", 3, 2, "7.70");

	// Test 12345678901234567890123456789012345678 with precision 38 and scale 0
	EXECUTE_AND_CHECK("SQLPrepare (?::numeric(38,0))", SQLPrepare, hstmt, ConvertToSQLCHAR("SELECT ?::numeric(38,0)"),
	                  SQL_NTS);
	TestNumericParams(hstmt, 1, "4EF338DE509049C4133302F0F6B04909", 38, 0, "12345678901234567890123456789012345678");

	// Test setting numeric struct within the application
	TestNumericResult(hstmt, "25.212", "7C62000000000000", 5, 3);
	TestNumericResult(hstmt, "24197857161011715162171839636988778104", "7856341278563412", 38, 0, 38, 0);
	TestNumericResult(hstmt, "12345678901234567890123456789012345678", "4EF338DE509049C4", 38, 0, 38, 0);
	TestNumericResult(hstmt, "-0", "0000000000000000", 1, 3);
	TestNumericResult(hstmt, "0", "0000000000000000", 1, 3);
	TestNumericResult(hstmt, "7.70", "0203000000000000", 3, 2, 3, 2);
	TestNumericResult(hstmt, "999999999999", "FF0FA5D4E8000000", 12, 3);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
