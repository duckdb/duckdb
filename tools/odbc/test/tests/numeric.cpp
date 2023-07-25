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
	unsigned char precision, unsigned char scale)
{
	numeric->sign = sign;
	numeric->precision = precision;
	numeric->scale = scale;

	memset(numeric, 0, sizeof(SQL_NUMERIC_STRUCT));

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

static void TestNumericParams(HSTMT &hstmt, unsigned char sign, const char *hexval,
	unsigned char precision, unsigned char scale)
{
	SQL_NUMERIC_STRUCT numeric;
	BuildNumericStruct(&numeric, sign, hexval, precision, scale);

	SQLLEN numeric_len = sizeof(numeric);
	EXECUTE_AND_CHECK("SQLBindParameter (numeric)", SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_NUMERIC, SQL_NUMERIC, precision, scale, &numeric, numeric_len, &numeric_len);

	EXECUTE_AND_CHECK("SQLExecute", SQLExecute, hstmt);

	DATA_CHECK(hstmt, 1, ConvertToString(numeric.val));
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
}

TEST_CASE("Test numeric limits and conversion", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	EXECUTE_AND_CHECK("SQLPrepare", SQLPrepare, hstmt, ConvertToSQLCHAR("SELECT ?::numeric"), SQL_NTS);

	TestNumericParams(hstmt, 1, "7C62", 5, 3);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
