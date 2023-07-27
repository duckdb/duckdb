#include "../common.h"

using namespace odbc_test;

#define EXPECTED_ERROR "error"
#define EPSILON        0.0000001

static const std::vector<std::vector<std::string>> all_types = {{"boolean", "true"},
                                                                {"bytea", "\\x464F4F"},
                                                                {"char", "x"},
                                                                {"int8", "1234567890"},
                                                                {"int2", "12345"},
                                                                {"int4", "1234567"},
                                                                {"text", "textdata"},
                                                                {"float4", "1.234"},
                                                                {"double", "1.23456789012"},
                                                                {"varchar", "foobar"},
                                                                {"date", "2011-02-13"},
                                                                {"time", "13:23:34"},
                                                                {"timestamp", "2011-02-15 15:49:18"},
                                                                {"interval", "10 years -11 months -12 days 13:14:00"},
                                                                {"numeric", "1234.567890"}};

static const std::vector<SQLINTEGER> all_sql_types = {SQL_C_CHAR,
                                                      SQL_C_WCHAR,
                                                      SQL_C_SSHORT,
                                                      SQL_C_USHORT,
                                                      SQL_C_SLONG,
                                                      SQL_C_ULONG,
                                                      SQL_C_FLOAT,
                                                      SQL_C_DOUBLE,
                                                      SQL_C_BIT,
                                                      SQL_C_STINYINT,
                                                      SQL_C_UTINYINT,
                                                      SQL_C_SBIGINT,
                                                      SQL_C_UBIGINT,
                                                      SQL_C_BINARY,
                                                      SQL_C_BOOKMARK,
                                                      SQL_C_VARBOOKMARK,
                                                      SQL_C_TYPE_DATE,
                                                      SQL_C_TYPE_TIME,
                                                      SQL_C_TYPE_TIMESTAMP,
                                                      SQL_C_NUMERIC,
                                                      SQL_C_GUID,
                                                      SQL_C_INTERVAL_YEAR,
                                                      SQL_C_INTERVAL_MONTH,
                                                      SQL_C_INTERVAL_DAY,
                                                      SQL_C_INTERVAL_HOUR,
                                                      SQL_C_INTERVAL_MINUTE,
                                                      SQL_C_INTERVAL_SECOND,
                                                      SQL_C_INTERVAL_YEAR_TO_MONTH,
                                                      SQL_C_INTERVAL_DAY_TO_HOUR,
                                                      SQL_C_INTERVAL_DAY_TO_MINUTE,
                                                      SQL_C_INTERVAL_DAY_TO_SECOND,
                                                      SQL_C_INTERVAL_HOUR_TO_MINUTE,
                                                      SQL_C_INTERVAL_HOUR_TO_SECOND,
                                                      SQL_C_INTERVAL_MINUTE_TO_SECOND};

static const std::vector<std::vector<std::string>> results = {
    {
        "true",
        "true",
        "1",
        "1",
        "1",
        "1",
        "1.000000",
        "1.000000",
        EXPECTED_ERROR,
        "1",
        "1",
        "1",
        "1",
        "74727565",
        "1",
        "74727565",
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
    },
    {"F4F4F",        "F4F4F",        EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, "4634463446",
     EXPECTED_ERROR, "4634463446",   EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR},
    {"x",
     "x",
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     "78",
     EXPECTED_ERROR,
     "78",
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR},
    {"1234567890",           "1234567890",        EXPECTED_ERROR,      EXPECTED_ERROR,         "1234567890",
     "1234567890",           "1234567936.000000", "1234567890.000000", EXPECTED_ERROR,         EXPECTED_ERROR,
     EXPECTED_ERROR,         "1234567890",        "1234567890",        "31323334353637383930", "1234567890",
     "31323334353637383930", EXPECTED_ERROR,      EXPECTED_ERROR,      EXPECTED_ERROR,         EXPECTED_ERROR,
     EXPECTED_ERROR,         EXPECTED_ERROR,      EXPECTED_ERROR,      EXPECTED_ERROR,         EXPECTED_ERROR,
     EXPECTED_ERROR,         EXPECTED_ERROR,      EXPECTED_ERROR,      EXPECTED_ERROR,         EXPECTED_ERROR,
     EXPECTED_ERROR,         EXPECTED_ERROR,      EXPECTED_ERROR,      EXPECTED_ERROR},
    {"12345",        "12345",        "12345",        "12345",        "12345",        "12345",        "12345.000000",
     "12345.000000", EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, "12345",        "12345",        "3132333435",
     "12345",        "3132333435",   EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR},
    {"1234567",        "1234567",        EXPECTED_ERROR, EXPECTED_ERROR,   "1234567",      "1234567",
     "1234567.000000", "1234567.000000", EXPECTED_ERROR, EXPECTED_ERROR,   EXPECTED_ERROR, "1234567",
     "1234567",        "31323334353637", "1234567",      "31323334353637", EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR,   EXPECTED_ERROR,   EXPECTED_ERROR, EXPECTED_ERROR,   EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR,   EXPECTED_ERROR,   EXPECTED_ERROR, EXPECTED_ERROR,   EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR,   EXPECTED_ERROR,   EXPECTED_ERROR, EXPECTED_ERROR},
    {"textdata",     "textdata",         EXPECTED_ERROR, EXPECTED_ERROR,     EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR,     EXPECTED_ERROR, EXPECTED_ERROR,     EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, "7465787464617461", EXPECTED_ERROR, "7465787464617461", EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR,     EXPECTED_ERROR, EXPECTED_ERROR,     EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR,     EXPECTED_ERROR, EXPECTED_ERROR,     EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR,     EXPECTED_ERROR, EXPECTED_ERROR},
    {
        "1.234",
        "1.234",
        "1",
        "1",
        "1",
        "1",
        "1.234000",
        "1.2339999676",
        EXPECTED_ERROR,
        "1",
        "1",
        "1",
        "1",
        "312E323334",
        "1",
        "312E323334",
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
        EXPECTED_ERROR,
    },
    {"1.23456789012",
     "1.23456789012",
     "1",
     "1",
     "1",
     "1",
     "1.234568",
     "1.234568",
     EXPECTED_ERROR,
     "1",
     "1",
     "1",
     "1",
     "312E3233343536373839303132",
     "1",
     "312E3233343536373839303132",
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR},
    {"foobar",       "foobar",       EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, "666F6F626172",
     EXPECTED_ERROR, "666F6F626172", EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR},
    {"2011-02-13",           "2011-02-13",   EXPECTED_ERROR, EXPECTED_ERROR,         EXPECTED_ERROR,
     EXPECTED_ERROR,         EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR,         EXPECTED_ERROR,
     EXPECTED_ERROR,         EXPECTED_ERROR, EXPECTED_ERROR, "323031312D30322D3133", EXPECTED_ERROR,
     "323031312D30322D3133", "2011-2-13",    EXPECTED_ERROR, "2011-2-13-0-0-0-0",    EXPECTED_ERROR,
     EXPECTED_ERROR,         EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR,         EXPECTED_ERROR,
     EXPECTED_ERROR,         EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR,         EXPECTED_ERROR,
     EXPECTED_ERROR,         EXPECTED_ERROR, EXPECTED_ERROR, EXPECTED_ERROR},
    {"13:23:34",     "13:23:34",         EXPECTED_ERROR, EXPECTED_ERROR,     EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR,     EXPECTED_ERROR, EXPECTED_ERROR,     EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, "31333A32333A3334", EXPECTED_ERROR, "31333A32333A3334", EXPECTED_ERROR, "13-23-34",
     EXPECTED_ERROR, EXPECTED_ERROR,     EXPECTED_ERROR, EXPECTED_ERROR,     EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR,     EXPECTED_ERROR, EXPECTED_ERROR,     EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR,     EXPECTED_ERROR, EXPECTED_ERROR},
    {"2011-02-15 15:49:18",
     "2011-02-15 15:49:18",
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     "323031312D30322D31352031353A34393A3138",
     EXPECTED_ERROR,
     "323031312D30322D31352031353A34393A3138",
     "2011-2-15",
     "15-49-18",
     "2011-2-15-15-49-18-0",
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR},
    {"9 years 1 month -12 days 13:14:00",
     "9 years 1 month -12 days 13:14:00",
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     "392079656172732031206D6F6E7468202D313220646179732031333A31343A3030",
     EXPECTED_ERROR,
     "392079656172732031206D6F6E7468202D313220646179732031333A31343A3030",
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     EXPECTED_ERROR,
     "0-9",
     "0-109",
     "0-3258",
     "0-78205",
     "0-4692314",
     "0-281538840",
     "0-9-1",
     "0-3258-13",
     "0-3258-13-14",
     "0-3258-13-14-0-0",
     "0-78205-14",
     "0-78205-14-0-0",
     "0-4692314-0-0"},
    {"1234.568",     "1234.568",
     "1235",         "1235",
     "1235",         "1235",
     "1234.567993",  "1234.568000",
     EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, "1235",
     "1235",         "313233342E353638",
     "1235",         "313233342E353638",
     EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, "7:3:1:88D61200000000000000000000000000",
     EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR,
     EXPECTED_ERROR, EXPECTED_ERROR},
};

static std::vector<std::string> Split(std::string text, char delim) {
	std::string line;
	std::vector<std::string> vec;
	std::stringstream ss(text);
	while (std::getline(ss, line, delim)) {
		vec.push_back(line);
	}
	return vec;
}

static void ConvertToTypes(SQLINTEGER sql_type, void *result, const std::string &expected_result) {
	switch (sql_type) {
	case SQL_C_CHAR:
		REQUIRE(STR_EQUAL(static_cast<char *>(result), expected_result.c_str()));
		return;
	case SQL_C_WCHAR: {
		WCHAR *wresult = static_cast<WCHAR *>(result);
		for (size_t i = 0; i < expected_result.size(); i++) {
			REQUIRE(wresult[i] == static_cast<WCHAR>(expected_result[i]));
		}
		return;
	}
	case SQL_C_SSHORT:
		REQUIRE(*static_cast<short *>(result) == static_cast<short>(std::stoi(expected_result)));
		return;
	case SQL_C_USHORT:
		REQUIRE(*static_cast<unsigned short *>(result) == static_cast<unsigned short>(std::stoul(expected_result)));
		return;
	case SQL_C_SLONG:
		REQUIRE(*static_cast<SQLINTEGER *>(result) == static_cast<SQLINTEGER>(std::stoi(expected_result)));
		return;
	case SQL_C_ULONG:
		REQUIRE(*static_cast<SQLUINTEGER *>(result) == static_cast<SQLUINTEGER>(std::stoul(expected_result)));
		return;
	case SQL_C_FLOAT:
		REQUIRE(*static_cast<SQLREAL *>(result) ==
		        Approx(static_cast<SQLREAL>(std::stof(expected_result))).margin(EPSILON));
		return;
	case SQL_C_DOUBLE:
		REQUIRE(*static_cast<SQLDOUBLE *>(result) ==
		        Approx(static_cast<SQLDOUBLE>(std::stod(expected_result))).margin(EPSILON));
		return;
	case SQL_C_BIT:
		REQUIRE(*static_cast<unsigned char *>(result) == static_cast<unsigned char>(std::stoi(expected_result)));
		return;
	case SQL_C_STINYINT:
		REQUIRE(*static_cast<signed char *>(result) == static_cast<signed char>(std::stoi(expected_result)));
		return;
	case SQL_C_UTINYINT:
		REQUIRE(*static_cast<unsigned char *>(result) == static_cast<unsigned char>(std::stoul(expected_result)));
		return;
	case SQL_C_SBIGINT:
		REQUIRE(*static_cast<SQLBIGINT *>(result) == static_cast<SQLBIGINT>(std::stoll(expected_result)));
		return;
	case SQL_C_UBIGINT:
		REQUIRE(*static_cast<SQLUBIGINT *>(result) == static_cast<SQLUBIGINT>(std::stoull(expected_result)));
		return;
	case SQL_C_BINARY:
		REQUIRE(ConvertHexToString(static_cast<unsigned char *>(result), expected_result.length()) == expected_result);
		return;
	case SQL_C_TYPE_DATE: {
		auto *date = static_cast<SQL_DATE_STRUCT *>(result);
		std::vector<std::string> split_expected_result = Split(expected_result, '-');
		REQUIRE(date->year == std::stoi(split_expected_result[0]));
		REQUIRE(date->month == std::stoi(split_expected_result[1]));
		REQUIRE(date->day == std::stoi(split_expected_result[2]));
		return;
	}
	case SQL_C_TYPE_TIME: {
		auto *time = static_cast<SQL_TIME_STRUCT *>(result);
		std::vector<std::string> split_expected_result = Split(expected_result, '-');
		REQUIRE(time->hour == std::stoi(split_expected_result[0]));
		REQUIRE(time->minute == std::stoi(split_expected_result[1]));
		REQUIRE(time->second == std::stoi(split_expected_result[2]));
		return;
	}
	case SQL_C_TYPE_TIMESTAMP: {
		auto *timestamp = static_cast<SQL_TIMESTAMP_STRUCT *>(result);
		std::vector<std::string> split_expected_result = Split(expected_result, '-');
		REQUIRE(timestamp->year == std::stoi(split_expected_result[0]));
		REQUIRE(timestamp->month == std::stoi(split_expected_result[1]));
		REQUIRE(timestamp->day == std::stoi(split_expected_result[2]));
		REQUIRE(timestamp->hour == std::stoi(split_expected_result[3]));
		REQUIRE(timestamp->minute == std::stoi(split_expected_result[4]));
		REQUIRE(timestamp->second == std::stoi(split_expected_result[5]));
		REQUIRE(timestamp->fraction == std::stoi(split_expected_result[6]));
		return;
	}
	case SQL_C_NUMERIC: {
		auto *numeric = static_cast<SQL_NUMERIC_STRUCT *>(result);
		std::vector<std::string> split_expected_result = Split(expected_result, ':');
		REQUIRE(numeric->precision == std::stoi(split_expected_result[0]));
		REQUIRE(numeric->scale == std::stoi(split_expected_result[1]));
		REQUIRE(numeric->sign == std::stoi(split_expected_result[2]));
		REQUIRE(ConvertHexToString(numeric->val, split_expected_result[3].length()) == split_expected_result[3]);
		return;
	}
	case SQL_C_GUID: {
		// This one never gets called because it never succeeds
		return;
	}
	case SQL_C_INTERVAL_YEAR:
	case SQL_C_INTERVAL_MONTH:
	case SQL_C_INTERVAL_DAY:
	case SQL_C_INTERVAL_HOUR:
	case SQL_C_INTERVAL_MINUTE:
	case SQL_C_INTERVAL_SECOND:
	case SQL_C_INTERVAL_YEAR_TO_MONTH:
	case SQL_C_INTERVAL_DAY_TO_HOUR:
	case SQL_C_INTERVAL_DAY_TO_MINUTE:
	case SQL_C_INTERVAL_DAY_TO_SECOND:
	case SQL_C_INTERVAL_HOUR_TO_MINUTE:
	case SQL_C_INTERVAL_HOUR_TO_SECOND:
	case SQL_C_INTERVAL_MINUTE_TO_SECOND: {
		auto *interval = static_cast<SQL_INTERVAL_STRUCT *>(result);
		std::vector<std::string> split_expected_result = Split(expected_result, '-');
		REQUIRE(interval->interval_sign == std::stoi(split_expected_result[0]));
		switch (interval->interval_type) {
		case SQL_IS_YEAR:
			REQUIRE(interval->intval.year_month.year == std::stoi(split_expected_result[1]));
			break;
		case SQL_IS_MONTH:
			REQUIRE(interval->intval.year_month.month == std::stoi(split_expected_result[1]));
			break;
		case SQL_IS_DAY:
			REQUIRE(interval->intval.day_second.day == std::stoi(split_expected_result[1]));
			break;
		case SQL_IS_HOUR:
			REQUIRE(interval->intval.day_second.hour == std::stoi(split_expected_result[1]));
			break;
		case SQL_IS_MINUTE:
			REQUIRE(interval->intval.day_second.minute == std::stoi(split_expected_result[1]));
			break;
		case SQL_IS_SECOND:
			REQUIRE(interval->intval.day_second.second == std::stoi(split_expected_result[1]));
			break;
		case SQL_IS_YEAR_TO_MONTH:
			REQUIRE(interval->intval.year_month.year == std::stoi(split_expected_result[1]));
			REQUIRE(interval->intval.year_month.month == std::stoi(split_expected_result[2]));
			break;
		case SQL_IS_DAY_TO_HOUR:
			REQUIRE(interval->intval.day_second.day == std::stoi(split_expected_result[1]));
			REQUIRE(interval->intval.day_second.hour == std::stoi(split_expected_result[2]));
			break;
		case SQL_IS_DAY_TO_MINUTE:
			REQUIRE(interval->intval.day_second.day == std::stoi(split_expected_result[1]));
			REQUIRE(interval->intval.day_second.hour == std::stoi(split_expected_result[2]));
			REQUIRE(interval->intval.day_second.minute == std::stoi(split_expected_result[3]));
			break;
		case SQL_IS_DAY_TO_SECOND:
			REQUIRE(interval->intval.day_second.day == std::stoi(split_expected_result[1]));
			REQUIRE(interval->intval.day_second.hour == std::stoi(split_expected_result[2]));
			REQUIRE(interval->intval.day_second.minute == std::stoi(split_expected_result[3]));
			REQUIRE(interval->intval.day_second.second == std::stoi(split_expected_result[4]));
			REQUIRE(interval->intval.day_second.fraction == std::stoi(split_expected_result[5]));
			break;
		case SQL_IS_HOUR_TO_MINUTE:
			REQUIRE(interval->intval.day_second.hour == std::stoi(split_expected_result[1]));
			REQUIRE(interval->intval.day_second.minute == std::stoi(split_expected_result[2]));
			break;
		case SQL_IS_HOUR_TO_SECOND:
			REQUIRE(interval->intval.day_second.hour == std::stoi(split_expected_result[1]));
			REQUIRE(interval->intval.day_second.minute == std::stoi(split_expected_result[2]));
			REQUIRE(interval->intval.day_second.second == std::stoi(split_expected_result[3]));
			REQUIRE(interval->intval.day_second.fraction == std::stoi(split_expected_result[4]));
			break;
		case SQL_IS_MINUTE_TO_SECOND:
			REQUIRE(interval->intval.day_second.minute == std::stoi(split_expected_result[1]));
			REQUIRE(interval->intval.day_second.second == std::stoi(split_expected_result[2]));
			REQUIRE(interval->intval.day_second.fraction == std::stoi(split_expected_result[3]));
			break;
		default:
			FAIL("Unknown interval type");
		}
		return;
	}
	default:
		FAIL("Unknown type");
	}
}

static void ConvertAndCheck(HSTMT &hstmt, const std::string &type, const std::string &type_to_convert,
                            SQLINTEGER sql_type, const std::string &expected_result, int content_size = 256) {
	std::string query = "SELECT $$" + type_to_convert + "$$::" + type;

	EXECUTE_AND_CHECK(query.c_str(), SQLExecDirect, hstmt, ConvertToSQLCHAR(query), SQL_NTS);

	EXECUTE_AND_CHECK("SQLFetch", SQLFetch, hstmt);
	SQLCHAR content[256];
	SQLLEN content_len;
	SQLRETURN ret = SQLGetData(hstmt, 1, sql_type, content, content_size, &content_len);
	if (expected_result == EXPECTED_ERROR) {
		REQUIRE(ret == SQL_ERROR);
		return;
	}
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		FAIL("SQLGetData failed");
	}
	ConvertToTypes(sql_type, content, expected_result);

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
}

TEST_CASE("Test converting using SQLGetData", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	for (int type_index = 0; type_index < all_types.size(); type_index++) {
		for (int sql_type_index = 0; sql_type_index < all_sql_types.size(); sql_type_index++) {
			ConvertAndCheck(hstmt, all_types[type_index][0], all_types[type_index][1], all_sql_types[sql_type_index],
			                results[type_index][sql_type_index]);
		}
	}

	// Conversion to GUID throws error if the string is not of correct form
	ConvertAndCheck(hstmt, "text", "543c5e21-435a-440b-943c-64af1ad571f1", SQL_C_GUID, EXPECTED_ERROR);

	// Test for truncations
	ConvertAndCheck(hstmt, "text", "foobar", SQL_C_CHAR, "foob", 5);
	ConvertAndCheck(hstmt, "text", "foobar", SQL_C_CHAR, "fooba", 6);
	ConvertAndCheck(hstmt, "text", "foobar", SQL_C_CHAR, "foobar", 7);

	ConvertAndCheck(hstmt, "text", "foobar", SQL_C_WCHAR, "foob", 10);
	ConvertAndCheck(hstmt, "text", "foobar", SQL_C_WCHAR, "foob", 11);
	ConvertAndCheck(hstmt, "text", "foobar", SQL_C_WCHAR, "fooba", 12);
	ConvertAndCheck(hstmt, "text", "foobar", SQL_C_WCHAR, "fooba", 13);
	ConvertAndCheck(hstmt, "text", "foobar", SQL_C_WCHAR, "foobar", 14);

	ConvertAndCheck(hstmt, "text", "", SQL_C_CHAR, "");

	// Test different timestamp subtype conversions
	// Timestamp -> Date
	ConvertAndCheck(hstmt, "timestamp_s", "2021-07-15 12:30:00", SQL_C_TYPE_DATE, "2021-7-15");
	ConvertAndCheck(hstmt, "timestamp_ms", "2021-07-15 12:30:00", SQL_C_TYPE_DATE, "2021-7-15");
	ConvertAndCheck(hstmt, "timestamp", "2021-07-15 12:30:00", SQL_C_TYPE_DATE, "2021-7-15");
	ConvertAndCheck(hstmt, "timestamp_ns", "2021-07-15 12:30:00", SQL_C_TYPE_DATE, "2021-7-15");

	// TIMESTAMP -> TIME
	ConvertAndCheck(hstmt, "timestamp_s", "2021-07-15 12:30:00", SQL_C_TYPE_TIME, "12-30-0");
	ConvertAndCheck(hstmt, "timestamp_ms", "2021-07-15 12:30:00", SQL_C_TYPE_TIME, "12-30-0");
	ConvertAndCheck(hstmt, "timestamp", "2021-07-15 12:30:00", SQL_C_TYPE_TIME, "12-30-0");
	ConvertAndCheck(hstmt, "timestamp_ns", "2021-07-15 12:30:00", SQL_C_TYPE_TIME, "12-30-0");

	// TIMESTAMP -> TIMESTAMP
	ConvertAndCheck(hstmt, "timestamp_s", "2021-07-15 12:30:00", SQL_C_TYPE_TIMESTAMP, "2021-7-15-12-30-0-0");
	ConvertAndCheck(hstmt, "timestamp_ms", "2021-07-15 12:30:00", SQL_C_TYPE_TIMESTAMP, "2021-7-15-12-30-0-0");
	ConvertAndCheck(hstmt, "timestamp", "2021-07-15 12:30:00", SQL_C_TYPE_TIMESTAMP, "2021-7-15-12-30-0-0");
	ConvertAndCheck(hstmt, "timestamp_ns", "2021-07-15 12:30:00", SQL_C_TYPE_TIMESTAMP, "2021-7-15-12-30-0-0");

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
