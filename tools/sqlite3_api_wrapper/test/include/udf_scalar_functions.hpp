#include "sqlite3.h"
#include "udf_struct_sqlite3.h"

#include <string>

// SQLite UDF to be register on DuckDB
static void multiply10(sqlite3_context *context, int argc, sqlite3_value **argv) {
	REQUIRE(argc == 1);
	int v = sqlite3_value_int(argv[0]);
	v *= 10;
	sqlite3_result_int(context, v);
}

static void sum_cols_int(sqlite3_context *context, int argc, sqlite3_value **argv) {
	REQUIRE(argc > 0);
	auto sum = sqlite3_value_int(argv[0]);
	for (int i = 1; i < argc; ++i) {
		sum += sqlite3_value_int(argv[i]);
	}
	sqlite3_result_int(context, sum);
}

static void sum_cols_int_check_nulls(sqlite3_context *context, int argc, sqlite3_value **argv) {
	REQUIRE(argc > 0);
	auto sum = 0;
	int res_type = SQLITE_INTEGER;
	for (int i = 0; i < argc; ++i) {
		int type = sqlite3_value_type(argv[i]);
		if (type == SQLITE_NULL) {
			res_type = SQLITE_NULL;
		} else {
			sum += sqlite3_value_int(argv[i]);
		}
	}
	if (res_type == SQLITE_NULL) {
		sqlite3_result_null(context);
	} else {
		sqlite3_result_int(context, sum);
	}
}

static void sum_cols_double(sqlite3_context *context, int argc, sqlite3_value **argv) {
	REQUIRE(argc > 0);
	auto sum = sqlite3_value_double(argv[0]);
	for (int i = 1; i < argc; ++i) {
		sum += sqlite3_value_double(argv[i]);
	}
	sqlite3_result_double(context, sum);
}

static void check_text(sqlite3_context *context, int argc, sqlite3_value **argv) {
	REQUIRE(argc == 1);
	char *str = (char *)sqlite3_value_text(argv[0]);
	int len = sqlite3_value_bytes(argv[0]);
	for (int i = 0; i < len; ++i) {
		str[i] = 'T';
	}
	sqlite3_result_text(context, str, len, nullptr);
}

static void check_null_terminated_string(sqlite3_context *context, int argc, sqlite3_value **argv) {
	REQUIRE(argc == 1);
	char *str = (char *)sqlite3_value_text(argv[0]);

	// strlen expects a null-terminated string
	// otherwise, the result is undefined, it's likely to happen a 'heap-buffer-overflow'
	size_t str_len = strlen(str);

	// both length must be equal
	REQUIRE(str_len == (size_t)sqlite3_value_bytes(argv[0]));

	sqlite3_result_text(context, str, str_len, nullptr);
}

static void check_blob(sqlite3_context *context, int argc, sqlite3_value **argv) {
	REQUIRE(argc == 1);
	auto blob = (char *)sqlite3_value_blob(argv[0]);
	int len = sqlite3_value_bytes(argv[0]);
	for (int i = 0; i < len; ++i) {
		blob[i] = 'B';
	}
	sqlite3_result_blob(context, blob, len, nullptr);
}

static void check_type(sqlite3_context *context, int argc, sqlite3_value **argv) {
	REQUIRE(argc == 1);
	int type_id = sqlite3_value_type(argv[0]);
	switch (type_id) {
	case SQLITE_INTEGER: {
		auto value = sqlite3_value_int(argv[0]);
		sqlite3_result_int(context, value * 10); // value x 10
		break;
	}
	case SQLITE_FLOAT: {
		auto value = sqlite3_value_double(argv[0]);
		sqlite3_result_double(context, value * 100); // value x 100
		break;
	}
	case SQLITE_TEXT: {
		auto value = (char *)sqlite3_value_text(argv[0]);
		value[0] = 'T';
		value[1] = 'E';
		value[2] = 'X';
		value[3] = 'T';
		auto len = sqlite3_value_bytes(argv[0]);
		sqlite3_result_text(context, value, len, nullptr);
		break;
	}
	case SQLITE_BLOB: {
		auto value = sqlite3_value_blob(argv[0]);
		((char *)value)[0] = 'B';
		((char *)value)[1] = 'L';
		((char *)value)[2] = 'O';
		((char *)value)[3] = 'B';
		auto len = sqlite3_value_bytes(argv[0]);
		sqlite3_result_blob(context, value, len, nullptr);
		break;
	}
	default:
		break;
	}
}

static void set_null(sqlite3_context *context, int argc, sqlite3_value **argv) {
	REQUIRE(argc == 1);
	sqlite3_result_null(context);
}

// get user data and replace the input value
static void get_user_data(sqlite3_context *context, int argc, sqlite3_value **argv) {
	REQUIRE(argc == 1);
	auto *pData = sqlite3_user_data(context);
	if (pData) {
		char *str = (char *)sqlite3_value_text(argv[0]);
		int str_len = sqlite3_value_bytes(argv[0]);
		int undescore_idx = -1;
		// find first '_' underscore
		for (int i = 0; i < str_len; ++i) {
			if (str[i] == '_') {
				undescore_idx = i;
				break;
			}
		}
		if (undescore_idx > -1) {
			char *userData = (char *)pData;
			int u_len = strlen(userData);
			// replace from the first undescore, case there is memory space in the string
			if (u_len + undescore_idx <= str_len) {
				memcpy(str + undescore_idx, userData, u_len);
			}
		}
		sqlite3_result_text(context, str, str_len, nullptr);
	} else {
		sqlite3_result_null(context);
	}
}

// get text value from interger or float types
static void cast_numbers_to_text(sqlite3_context *context, int argc, sqlite3_value **argv) {
	REQUIRE((sqlite3_value_type(argv[0]) == SQLITE_INTEGER || sqlite3_value_type(argv[0]) == SQLITE_FLOAT));
	char *str = (char *)sqlite3_value_text(argv[0]); // argv[0] is a an integer
	REQUIRE(sqlite3_value_type(argv[0]) == SQLITE_TEXT);
	size_t len = sqlite3_value_bytes(argv[0]);
	sqlite3_result_text(context, str, len, nullptr);
}

static void cast_to_int32(sqlite3_context *context, int argc, sqlite3_value **argv) {
	// argv[0] is not a 32-bit integer, internal casting must occur
	int value = sqlite3_value_int(argv[0]);
	if (sqlite3_errcode(argv[0]->db) == SQLITE_MISMATCH) {
		sqlite3_result_null(context);
	} else {
		sqlite3_result_int(context, value);
	}
}

static void cast_to_int64(sqlite3_context *context, int argc, sqlite3_value **argv) {
	// argv[0] is not a 64-bit integer, internal casting must occur
	int64_t value = sqlite3_value_int64(argv[0]);
	if (sqlite3_errcode(argv[0]->db) == SQLITE_MISMATCH) {
		sqlite3_result_null(context);
	} else {
		sqlite3_result_int64(context, value);
	}
}

static void cast_to_float(sqlite3_context *context, int argc, sqlite3_value **argv) {
	REQUIRE(sqlite3_value_type(argv[0]) != SQLITE_FLOAT);
	// argv[0] is not a float, internal casting must occur
	double value = sqlite3_value_double(argv[0]);
	if (sqlite3_errcode(argv[0]->db) == SQLITE_MISMATCH) {
		sqlite3_result_null(context);
	} else {
		sqlite3_result_double(context, value);
	}
}

static void sum_overload_function(sqlite3_context *context, int argc, sqlite3_value **argv) {
	REQUIRE(argc > 0);
	int value1, value2, value3;
	value2 = 0;
	value3 = 0;
	value1 = sqlite3_value_int(argv[0]);
	if (argc == 2) {
		value2 = sqlite3_value_int(argv[1]);
	} else if (argc == 3) {
		value2 = sqlite3_value_int(argv[1]);
		value3 = sqlite3_value_int(argv[2]);
	}

	sqlite3_result_int(context, value1 + value2 + value3);
}

// calling sqlite3_value_text() multiple times
static void calling_value_text_multiple_times(sqlite3_context *context, int argc, sqlite3_value **argv) {
	REQUIRE((sqlite3_value_type(argv[0]) == SQLITE_INTEGER || sqlite3_value_type(argv[0]) == SQLITE_FLOAT ||
	         sqlite3_value_type(argv[0]) == SQLITE_TEXT));
	char *str = (char *)sqlite3_value_text(argv[0]);
	REQUIRE(sqlite3_value_type(argv[0]) == SQLITE_TEXT);
	auto len = strlen(str);
	size_t len2;
	char *str2;
	// calling sqlite3_value_text multiple times,  i.e., 10x
	for (size_t i = 0; i < 10; ++i) {
		str2 = (char *)sqlite3_value_text(argv[0]);
		REQUIRE(str == str2);
		len2 = strlen(str2);
		REQUIRE(len == len2);
	}
	len = sqlite3_value_bytes(argv[0]);
	sqlite3_result_text(context, str, len, nullptr);
}
