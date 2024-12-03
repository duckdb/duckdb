#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/logging/logger.hpp"

using namespace duckdb;
using namespace std;

template <class SOURCE, typename FUN>
void LogSimple(SOURCE &src, FUN f) {
	f(src, "log-a-lot: 'simple'");
}

template <class SOURCE, typename FUN>
void LogFormatString(SOURCE &src, FUN f) {
	f(src, "log-a-lot: '%s'", "format");
}

template <class SOURCE, typename FUN>
void LogCallback(SOURCE &src, FUN f) {
	f(src, []() {
		return "log-a-lot: 'callback'";
	});
}

template <class SOURCE, typename FUN>
void LogSimpleCustomType(SOURCE &src, FUN f) {
	f("custom_type", src, "log-a-lot: 'simple with type'");
}

template <class SOURCE, typename FUN>
void LogFormatStringCustomType(SOURCE &src, FUN f) {
	f("custom_type", src, "log-a-lot: '%s'", "format with type");
}

template <class SOURCE, typename FUN>
void LogCallbackCustomType(SOURCE &src, FUN f) {
	f("custom_type", src, []() {
		return "log-a-lot: 'callback with type'";
	});
}

#define TEST_ALL_LOG_TEMPLATES(FUNCTION, SOURCE, SOURCE_TYPE) \
	LogSimple<SOURCE_TYPE, void (*)(SOURCE_TYPE&, const char*)>(SOURCE, &FUNCTION); \
	LogFormatString<SOURCE_TYPE, void (*)(SOURCE_TYPE&, const char*, const char*)>(SOURCE, &FUNCTION); \
	LogCallback<SOURCE_TYPE, void (*)(SOURCE_TYPE&, std::function<string()>)>(SOURCE, &FUNCTION); \
	LogSimpleCustomType<SOURCE_TYPE, void (*)(const char*, SOURCE_TYPE&, const char*)>(SOURCE, &FUNCTION); \
	LogFormatStringCustomType<SOURCE_TYPE, void (*)(const char*, SOURCE_TYPE&, const char*, const char*)>(SOURCE, &FUNCTION); \
	LogCallbackCustomType<SOURCE_TYPE, void (*)(const char*, SOURCE_TYPE&, std::function<string()>)>(SOURCE, &FUNCTION); \

TEST_CASE("Test logging level", "[logging][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("set enable_logging=true;"));
	REQUIRE_NO_FAIL(con.Query("set logging_level='debugging';"));

	// Log all to global logger
	TEST_ALL_LOG_TEMPLATES(Logger::Debug, *db.instance, DatabaseInstance);
	TEST_ALL_LOG_TEMPLATES(Logger::Info, *db.instance, DatabaseInstance);
	TEST_ALL_LOG_TEMPLATES(Logger::Warn, *db.instance, DatabaseInstance);
	TEST_ALL_LOG_TEMPLATES(Logger::Error, *db.instance, DatabaseInstance);
	TEST_ALL_LOG_TEMPLATES(Logger::Fatal, *db.instance, DatabaseInstance);

	// Log all to client context logger
	TEST_ALL_LOG_TEMPLATES(Logger::Debug, *con.context, ClientContext);
	TEST_ALL_LOG_TEMPLATES(Logger::Info, *con.context, ClientContext);
	TEST_ALL_LOG_TEMPLATES(Logger::Warn, *con.context, ClientContext);
	TEST_ALL_LOG_TEMPLATES(Logger::Error, *con.context, ClientContext);
	TEST_ALL_LOG_TEMPLATES(Logger::Fatal, *con.context, ClientContext);

	// TODO: test thread logger

	// Generate expected log messages
	duckdb::vector<Value> default_types = {"global_logger", "client_context"};
	duckdb::vector<Value> log_levels = {"DEBUGGING", "INFO", "WARN", "ERROR", "FATAL"};
	duckdb::vector<Value> expected_types;
	duckdb::vector<Value> expected_messages;
	duckdb::vector<Value> expected_log_levels;
	idx_t num_runs = 2;
	idx_t num_log_levels = 5;
	idx_t num_functions = 6;

	for (idx_t i = 0; i < num_runs; i++) {
		for (idx_t j = 0; j < num_log_levels; j++) {
			for (idx_t k = 0; k < num_functions; k++) {
				expected_log_levels.push_back(log_levels[j]);
			}
			expected_messages.push_back("log-a-lot: 'simple'");
			expected_messages.push_back("log-a-lot: 'format'");
			expected_messages.push_back("log-a-lot: 'callback'");
			expected_messages.push_back("log-a-lot: 'simple with type'");
			expected_messages.push_back("log-a-lot: 'format with type'");
			expected_messages.push_back("log-a-lot: 'callback with type'");

			expected_types.push_back(default_types[i]);
			expected_types.push_back(default_types[i]);
			expected_types.push_back(default_types[i]);
			expected_types.push_back("custom_type");
			expected_types.push_back("custom_type");
			expected_types.push_back("custom_type");
		}
	}

	auto res = con.Query("SELECT type, log_level, message, from duckdb_logs where starts_with(message, 'log-a-lot:')");
	REQUIRE(CHECK_COLUMN(res, 0, expected_types));
	REQUIRE(CHECK_COLUMN(res, 1, expected_log_levels));
	REQUIRE(CHECK_COLUMN(res, 2, expected_messages));
}