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


// Tests all Logger function entrypoints at the specified log level with the specified enabled/disabled loggers
void test_logging(const string &minimum_level, const string &enabled_loggers, const string &disabled_loggers) {
	DuckDB db(nullptr);
	Connection con(db);

	duckdb::vector<Value> default_types = {"default", "default"};
	duckdb::vector<string> log_levels = {"DEBUGGING", "INFO", "WARN", "ERROR", "FATAL"};
	auto minimum_level_index = std::find(log_levels.begin(), log_levels.end(), minimum_level) - log_levels.begin();

	REQUIRE_NO_FAIL(con.Query("set enable_logging=true;"));
	REQUIRE_NO_FAIL(con.Query("set logging_level='" + minimum_level + """';"));
	if (!enabled_loggers.empty()) {
		REQUIRE_NO_FAIL(con.Query("set enabled_loggers='" + enabled_loggers + """';"));
		REQUIRE_NO_FAIL(con.Query("set logging_mode='enable_selected';"));
	}
	if (!disabled_loggers.empty()) {
		REQUIRE_NO_FAIL(con.Query("set disabled_loggers='" + disabled_loggers + """';"));
		REQUIRE_NO_FAIL(con.Query("set logging_mode='disable_selected';"));
	}

	bool level_only = (enabled_loggers.empty() && disabled_loggers.empty());
	bool enabled_mode = !enabled_loggers.empty();
	bool disabled_mode = !disabled_loggers.empty();

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

	// Generate expected log messages
	duckdb::vector<Value> expected_types;
	duckdb::vector<Value> expected_messages;
	duckdb::vector<Value> expected_log_levels;
	idx_t num_runs = 2;
	idx_t num_log_levels = 5;

	for (idx_t i = 0; i < num_runs; i++) {
		for (idx_t j = minimum_level_index; j < num_log_levels; j++) {
			if (level_only ||
				(enabled_mode && enabled_loggers.find(default_types[i].ToString()) != enabled_loggers.npos ) ||
				(disabled_mode && disabled_loggers.find(default_types[i].ToString()) == enabled_loggers.npos )) {
				expected_messages.push_back("log-a-lot: 'simple'");
				expected_messages.push_back("log-a-lot: 'format'");
				expected_messages.push_back("log-a-lot: 'callback'");
				expected_types.push_back(default_types[i]);
				expected_types.push_back(default_types[i]);
				expected_types.push_back(default_types[i]);
				expected_log_levels.push_back(Value(log_levels[j]));
				expected_log_levels.push_back(Value(log_levels[j]));
				expected_log_levels.push_back(Value(log_levels[j]));
			}

			if (level_only ||
				(enabled_mode && enabled_loggers.find("custom_type") != enabled_loggers.npos ) ||
				(disabled_mode && disabled_loggers.find("custom_type") == enabled_loggers.npos )) {
				expected_messages.push_back("log-a-lot: 'simple with type'");
				expected_messages.push_back("log-a-lot: 'format with type'");
				expected_messages.push_back("log-a-lot: 'callback with type'");
				expected_types.push_back("custom_type");
				expected_types.push_back("custom_type");
				expected_types.push_back("custom_type");
				expected_log_levels.push_back(Value(log_levels[j]));
				expected_log_levels.push_back(Value(log_levels[j]));
				expected_log_levels.push_back(Value(log_levels[j]));
			}
		}
	}

	auto res = con.Query("SELECT type, log_level, message, from duckdb_logs where starts_with(message, 'log-a-lot:')");
	REQUIRE(CHECK_COLUMN(res, 0, expected_types));
	REQUIRE(CHECK_COLUMN(res, 1, expected_log_levels));
	REQUIRE(CHECK_COLUMN(res, 2, expected_messages));
}

// This tests
// - all log levels
// - all combinations of log levels and having either enabled_loggers or disabled_loggers
TEST_CASE("Test logging", "[logging][.]") {
	duckdb::vector<string> log_levels = {"DEBUGGING", "INFO", "WARN", "ERROR", "FATAL"};
	for (const auto & level : log_levels) {
		// Test in regular mode without explicitly enabled or disabled loggers
		test_logging(level, "", "");

		// Test various combinations of enabled and disabled loggers
		test_logging(level, "custom_type,default", "");
		test_logging(level, "custom_type", "");
		test_logging(level, "", "default");
		test_logging(level, "", "custom_type,default");
	}

	// TODO: test thread-local logger
}