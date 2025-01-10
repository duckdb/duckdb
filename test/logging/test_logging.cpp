#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/logging/log_storage.hpp"

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
	f(src, []() { return "log-a-lot: 'callback'"; });
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
	f("custom_type", src, []() { return "log-a-lot: 'callback with type'"; });
}

#define TEST_ALL_LOG_TEMPLATES(FUNCTION, SOURCE, SOURCE_TYPE)                                                          \
	LogSimple<SOURCE_TYPE, void (*)(SOURCE_TYPE &, const char *)>(SOURCE, &FUNCTION);                                  \
	LogFormatString<SOURCE_TYPE, void (*)(SOURCE_TYPE &, const char *, const char *)>(SOURCE, &FUNCTION);              \
	LogCallback<SOURCE_TYPE, void (*)(SOURCE_TYPE &, std::function<string()>)>(SOURCE, &FUNCTION);                     \
	LogSimpleCustomType<SOURCE_TYPE, void (*)(const char *, SOURCE_TYPE &, const char *)>(SOURCE, &FUNCTION);          \
	LogFormatStringCustomType<SOURCE_TYPE, void (*)(const char *, SOURCE_TYPE &, const char *, const char *)>(         \
	    SOURCE, &FUNCTION);                                                                                            \
	LogCallbackCustomType<SOURCE_TYPE, void (*)(const char *, SOURCE_TYPE &, std::function<string()>)>(SOURCE,         \
	                                                                                                   &FUNCTION);

// Tests all Logger function entrypoints at the specified log level with the specified enabled/disabled loggers
void test_logging(const string &minimum_level, const string &enabled_log_types, const string &disabled_log_types) {
	DuckDB db(nullptr);
	Connection con(db);

	duckdb::vector<Value> default_types = {"default", "default"};
	duckdb::vector<string> log_levels = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"};
	auto minimum_level_index = std::find(log_levels.begin(), log_levels.end(), minimum_level) - log_levels.begin();

	REQUIRE_NO_FAIL(con.Query("set enable_logging=true;"));
	REQUIRE_NO_FAIL(con.Query("set logging_level='" + minimum_level +
	                          ""
	                          "';"));
	if (!enabled_log_types.empty()) {
		REQUIRE_NO_FAIL(con.Query("set enabled_log_types='" + enabled_log_types +
		                          ""
		                          "';"));
		REQUIRE_NO_FAIL(con.Query("set logging_mode='enable_selected';"));
	}
	if (!disabled_log_types.empty()) {
		REQUIRE_NO_FAIL(con.Query("set disabled_log_types='" + disabled_log_types +
		                          ""
		                          "';"));
		REQUIRE_NO_FAIL(con.Query("set logging_mode='disable_selected';"));
	}

	bool level_only = (enabled_log_types.empty() && disabled_log_types.empty());
	bool enabled_mode = !enabled_log_types.empty();
	bool disabled_mode = !disabled_log_types.empty();

	// Log all to global logger
	TEST_ALL_LOG_TEMPLATES(Logger::Trace, *db.instance, DatabaseInstance);
	TEST_ALL_LOG_TEMPLATES(Logger::Debug, *db.instance, DatabaseInstance);
	TEST_ALL_LOG_TEMPLATES(Logger::Info, *db.instance, DatabaseInstance);
	TEST_ALL_LOG_TEMPLATES(Logger::Warn, *db.instance, DatabaseInstance);
	TEST_ALL_LOG_TEMPLATES(Logger::Error, *db.instance, DatabaseInstance);
	TEST_ALL_LOG_TEMPLATES(Logger::Fatal, *db.instance, DatabaseInstance);

	// Log all to client context logger
	TEST_ALL_LOG_TEMPLATES(Logger::Trace, *con.context, ClientContext);
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
	idx_t num_log_levels = 6;

	for (idx_t i = 0; i < num_runs; i++) {
		for (idx_t j = minimum_level_index; j < num_log_levels; j++) {
			if (level_only ||
			    (enabled_mode && enabled_log_types.find(default_types[i].ToString()) != enabled_log_types.npos) ||
			    (disabled_mode && disabled_log_types.find(default_types[i].ToString()) == enabled_log_types.npos)) {
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

			if (level_only || (enabled_mode && enabled_log_types.find("custom_type") != enabled_log_types.npos) ||
			    (disabled_mode && disabled_log_types.find("custom_type") == enabled_log_types.npos)) {
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
// - all combinations of log levels and having either enabled_log_types or disabled_log_types
TEST_CASE("Test logging", "[logging][.]") {
	duckdb::vector<string> log_levels = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"};
	for (const auto &level : log_levels) {
		// Test in regular mode without explicitly enabled or disabled loggers
		test_logging(level, "", "");

		// Test various combinations of enabled and disabled loggers
		test_logging(level, "custom_type,default", "");
		test_logging(level, "custom_type", "");
		test_logging(level, "", "default");
		test_logging(level, "", "custom_type,default");
	}
}

struct TestLoggingData : public LocalTableFunctionState {
	explicit TestLoggingData(ExecutionContext &context_p) : context(context_p) {};
	ExecutionContext &context;
};

static duckdb::unique_ptr<LocalTableFunctionState>
TestLoggingInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *global_state) {
	return make_uniq<TestLoggingData>(context);
}

static duckdb::unique_ptr<FunctionData> TestLoggingBind(ClientContext &context, TableFunctionBindInput &input,
                                                        duckdb::vector<LogicalType> &return_types,
                                                        duckdb::vector<string> &names) {
	names.emplace_back("value");
	return_types.emplace_back(LogicalType::INTEGER);

	return make_uniq<TableFunctionData>();
}

static void TestLoggingFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &local_state = data_p.local_state->Cast<TestLoggingData>();
	Logger::Warn(local_state.context, "thread_logger");
	output.SetCardinality(0);
}

// This test the thread context logger
TEST_CASE("Test thread context logger", "[logging][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	duckdb::TableFunction tf("test_thread_logger", {}, TestLoggingFunction, TestLoggingBind, nullptr,
	                         TestLoggingInitLocal);
	ExtensionUtil::RegisterFunction(*db.instance, tf);

	REQUIRE_NO_FAIL(con.Query("set enable_logging=true;"));

	// Run our dummy table function to call the thread local logger
	REQUIRE_NO_FAIL(con.Query("FROM test_thread_logger()"));

	auto res = con.Query("SELECT scope, message, from duckdb_logs where starts_with(message, 'thread_logger')");
	REQUIRE(CHECK_COLUMN(res, 0, {"THREAD"}));
	REQUIRE(CHECK_COLUMN(res, 1, {"thread_logger"}));
}

// Testing pluggable log storage
class MyLogStorage : public LogStorage {
public:
	void WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type, const string &log_message,
	                   const RegisteredLoggingContext &context) override {
		log_store.insert(log_message);
	};
	void WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) override {};
	void Flush() override {};

	unordered_set<string> log_store;
};

TEST_CASE("Test pluggable log storage", "[logging][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto my_log_storage = make_shared_ptr<MyLogStorage>();

	duckdb::shared_ptr<LogStorage> base_ptr = my_log_storage;
	db.instance->GetLogManager().RegisterLogStorage("my_log_storage", base_ptr);

	REQUIRE_NO_FAIL(con.Query("set enable_logging=true;"));
	REQUIRE_NO_FAIL(con.Query("set logging_storage='my_log_storage';"));

	REQUIRE_NO_FAIL(con.Query("select write_log('HELLO, BRO');"));

	REQUIRE(my_log_storage->log_store.find("HELLO, BRO") != my_log_storage->log_store.end());
}
