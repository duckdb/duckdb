#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb_test/test_registry.hpp"
#include "test_config.hpp"
#include "test_environment.hpp"
#include "test_helpers.hpp"
#include "test_reporter.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <sstream>

#ifdef DUCKDB_TEST_BUNDLE_DEBUG_FS
#include "debug_fs_extension.hpp"
#endif

#ifndef DUCKDB_TEST_BUNDLE_NAME
#error DUCKDB_TEST_BUNDLE_NAME must be defined
#endif

namespace {

using namespace duckdb;
using namespace duckdb_test;

struct CapturedFailure {
	string file;
	uint64_t line = 0;
	string expression;
	string message;
};

struct CaptureState {
	string test_name;
	vector<CapturedFailure> failures;
	std::chrono::steady_clock::time_point start;
};

struct RunContext {
	CaptureState capture;
	test_result_callback_t callback;
	void *callback_data;
	bool &success;
	bool test_active = false;
};

struct StreamCapture {
	StreamCapture() : old_stdout(std::cout.rdbuf(output.rdbuf())), old_stderr(std::cerr.rdbuf(output.rdbuf())) {
	}

	~StreamCapture() {
		std::cout.rdbuf(old_stdout);
		std::cerr.rdbuf(old_stderr);
	}

	std::ostringstream output;
	std::streambuf *old_stdout;
	std::streambuf *old_stderr;
};

struct TempDirReclaimer {
	~TempDirReclaimer() {
		DestroyTempDir(success && std::uncaught_exceptions() == 0);
	}

	bool success = false;
};

static thread_local optional_ptr<RunContext> current_run;

struct RunContextScope {
	explicit RunContextScope(RunContext &context) {
		current_run = context;
	}

	~RunContextScope() {
		current_run = nullptr;
	}
};

struct CatchTestReporter : public TestReporter {
	string CurrentTestName() override {
		return Catch::getResultCapture().getCurrentTestName();
	}
};

static void ReportResult(RunContext &run_context, const string &test_name, TestStatus status, double duration_ms,
                         idx_t assertion_count, vector<CapturedFailure> failures, const string &message,
                         const string &diagnostics = "") {
	vector<TestFailure> reported_failures;
	reported_failures.reserve(failures.size());
	for (auto &failure : failures) {
		reported_failures.push_back(
		    {failure.file.c_str(), failure.line, failure.expression.c_str(), failure.message.c_str()});
	}
	if (status == TestStatus::FAILED) {
		run_context.success = false;
	}
	TestResult result {test_name.c_str(),
	                   status,
	                   duration_ms,
	                   NumericCast<uint64_t>(assertion_count),
	                   reported_failures.data(),
	                   NumericCast<uint64_t>(reported_failures.size()),
	                   message.c_str(),
	                   diagnostics.c_str()};
	run_context.callback(run_context.callback_data, &result);
}

class TestBundleReporter : public Catch::StreamingReporterBase<TestBundleReporter> {
public:
	explicit TestBundleReporter(const Catch::ReporterConfig &config) : StreamingReporterBase(config) {
		m_reporterPrefs.shouldRedirectStdOut = true;
	}

	static std::string getDescription() {
		return "DuckDB test bundle reporter";
	}

	void assertionStarting(Catch::AssertionInfo const &) override {
	}

	bool assertionEnded(Catch::AssertionStats const &stats) override {
		if (!current_run || !current_run->test_active) {
			return true;
		}
		auto &capture = current_run->capture;
		auto &result = stats.assertionResult;
		if (!result.isOk()) {
			auto source = result.getSourceInfo();
			CapturedFailure failure;
			failure.file = source.file;
			failure.line = NumericCast<uint64_t>(source.line);
			failure.expression =
			    result.hasExpandedExpression() ? result.getExpandedExpression() : result.getExpression();
			failure.message = result.getMessage();
			for (auto &message : stats.infoMessages) {
				if (message.type == result.getResultType() && message.message == result.getMessage()) {
					continue;
				}
				if (!failure.message.empty()) {
					failure.message += "\n";
				}
				failure.message += message.message;
			}
			capture.failures.push_back(std::move(failure));
			if (result.getResultType() == Catch::ResultWas::FatalErrorCondition) {
				std::fprintf(stderr, "DuckDB test process received a fatal signal while running cpp:%s:%s\n%s\n",
				             DUCKDB_TEST_BUNDLE_NAME, capture.test_name.c_str(), result.getMessage().c_str());
				std::fflush(stderr);
			}
		}
		return true;
	}

	void testCaseStarting(Catch::TestCaseInfo const &test_info) override {
		StreamingReporterBase::testCaseStarting(test_info);
		if (!current_run) {
			return;
		}
		current_run->capture = CaptureState();
		current_run->capture.test_name = test_info.name;
		current_run->capture.start = std::chrono::steady_clock::now();
		current_run->test_active = true;
	}

	void testCaseEnded(Catch::TestCaseStats const &stats) override {
		if (current_run && current_run->test_active) {
			auto &capture = current_run->capture;
			auto status = TestStatus::PASSED;
			if (stats.totals.testCases.failed > 0 || stats.totals.assertions.failed > 0 || stats.totals.error != 0) {
				status = TestStatus::FAILED;
			} else if (stats.totals.skippedTests > 0) {
				status = TestStatus::SKIPPED;
			}
			string message = stats.skipReason;
			string diagnostics;
			if (status == TestStatus::FAILED) {
				if (!stats.stdOut.empty()) {
					diagnostics += "\n--- test stdout ---\n" + stats.stdOut;
				}
				if (!stats.stdErr.empty()) {
					diagnostics += "\n--- test stderr ---\n" + stats.stdErr;
				}
			}
			auto end = std::chrono::steady_clock::now();
			auto duration = std::chrono::duration<double, std::milli>(end - capture.start).count();
			ReportResult(*current_run, capture.test_name, status, duration, stats.totals.assertions.total(),
			             std::move(capture.failures), message, diagnostics);
			current_run->test_active = false;
		}
		StreamingReporterBase::testCaseEnded(stats);
	}

	void skipTest(Catch::TestCaseInfo const &test_info) override {
		if (current_run) {
			ReportResult(*current_run, test_info.name, TestStatus::SKIPPED, 0, 0, {},
			             "Skipped after an earlier test aborted the run");
		}
	}
};

CATCH_REGISTER_REPORTER("duckdb_test_bundle", TestBundleReporter)

static string QuoteCatchTest(const string &name) {
	string result = "\"";
	for (auto character : name) {
		if (character == '\\' || character == '"' || character == ',') {
			result += '\\';
		}
		result += character;
	}
	result += '"';
	return result;
}

static void InitializeTestEnvironment() {
	auto &config = TestConfiguration::Get();
	string error;
	if (!PrepareTempDir(error)) {
		throw InvalidInputException("Failed to prepare test directory: %s", error);
	}
	config.UpdateEnvironment();
	if (!config.ValidateDataDirs(error)) {
		throw InvalidInputException("%s", error);
	}
	if (!config.GetKeepHome()) {
		auto home_dir = TestMakeAbsolute(GetTempDirHome(), TestGetCurrentDirectory());
#ifdef DUCKDB_WINDOWS
		if (_putenv_s("USERPROFILE", home_dir.c_str()) != 0) {
			throw IOException("Failed to set USERPROFILE environment variable");
		}
#else
		if (setenv("HOME", home_dir.c_str(), 1) != 0) {
			throw IOException("Failed to set HOME environment variable");
		}
#endif
	}
#ifdef DUCKDB_TEST_BUNDLE_DEBUG_FS
	static bool loader_initialized = false;
	if (!loader_initialized) {
		SetStaticExtensionLoader([](DuckDB &db) { db.LoadStaticExtension<DebugFsExtension>(); });
		loader_initialized = true;
	}
#endif
}

static void EnumerateTests(void *, test_case_callback_t callback, void *callback_data) {
	for (auto &test : Catch::getRegistryHub().getTestCaseRegistry().getAllTests()) {
		auto &info = test.getTestCaseInfo();
		auto tags = info.tagsAsString();
		TestCase result {info.name.c_str(), tags.c_str(), info.lineInfo.file,
		                 NumericCast<uint64_t>(info.lineInfo.line)};
		callback(callback_data, &result);
	}
}

static void ConfigureTestFlags(const char *const *test_flags, uint64_t test_flag_count) {
	vector<string> flags;
	for (uint64_t index = 0; index < test_flag_count; index++) {
		flags.emplace_back(test_flags[index]);
	}

	auto &config = TestConfiguration::Get();
	config.Initialize();
	vector<string> arguments {"duckdb_tests"};
	arguments.insert(arguments.end(), flags.begin(), flags.end());
	vector<char *> argv;
	for (auto &argument : arguments) {
		argv.push_back(&argument[0]);
	}
	for (idx_t index = 1; index < argv.size(); index++) {
		auto argument = string(argv[index]);
		try {
			if (!config.ParseArgument(argument, NumericCast<idx_t>(argv.size()), argv.data(), index)) {
				throw InvalidInputException("Unsupported C++ test flag '%s'", argument);
			}
		} catch (std::exception &ex) {
			throw InvalidInputException("Invalid C++ test flag '%s': %s", argument, ex.what());
		}
	}
}

static void RunTests(void *, const char *const *test_names, uint64_t test_count, const char *const *test_flags,
                     uint64_t test_flag_count, test_result_callback_t callback, void *callback_data) {
	static mutex run_lock;
	lock_guard<mutex> guard(run_lock);
	TestEnvironmentScope environment_scope;
	ConfigureTestFlags(test_flags, test_flag_count);
	TempDirReclaimer temp_dir_reclaimer;
	InitializeTestEnvironment();
	temp_dir_reclaimer.success = true;

	RunContext run_context {{}, callback, callback_data, temp_dir_reclaimer.success};
	RunContextScope run_scope(run_context);

	// Catch destroys its global test registry with a Session, so keep the embedded session process-local.
	static auto session = new Catch::Session();
	Catch::ConfigData config;
	config.name = DUCKDB_TEST_BUNDLE_NAME;
	config.reporterName = "duckdb_test_bundle";
	config.useColour = Catch::UseColour::No;
	string test_filter;
	for (uint64_t test_idx = 0; test_idx < test_count; test_idx++) {
		if (!test_filter.empty()) {
			test_filter += ",";
		}
		test_filter += QuoteCatchTest(test_names[test_idx]);
	}
	config.testsOrTags.push_back(std::move(test_filter));
	session->useConfigData(config);
	{
		CatchTestReporter reporter;
		TestReporterScope reporter_scope(reporter);
		StreamCapture stream_capture;
		auto session_result = session->run();
		if (session_result != 0) {
			temp_dir_reclaimer.success = false;
			throw InvalidInputException("C++ test bundle '%s' failed (Catch exit code %d):\n%s",
			                            DUCKDB_TEST_BUNDLE_NAME, session_result, stream_capture.output.str());
		}
	}
}

static TestBundle bundle {DUCKDB_TEST_BUNDLE_NAME, nullptr, EnumerateTests, RunTests};

} // namespace

#define DUCKDB_TEST_BUNDLE_ENTRY(name) DUCKDB_CPP_EXTENSION_ENTRY(name, loader)

extern "C" {

DUCKDB_TEST_BUNDLE_ENTRY(DUCKDB_TEST_BUNDLE_IDENTIFIER) {
	auto registry = loader.GetDatabaseInstance().GetObjectCache().Get<duckdb_test::TestRegistryEntry>(
	    duckdb_test::TestRegistryEntry::ObjectType());
	if (!registry) {
		throw duckdb::InvalidInputException("The test extension must be loaded before '%s'", DUCKDB_TEST_BUNDLE_NAME);
	}
	registry->registry.register_bundle(registry->registry.data, &bundle);
}
}
