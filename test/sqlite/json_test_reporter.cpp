#include "catch.hpp"

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/string_util.hpp"
#include "test_failure_record.hpp"
#include "test_helpers.hpp"
#include "test_reporter.hpp"

#include <chrono>

namespace duckdb {

namespace {

//! Renders the run as JSON Lines on the reporter's stream: a "test" object per test case that did not
//! pass, then a single "summary" object. Selected with --output=json (or -r json). Replacing the
//! console reporter rather than adding to it is what keeps the stream free of prose a consumer would
//! have to skip.
//!
//! Passing tests are counted in the summary but not emitted individually - on a full run they are
//! ~99% of the lines and carry nothing a consumer acts on. Catch's own -s/--success asks for them
//! back, matching what that flag means for the built-in reporters. Skips are always emitted: they are
//! few, and a test that silently did not run is exactly what a consumer needs to be told about.
class JSONTestReporter : public Catch::StreamingReporterBase<JSONTestReporter> {
public:
	explicit JSONTestReporter(Catch::ReporterConfig const &config)
	    : Catch::StreamingReporterBase<JSONTestReporter>(config),
	      include_passing(config.fullConfig()->includeSuccessfulResults()) {
	}

	~JSONTestReporter() override = default;

	static std::string getDescription() {
		return "Reports each test case as a JSON object on its own line, followed by a summary object";
	}

	// The verdict this reporter renders is per test case, so assertions produce no output of their own.
	void assertionStarting(Catch::AssertionInfo const &) override {
	}

	bool assertionEnded(Catch::AssertionStats const &stats) override {
		auto &result = stats.assertionResult;
		if (result.succeeded() || !TestFailureRecorder::Enabled() || TestFailureRecorder::HasRecord()) {
			// Both sqllogictest paths (the logger and CatchTestReporter::Fail) have already recorded, and
			// they know the script location; what is left here is a compiled C++ test, whose failure
			// location genuinely is the Catch source info below.
			return true;
		}
		TestFailureRecord record;
		record.kind = TestFailureKind::OTHER;
		if (result.hasExpression()) {
			record.message = result.getExpressionInMacro();
			if (result.hasExpandedExpression()) {
				record.error_message = result.getExpandedExpression();
			}
		} else if (result.hasMessage()) {
			record.message = result.getMessage();
		}
		auto source_info = result.getSourceInfo();
		record.file = source_info.file;
		record.line = NumericCast<idx_t>(source_info.line);
		for (auto &info : stats.infoMessages) {
			if (!record.error_message.empty()) {
				record.error_message += "\n";
			}
			record.error_message += info.message;
		}
		TestFailureRecorder::Record(std::move(record));
		return true;
	}

	void testCaseStarting(Catch::TestCaseInfo const &test_info) override {
		Catch::StreamingReporterBase<JSONTestReporter>::testCaseStarting(test_info);
		// A record left by the previous test case would otherwise be attributed to this one
		TestFailureRecorder::Clear();
		TestReporter::Get().skip_reason.clear();
		test_start = std::chrono::steady_clock::now();
	}

	void testCaseEnded(Catch::TestCaseStats const &stats) override {
		const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - test_start).count();
		const bool skipped = stats.totals.skippedTests > 0;
		const bool failed = stats.totals.testCases.failed > 0 || stats.totals.assertions.failed > 0;

		JSONWriter writer;
		auto obj = writer.CreateObject();
		obj.AddString("event", "test");
		obj.AddString("name", stats.testInfo.name);
		obj.AddString("status", skipped ? "skip" : (failed ? "fail" : "pass"));
		obj.Add("duration", writer.CreateDouble(elapsed));

		auto assertions = writer.CreateObject();
		assertions.Add("passed", writer.CreateUnsignedInteger(stats.totals.assertions.passed));
		assertions.Add("failed", writer.CreateUnsignedInteger(stats.totals.assertions.failed));
		obj.Add("assertions", std::move(assertions));

		if (skipped) {
			total_skipped++;
			auto &reason = TestReporter::Get().skip_reason;
			if (!reason.empty()) {
				obj.AddString("skip_reason", reason);
				skip_reasons[reason]++;
			}
		} else if (failed) {
			total_failed++;
			auto record = TestFailureRecorder::Take();
			if (record.IsSet()) {
				obj.Add("failure", SerializeFailure(writer, record));
				failed_tests.emplace_back(stats.testInfo.name, record.line, TestFailureKindToString(record.kind));
			} else {
				// A compiled C++ test, or a failure raised outside the sqllogictest logger: the verdict is
				// still exact, only the structured detail is unavailable.
				failed_tests.emplace_back(stats.testInfo.name, 0, "unknown");
			}
		} else {
			total_passed++;
		}

		total_assertions_passed += stats.totals.assertions.passed;
		total_assertions_failed += stats.totals.assertions.failed;

		if (failed || skipped || include_passing) {
			writer.SetRoot(obj);
			WriteLine(writer.ToString());
		}
		Catch::StreamingReporterBase<JSONTestReporter>::testCaseEnded(stats);
	}

	void testRunEnded(Catch::TestRunStats const &stats) override {
		JSONWriter writer;
		auto obj = writer.CreateObject();
		obj.AddString("event", "summary");
		obj.Add("total", writer.CreateUnsignedInteger(total_passed + total_failed + total_skipped));
		obj.Add("passed", writer.CreateUnsignedInteger(total_passed));
		obj.Add("failed", writer.CreateUnsignedInteger(total_failed));
		obj.Add("skipped", writer.CreateUnsignedInteger(total_skipped));

		auto assertions = writer.CreateObject();
		assertions.Add("passed", writer.CreateUnsignedInteger(total_assertions_passed));
		assertions.Add("failed", writer.CreateUnsignedInteger(total_assertions_failed));
		obj.Add("assertions", std::move(assertions));

		auto failed_array = writer.CreateArray();
		for (auto &entry : failed_tests) {
			auto failed_obj = writer.CreateObject();
			failed_obj.AddString("name", entry.name);
			if (entry.line > 0) {
				failed_obj.Add("line", writer.CreateUnsignedInteger(entry.line));
			}
			failed_obj.AddString("kind", entry.kind);
			failed_array.Append(std::move(failed_obj));
		}
		obj.Add("failed_tests", std::move(failed_array));

		if (!skip_reasons.empty()) {
			auto reasons = writer.CreateObject();
			for (auto &entry : skip_reasons) {
				reasons.Add(entry.first, writer.CreateUnsignedInteger(entry.second));
			}
			obj.Add("skip_reasons", std::move(reasons));
		}

		writer.SetRoot(obj);
		WriteLine(writer.ToString());
		Catch::StreamingReporterBase<JSONTestReporter>::testRunEnded(stats);
	}

	void noMatchingTestCases(std::string const &spec) override {
		// Emitted as an event rather than swallowed: an empty run is otherwise indistinguishable from a
		// filter that matched nothing, which is the most common way a run is silently wrong.
		JSONWriter writer;
		auto obj = writer.CreateObject();
		obj.AddString("event", "no_matching_tests");
		obj.AddString("filter", spec);
		writer.SetRoot(obj);
		WriteLine(writer.ToString());
	}

private:
	struct FailedTest {
		FailedTest(string name_p, idx_t line_p, string kind_p)
		    : name(std::move(name_p)), line(line_p), kind(std::move(kind_p)) {
		}
		string name;
		idx_t line;
		string kind;
	};

	void WriteLine(const string &line) {
		stream << line << "\n";
		stream.flush();
	}

	static JSONMutableValue SerializeValues(JSONWriter &writer, const vector<string> &values) {
		auto array = writer.CreateArray();
		for (auto &value : values) {
			array.AppendString(value);
		}
		return array;
	}

	static JSONMutableValue SerializeFailure(JSONWriter &writer, const TestFailureRecord &record) {
		auto failure = writer.CreateObject();
		failure.AddString("kind", TestFailureKindToString(record.kind));
		failure.AddString("message", record.message);
		if (!record.file.empty()) {
			failure.AddString("file", record.file);
		}
		if (record.line > 0) {
			failure.Add("line", writer.CreateUnsignedInteger(record.line));
		}
		if (!record.query.empty()) {
			auto query = record.query;
			StringUtil::RTrim(query);
			failure.AddString("query", query);
		}
		if (record.columns > 0) {
			failure.Add("columns", writer.CreateUnsignedInteger(record.columns));
		}
		if (record.has_expected) {
			failure.Add("expected", SerializeValues(writer, record.expected));
			failure.Add("expected_rows", writer.CreateUnsignedInteger(record.expected_rows));
			if (record.expected_truncated) {
				failure.Add("expected_truncated", writer.CreateBoolean(true));
			}
		}
		if (record.has_actual) {
			failure.Add("actual", SerializeValues(writer, record.actual));
			failure.Add("actual_rows", writer.CreateUnsignedInteger(record.actual_rows));
			if (record.actual_truncated) {
				failure.Add("actual_truncated", writer.CreateBoolean(true));
			}
		}
		if (!record.mismatch_rows.empty()) {
			auto rows = writer.CreateArray();
			for (auto row : record.mismatch_rows) {
				rows.Append(writer.CreateUnsignedInteger(row));
			}
			failure.Add("mismatch_rows", std::move(rows));
		}
		if (!record.expected_hash.empty()) {
			failure.AddString("expected_hash", record.expected_hash);
		}
		if (!record.actual_hash.empty()) {
			failure.AddString("actual_hash", record.actual_hash);
		}
		if (!record.suggested_fix.empty()) {
			failure.AddString("suggested_fix", record.suggested_fix);
		}
		if (!record.error_message.empty()) {
			failure.AddString("error", record.error_message);
		}
		return failure;
	}

private:
	const bool include_passing;
	std::chrono::steady_clock::time_point test_start;
	idx_t total_passed = 0;
	idx_t total_failed = 0;
	idx_t total_skipped = 0;
	idx_t total_assertions_passed = 0;
	idx_t total_assertions_failed = 0;
	vector<FailedTest> failed_tests;
	std::map<string, idx_t> skip_reasons;
};

CATCH_REGISTER_REPORTER("json", JSONTestReporter)

} // namespace

} // namespace duckdb
