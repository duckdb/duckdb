#include <stdint.h>

#if (!defined(_WIN32) && !defined(WIN32)) || defined(__MINGW32__)
#include <unistd.h>
#endif

#include "duckdb/common/string_util.hpp"
#include "duckdb.hpp"
#include "test_helpers.hpp"

namespace duckdb {

static bool IsEmptyOrComment(const string &line) {
	auto trimmed = line;
	StringUtil::Trim(trimmed);
	return trimmed.empty() || StringUtil::StartsWith(trimmed, "#");
}

static string GetLineToken(const string &line) {
	auto trimmed = line;
	StringUtil::Trim(trimmed);
	auto separator_pos = trimmed.find_first_of(" \t");
	if (separator_pos == string::npos) {
		return StringUtil::Lower(trimmed);
	}
	return StringUtil::Lower(trimmed.substr(0, separator_pos));
}

static void ExecuteSQLBlocks(Connection &con, const string &script) {
	vector<string> lines;
	lines.reserve(1024);
	string line;
	for (auto c : script) {
		if (c == '\r') {
			continue;
		}
		if (c == '\n') {
			lines.push_back(line);
			line.clear();
			continue;
		}
		line.push_back(c);
	}
	if (!line.empty()) {
		lines.push_back(line);
	}

	idx_t i = 0;
	while (i < lines.size()) {
		if (IsEmptyOrComment(lines[i])) {
			i++;
			continue;
		}

		idx_t statement_line = i;
		while (statement_line < lines.size()) {
			auto token = GetLineToken(lines[statement_line]);
			if (token == "skipif" || token == "onlyif") {
				statement_line++;
				continue;
			}
			break;
		}
		if (statement_line >= lines.size()) {
			break;
		}

		auto token = GetLineToken(lines[statement_line]);
		if (token != "statement" && token != "query") {
			while (i < lines.size() && !IsEmptyOrComment(lines[i])) {
				i++;
			}
			continue;
		}

		i = statement_line + 1;
		string sql;
		bool first_line = true;
		while (i < lines.size()) {
			auto trimmed = lines[i];
			StringUtil::Trim(trimmed);
			if (trimmed == "----" || IsEmptyOrComment(lines[i])) {
				break;
			}
			if (!first_line) {
				sql += "\n";
			}
			sql += lines[i];
			first_line = false;
			i++;
		}
		if (!sql.empty()) {
			try {
				(void)con.Query(sql);
			} catch (...) {
			}
		}

		if (i < lines.size()) {
			auto delimiter = lines[i];
			StringUtil::Trim(delimiter);
			if (delimiter == "----") {
				i++;
				while (i < lines.size() && !IsEmptyOrComment(lines[i])) {
					i++;
				}
			}
		}
	}
}

#ifdef DUCKDB_FUZZER
static string BuildFuzzScript(const uint8_t *data, size_t size) {
	constexpr size_t MAX_SCRIPT_SIZE = 1 << 20;
	if (size > MAX_SCRIPT_SIZE) {
		size = MAX_SCRIPT_SIZE;
	}
	string script;
	script.reserve(size + 1);
	for (size_t i = 0; i < size; i++) {
		auto c = static_cast<char>(data[i]);
		if (c == '\0') {
			c = '\n';
		}
		script.push_back(c);
	}
	if (script.empty() || script.back() != '\n') {
		script.push_back('\n');
	}
	return script;
}

static void RunFuzzIteration(const uint8_t *data, size_t size) {
	auto script = BuildFuzzScript(data, size);
	try {
		ClearTestDirectory();
		auto config = GetTestConfig();
		DuckDB db(nullptr, config.get());
		Connection con(db);
		(void)con.Query("PRAGMA disable_progress_bar");
		ExecuteSQLBlocks(con, script);
	} catch (...) {
	}
	try {
		ClearTestDirectory();
	} catch (...) {
	}
}
#endif

} // namespace duckdb

#ifdef DUCKDB_FUZZER
static int RunAFLFuzzerLoopInternal() {
	__AFL_FUZZ_INIT();
#ifdef __AFL_HAVE_MANUAL_CONTROL
	__AFL_INIT();
#endif

	auto *buf = __AFL_FUZZ_TESTCASE_BUF;
	while (__AFL_LOOP(1000)) {
		auto size = static_cast<size_t>(__AFL_FUZZ_TESTCASE_LEN);
		if (!buf || size == 0) {
			continue;
		}
		duckdb::RunFuzzIteration(buf, size);
	}
	return 0;
}
#endif

namespace duckdb {

int RunAFLFuzzerLoop() {
#ifdef DUCKDB_FUZZER
	return RunAFLFuzzerLoopInternal();
#endif
	return 0;
}

} // namespace duckdb
