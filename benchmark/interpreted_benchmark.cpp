#include "interpreted_benchmark.hpp"
#include "duckdb.hpp"

#include <fstream>
#include <sstream>

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

static string ParseGroupFromPath(string file) {
	string extension = "";
	// move backwards to the last slash
	int group_begin = -1, group_end = -1;
	for(size_t i = file.size(); i > 0; i--) {
		if (file[i - 1] == '/' || file[i - 1] == '\\') {
			if (group_end == -1) {
				group_end = i - 1;
			} else {
				group_begin = i;
				return "[" + file.substr(group_begin, group_end - group_begin) + "]" + extension;
			}
		}
	}
	if (group_end == -1) {
		return "[" + file + "]" + extension;
	}
	return "[" + file.substr(0, group_end) + "]" + extension;
}

struct InterpretedBenchmarkState : public BenchmarkState {
	DuckDB db;
	Connection con;
	unique_ptr<MaterializedQueryResult> result;

	InterpretedBenchmarkState() : db(nullptr), con(db) {
		con.EnableProfiling();
	}
};

InterpretedBenchmark::InterpretedBenchmark(string full_path) :
	Benchmark(true, full_path, ParseGroupFromPath(full_path)), benchmark_path(full_path) {
}

void InterpretedBenchmark::LoadBenchmark() {
	std::ifstream infile(benchmark_path);
	std::string line;
	int linenr = 0;
	while (std::getline(infile, line)) {
		linenr++;
		// skip comments
		if (line[0] == '#') {
			continue;
		}
		StringUtil::Trim(line);
		// skip blank lines
		if (line.empty()) {
			continue;
		}
		// look for a command in this line
		auto splits = StringUtil::Split(StringUtil::Lower(line), ' ');
		if (splits[0] == "load") {
			if (!init_query.empty()) {
				throw std::runtime_error("Multiple calls to LOAD in the same benchmark file");
			}
			// load command: keep reading until we find a blank line or EOF
			while (std::getline(infile, line)) {
				linenr++;
				StringUtil::Trim(line);
				if (line.empty()) {
					break;
				} else {
					init_query += line;
				}
			}
		} else if (splits[0] == "run") {
			if (!run_query.empty()) {
				throw std::runtime_error("Multiple calls to RUN in the same benchmark file");
			}
			// load command: keep reading until we find a blank line or EOF
			while (std::getline(infile, line)) {
				linenr++;
				StringUtil::Trim(line);
				if (line.empty()) {
					break;
				} else {
					run_query += line;
				}
			}
		} else if (splits[0] == "result") {
			if (result_column_count > 0) {
				throw std::runtime_error("multiple results found!");
			}
			// count the amount of columns
			if (splits.size() <= 1 || splits[1].size() == 0) {
				throw std::runtime_error("result must be followed by a column count (e.g. result III)");
			}
			for(int i = 0; i < splits[1].size(); i++) {
				if (splits[1][i] != 'i') {
					throw std::runtime_error("result must be followed by a column count (e.g. result III)");
				}
			}
			result_column_count = splits[1].size();
			// keep reading results until eof
			while (std::getline(infile, line)) {
				linenr++;
				auto result_splits = StringUtil::Split(line, "\t");
				if (result_splits.size() != result_column_count) {
					throw std::runtime_error("error on line " + to_string(linenr) + ", expected " + to_string(result_column_count) + " values but got " + to_string(result_splits.size()));
				}
				result_values.push_back(move(result_splits));
			}
		}
	}

}

unique_ptr<BenchmarkState> InterpretedBenchmark::Initialize() {
	LoadBenchmark();

	auto state = make_unique<InterpretedBenchmarkState>();
	auto result = state->con.Query(init_query);
	if (!result->success) {
		throw Exception(result->error);
	}
	return state;
}

void InterpretedBenchmark::Run(BenchmarkState *state_) {
	auto &state = (InterpretedBenchmarkState &) *state_;
	state.result = state.con.Query(run_query);
}

void InterpretedBenchmark::Cleanup(BenchmarkState *state) {

}

string InterpretedBenchmark::Verify(BenchmarkState *state_) {
	auto &state = (InterpretedBenchmarkState &) *state_;
	if (!state.result->success) {
		return state.result->error;
	}
	if (result_column_count == 0) {
		// no result specified
		return string();
	}
	// compare the column count
	if (state.result->column_count() != result_column_count) {
		return StringUtil::Format("Error in result: expected %lld columns but got %lld", (int64_t) result_column_count, (int64_t) state.result->column_count());
	}
	// compare row count
	if (state.result->collection.count != result_values.size()) {
		return StringUtil::Format("Error in result: expected %lld rows but got %lld", (int64_t) state.result->collection.count, (int64_t) result_values.size());
	}
	// compare values
	for(int64_t r = 0; r < (int64_t) result_values.size(); r++) {
		for(int64_t c = 0; c < result_column_count; c++) {
			auto value = state.result->collection.GetValue(c, r);
			Value verify_val(result_values[r][c]);
			verify_val = verify_val.CastAs(state.result->types[c]);
			if (!Value::ValuesAreEqual(value, verify_val)) {
				return StringUtil::Format("Error in result on row %lld column %lld: expected value \"%s\" but got value \"%s\"", r, c, verify_val.ToString().c_str(), value.ToString().c_str());
			}
		}
	}
	return string();
}


void InterpretedBenchmark::Interrupt(BenchmarkState *state_) {
	auto &state = (InterpretedBenchmarkState &) *state_;
	state.con.Interrupt();
}

string InterpretedBenchmark::BenchmarkInfo() {
	return name + " - " + run_query;
}

string InterpretedBenchmark::GetLogOutput(BenchmarkState *state_) {
	auto &state = (InterpretedBenchmarkState &) *state_;
	return state.con.context->profiler.ToJSON();
}

}
