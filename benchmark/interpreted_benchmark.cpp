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
	unique_ptr<QueryResult> result;

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
	while (std::getline(infile, line)) {
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
				StringUtil::Trim(line);
				if (line.empty()) {
					break;
				} else {
					run_query += line;
				}
			}
		}
	}

}

unique_ptr<BenchmarkState> InterpretedBenchmark::Initialize() {
	LoadBenchmark();

	auto state = make_unique<InterpretedBenchmarkState>();
	state->con.Query(init_query);
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
	if (state.result->success) {
		return string();
	}
	return state.result->error;
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
