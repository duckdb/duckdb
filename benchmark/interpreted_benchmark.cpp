#include "interpreted_benchmark.hpp"

#include "benchmark_runner.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "test_helpers.hpp"

#include <fstream>
#include <sstream>

namespace duckdb {

static string ParseGroupFromPath(string file) {
	string extension = "";
	// move backwards to the last slash
	int group_begin = -1, group_end = -1;
	for (size_t i = file.size(); i > 0; i--) {
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
	unique_ptr<DBConfig> benchmark_config;
	DuckDB db;
	Connection con;
	unique_ptr<MaterializedQueryResult> result;

	explicit InterpretedBenchmarkState(string path)
	    : benchmark_config(GetBenchmarkConfig()), db(path.empty() ? nullptr : path.c_str(), benchmark_config.get()),
	      con(db) {
		auto &instance = BenchmarkRunner::GetInstance();
		auto res = con.Query("PRAGMA threads=" + to_string(instance.threads));
		D_ASSERT(res->success);
	}

	unique_ptr<DBConfig> GetBenchmarkConfig() {
		auto result = make_unique<DBConfig>();
		result->options.load_extensions = false;
		return result;
	}
};

struct BenchmarkFileReader {
	BenchmarkFileReader(string path_, unordered_map<std::string, std::string> replacement_map)
	    : path(path_), infile(path), linenr(0), replacements(replacement_map) {
	}

public:
	bool ReadLine(std::string &line) {
		if (!std::getline(infile, line)) {
			return false;
		}
		linenr++;
		for (auto &replacement : replacements) {
			line = StringUtil::Replace(line, "${" + replacement.first + "}", replacement.second);
		}
		StringUtil::Trim(line);
		return true;
	}

	int LineNumber() {
		return linenr;
	}

	std::string FormatException(string exception_msg) {
		return path + ":" + std::to_string(linenr) + " - " + exception_msg;
	}

private:
	std::string path;
	std::ifstream infile;
	int linenr;
	unordered_map<std::string, std::string> replacements;
};

InterpretedBenchmark::InterpretedBenchmark(string full_path)
    : Benchmark(true, full_path, ParseGroupFromPath(full_path)), benchmark_path(full_path) {
	replacement_mapping["BENCHMARK_DIR"] = BenchmarkRunner::DUCKDB_BENCHMARK_DIRECTORY;
}

void InterpretedBenchmark::LoadBenchmark() {
	if (is_loaded) {
		return;
	}
	BenchmarkFileReader reader(benchmark_path, replacement_mapping);
	string line;
	while (reader.ReadLine(line)) {
		// skip blank lines and comments
		if (line.empty() || line[0] == '#') {
			continue;
		}
		// look for a command in this line
		auto splits = StringUtil::Split(StringUtil::Lower(line), ' ');
		if (splits[0] == "load" || splits[0] == "run" || splits[0] == "init" || splits[0] == "cleanup") {
			if (queries.find(splits[0]) != queries.end()) {
				throw std::runtime_error("Multiple calls to " + splits[0] + " in the same benchmark file");
			}
			// load command: keep reading until we find a blank line or EOF
			string query;
			while (reader.ReadLine(line)) {
				if (line.empty()) {
					break;
				} else {
					query += line + " ";
				}
			}
			if (splits.size() > 1 && !splits[1].empty()) {
				// read entire file into query
				std::ifstream file(splits[1], std::ios::ate);
				std::streamsize size = file.tellg();
				file.seekg(0, std::ios::beg);
				if (size < 0) {
					throw std::runtime_error("Failed to read " + splits[0] + " from file " + splits[1]);
				}

				auto buffer = unique_ptr<char[]>(new char[size]);
				if (!file.read(buffer.get(), size)) {
					throw std::runtime_error("Failed to read " + splits[0] + " from file " + splits[1]);
				}
				query = string(buffer.get(), size);
			}
			StringUtil::Trim(query);
			if (query.empty()) {
				throw std::runtime_error("Encountered an empty " + splits[0] + " node!");
			}
			queries[splits[0]] = query;
		} else if (splits[0] == "require") {
			if (splits.size() != 2) {
				throw std::runtime_error(reader.FormatException("require requires a single parameter"));
			}
			extensions.insert(splits[1]);
		} else if (splits[0] == "cache") {
			if (splits.size() != 2) {
				throw std::runtime_error(reader.FormatException("cache requires a single parameter"));
			}
			data_cache = splits[1];
		} else if (splits[0] == "storage") {
			if (splits.size() != 2) {
				throw std::runtime_error(reader.FormatException("storage requires a single parameter"));
			}
			if (splits[1] == "transient") {
				in_memory = true;
			} else if (splits[1] == "persistent") {
				in_memory = false;
			} else {
				throw std::runtime_error(reader.FormatException("Invalid argument for storage"));
			}
		} else if (splits[0] == "require_reinit") {
			if (splits.size() != 1) {
				throw std::runtime_error(reader.FormatException("require_reinit does not take any parameters"));
			}
			require_reinit = true;
		} else if (splits[0] == "name" || splits[0] == "group" || splits[0] == "subgroup") {
			if (splits.size() == 1) {
				throw std::runtime_error(reader.FormatException(splits[0] + " requires a parameter"));
			}
			string result = line.substr(splits[0].size() + 1, line.size() - 1);
			StringUtil::Trim(result);
			if (splits[0] == "name") {
				display_name = result;
			} else if (splits[0] == "group") {
				display_group = result;
			} else {
				subgroup = result;
			}
		} else if (splits[0] == "result") {
			if (result_column_count > 0) {
				throw std::runtime_error(reader.FormatException("multiple results found"));
			}
			// count the amount of columns
			if (splits.size() <= 1 || splits[1].size() == 0) {
				throw std::runtime_error(
				    reader.FormatException("result must be followed by a column count (e.g. result III) or a file "
				                           "(e.g. result /path/to/file.csv)"));
			}
			bool is_file = false;
			for (idx_t i = 0; i < splits[1].size(); i++) {
				if (splits[1][i] != 'i') {
					is_file = true;
					break;
				}
			}
			if (is_file) {
				// read the results from the file
				result_column_count = -1;
				std::ifstream csv_infile(splits[1]);
				bool skipped_header = false;
				idx_t line_number = 0;
				while (std::getline(csv_infile, line)) {
					line_number++;
					if (line.empty()) {
						break;
					}
					if (!skipped_header) {
						skipped_header = true;
						continue;
					}
					auto result_splits = StringUtil::Split(line, "|");
					if (result_column_count < 0) {
						result_column_count = result_splits.size();
					} else if (idx_t(result_column_count) != result_splits.size()) {
						throw std::runtime_error("error in file " + splits[1] +
						                         ", inconsistent amount of rows in CSV on line " +
						                         to_string(line_number));
					}
					result_values.push_back(move(result_splits));
				}

				// read the main file until we encounter an empty line
				while (reader.ReadLine(line)) {
					if (line.empty()) {
						break;
					}
				}
			} else {
				result_column_count = splits[1].size();
				// keep reading results until eof
				while (reader.ReadLine(line)) {
					if (line.empty()) {
						break;
					}
					auto result_splits = StringUtil::Split(line, "\t");
					if ((int64_t)result_splits.size() != result_column_count) {
						throw std::runtime_error(
						    reader.FormatException("expected " + std::to_string(result_splits.size()) +
						                           " values but got " + std::to_string(result_column_count)));
					}
					result_values.push_back(move(result_splits));
				}
			}
		} else if (splits[0] == "template") {
			// template: update the path to read
			benchmark_path = splits[1];
			// now read parameters
			while (reader.ReadLine(line)) {
				if (line.empty()) {
					break;
				}
				auto parameters = StringUtil::Split(line, '=');
				if (parameters.size() != 2) {
					throw std::runtime_error(
					    reader.FormatException("Expected a template parameter in the form of X=Y"));
				}
				replacement_mapping[parameters[0]] = parameters[1];
			}
			// restart the load from the template file
			LoadBenchmark();
			return;
		} else {
			throw std::runtime_error(reader.FormatException("unrecognized command " + splits[0]));
		}
	}
	// set up the queries
	if (queries.find("run") == queries.end()) {
		throw Exception("Invalid benchmark file: no \"run\" query specified");
	}
	run_query = queries["run"];
	is_loaded = true;
}

unique_ptr<BenchmarkState> InterpretedBenchmark::Initialize(BenchmarkConfiguration &config) {
	unique_ptr<QueryResult> result;
	LoadBenchmark();
	auto state = make_unique<InterpretedBenchmarkState>(GetDatabasePath());
	for (auto &extension : extensions) {
		auto result = ExtensionHelper::LoadExtension(state->db, extension);
		if (result == ExtensionLoadResult::EXTENSION_UNKNOWN) {
			throw std::runtime_error("Unknown extension " + extension);
		} else if (result == ExtensionLoadResult::NOT_LOADED) {
			throw std::runtime_error("Extension " + extension +
			                         " is not available/was not compiled. Cannot run this benchmark.");
		}
	}

	if (queries.find("init") != queries.end()) {
		string init_query = queries["init"];
		result = state->con.Query(init_query);
		while (result) {
			if (!result->success) {
				throw Exception(result->error);
			}
			result = move(result->next);
		}
	}

	string load_query;
	if (queries.find("load") != queries.end()) {
		load_query = queries["load"];
	}

	if (data_cache.empty()) {
		// no cache specified: just run the initialization code
		result = state->con.Query(load_query);
	} else {
		// cache specified: try to load the cache
		if (!BenchmarkRunner::TryLoadDatabase(state->db, data_cache)) {
			// failed to load: write the cache
			result = state->con.Query(load_query);
			BenchmarkRunner::SaveDatabase(state->db, data_cache);
		}
	}
	while (result) {
		if (!result->success) {
			throw Exception(result->error);
		}
		result = move(result->next);
	}
	if (config.profile_info == BenchmarkProfileInfo::NORMAL) {
		state->con.Query("PRAGMA enable_profiling");
	} else if (config.profile_info == BenchmarkProfileInfo::DETAILED) {
		state->con.Query("PRAGMA enable_profiling");
		state->con.Query("PRAGMA profiling_mode='detailed'");
	}
	return state;
}

string InterpretedBenchmark::GetQuery() {
	LoadBenchmark();
	return run_query;
}

void InterpretedBenchmark::Run(BenchmarkState *state_p) {
	auto &state = (InterpretedBenchmarkState &)*state_p;
	state.result = state.con.Query(run_query);
}

void InterpretedBenchmark::Cleanup(BenchmarkState *state_p) {
	auto &state = (InterpretedBenchmarkState &)*state_p;
	if (queries.find("cleanup") != queries.end()) {
		unique_ptr<QueryResult> result;
		string cleanup_query = queries["cleanup"];
		result = state.con.Query(cleanup_query);
		while (result) {
			if (!result->success) {
				throw Exception(result->error);
			}
			result = move(result->next);
		}
	}
}

string InterpretedBenchmark::GetDatabasePath() {
	if (!InMemory()) {
		string path = "duckdb_benchmark_db.db";
		DeleteDatabase(path);
		return path;
	} else {
		return string();
	}
}

string InterpretedBenchmark::Verify(BenchmarkState *state_p) {
	auto &state = (InterpretedBenchmarkState &)*state_p;
	if (!state.result->success) {
		return state.result->error;
	}
	if (result_column_count == 0) {
		// no result specified
		return string();
	}
	// compare the column count
	if (result_column_count >= 0 && (int64_t)state.result->ColumnCount() != result_column_count) {
		return StringUtil::Format("Error in result: expected %lld columns but got %lld\nObtained result: %s",
		                          (int64_t)result_column_count, (int64_t)state.result->ColumnCount(),
		                          state.result->ToString());
	}
	// compare row count
	if (state.result->collection.Count() != result_values.size()) {
		return StringUtil::Format("Error in result: expected %lld rows but got %lld\nObtained result: %s",
		                          (int64_t)result_values.size(), (int64_t)state.result->collection.Count(),
		                          state.result->ToString());
	}
	// compare values
	for (int64_t r = 0; r < (int64_t)result_values.size(); r++) {
		for (int64_t c = 0; c < result_column_count; c++) {
			auto value = state.result->collection.GetValue(c, r);
			if (result_values[r][c] == "NULL" && value.IsNull()) {
				continue;
			}

			Value verify_val(result_values[r][c]);
			try {
				if (result_values[r][c] == value.ToString()) {
					continue;
				}
				verify_val = verify_val.CastAs(state.result->types[c]);
				if (result_values[r][c] == "(empty)" && (verify_val.ToString() == "" || value.IsNull())) {
					continue;
				}
			} catch (...) {
			}
			if (!Value::ValuesAreEqual(value, verify_val)) {
				return StringUtil::Format(
				    "Error in result on row %lld column %lld: expected value \"%s\" but got value \"%s\"", r + 1, c + 1,
				    verify_val.ToString().c_str(), value.ToString().c_str());
			}
		}
	}
	return string();
}

void InterpretedBenchmark::Interrupt(BenchmarkState *state_p) {
	auto &state = (InterpretedBenchmarkState &)*state_p;
	state.con.Interrupt();
}

string InterpretedBenchmark::BenchmarkInfo() {
	return string();
}

string InterpretedBenchmark::GetLogOutput(BenchmarkState *state_p) {
	auto &state = (InterpretedBenchmarkState &)*state_p;
	auto &profiler = QueryProfiler::Get(*state.con.context);
	return profiler.ToJSON();
}

string InterpretedBenchmark::DisplayName() {
	LoadBenchmark();
	return display_name.empty() ? name : display_name;
}

string InterpretedBenchmark::Group() {
	LoadBenchmark();
	return display_group.empty() ? group : display_group;
}

string InterpretedBenchmark::Subgroup() {
	LoadBenchmark();
	return subgroup;
}

} // namespace duckdb
