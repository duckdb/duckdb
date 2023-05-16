#include "interpreted_benchmark.hpp"

#include "benchmark_runner.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/helper.hpp"

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
	duckdb::unique_ptr<DBConfig> benchmark_config;
	DuckDB db;
	Connection con;
	duckdb::unique_ptr<MaterializedQueryResult> result;

	explicit InterpretedBenchmarkState(string path)
	    : benchmark_config(GetBenchmarkConfig()), db(path.empty() ? nullptr : path.c_str(), benchmark_config.get()),
	      con(db) {
		auto &instance = BenchmarkRunner::GetInstance();
		auto res = con.Query("PRAGMA threads=" + to_string(instance.threads));
		D_ASSERT(!res->HasError());
	}

	duckdb::unique_ptr<DBConfig> GetBenchmarkConfig() {
		auto result = make_uniq<DBConfig>();
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

void InterpretedBenchmark::ReadResultFromFile(BenchmarkFileReader &reader, const string &file) {
	// read the results from the file
	DuckDB db;
	Connection con(db);
	auto result =
	    con.Query("SELECT * FROM read_csv_auto('" + file + "', delim='|', header=1, nullstr='NULL', all_varchar=1)");
	result_column_count = result->ColumnCount();
	for (auto &row : *result) {
		vector<string> row_values;
		for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
			row_values.push_back(row.GetValue<string>(col_idx));
		}
		result_values.push_back(std::move(row_values));
	}
}

void InterpretedBenchmark::ReadResultFromReader(BenchmarkFileReader &reader, const string &header) {
	result_column_count = header.size();
	// keep reading results until eof
	string line;
	while (reader.ReadLine(line)) {
		if (line.empty()) {
			break;
		}
		auto result_splits = StringUtil::Split(line, "\t");
		if ((int64_t)result_splits.size() != result_column_count) {
			throw std::runtime_error(reader.FormatException("expected " + std::to_string(result_splits.size()) +
			                                                " values but got " + std::to_string(result_column_count)));
		}
		result_values.push_back(std::move(result_splits));
	}
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

				auto buffer = make_unsafe_uniq_array<char>(size);
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
			cache_db = splits[1];
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
		} else if (splits[0] == "result_query") {
			if (result_column_count > 0) {
				throw std::runtime_error(reader.FormatException("multiple results found"));
			}
			// count the amount of columns
			if (splits.size() <= 1 || splits[1].size() == 0) {
				throw std::runtime_error(
				    reader.FormatException("result_query must be followed by a column count (e.g. result III)"));
			}
			bool is_file = false;
			for (idx_t i = 0; i < splits[1].size(); i++) {
				if (splits[1][i] != 'i') {
					is_file = true;
					break;
				}
			}
			if (is_file) {
				ReadResultFromFile(reader, splits[1]);
			}
			// read the actual query
			bool found_end = false;
			string sql;
			while (reader.ReadLine(line)) {
				if (line == "----") {
					found_end = true;
					break;
				}
				sql += "\n" + line;
			}
			result_query = sql;
			if (!found_end) {
				throw std::runtime_error(reader.FormatException(
				    "result_query must be followed by a query and a result (separated by ----)"));
			}
			if (!is_file) {
				ReadResultFromReader(reader, splits[1]);
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
				ReadResultFromFile(reader, splits[1]);

				// read the main file until we encounter an empty line
				string line;
				while (reader.ReadLine(line)) {
					if (line.empty()) {
						break;
					}
				}
			} else {
				ReadResultFromReader(reader, splits[1]);
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
	duckdb::unique_ptr<QueryResult> result;
	LoadBenchmark();
	duckdb::unique_ptr<InterpretedBenchmarkState> state;
	auto full_db_path = GetDatabasePath();
	try {
		state = make_uniq<InterpretedBenchmarkState>(full_db_path);
	} catch (Exception &e) {
		// if the connection throws an error, chances are it's a storage format error.
		// In this case delete the file and connect again.
		DeleteDatabase(full_db_path);
		state = make_uniq<InterpretedBenchmarkState>(full_db_path);
	}
	extensions.insert("parquet");
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
			if (result->HasError()) {
				result->ThrowError();
			}
			result = std::move(result->next);
		}
	}

	string load_query;
	if (queries.find("load") != queries.end()) {
		load_query = queries["load"];
	}

	if (cache_db.empty() && cache_db.compare(DEFAULT_DB_PATH) != 0) {
		// no cache or db_path specified: just run the initialization code
		result = state->con.Query(load_query);
	} else {
		// cache or db_path is specified: try to load from one of them
		bool in_memory_db_has_data = false;
		if (!cache_db.empty()) {
			// Currently connected to a cached db. check if any tables exist.
			// If tables exist, it's a good indication that the database is fine
			// If they don't load the database
			auto result = state->con.Query("SHOW TABLES;");
			if (result->HasError()) {
				result->ThrowError();
			}
			if (result->RowCount() > 0) {
				in_memory_db_has_data = true;
			}
		}
		if (!in_memory_db_has_data) {
			// failed to load: write the cache
			result = state->con.Query(load_query);
		}
	}
	while (result) {
		if (result->HasError()) {
			result->ThrowError();
		}
		result = std::move(result->next);
	}
	if (config.profile_info == BenchmarkProfileInfo::NORMAL) {
		state->con.Query("PRAGMA enable_profiling");
	} else if (config.profile_info == BenchmarkProfileInfo::DETAILED) {
		state->con.Query("PRAGMA enable_profiling");
		state->con.Query("PRAGMA profiling_mode='detailed'");
	}
	return std::move(state);
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
		duckdb::unique_ptr<QueryResult> result;
		string cleanup_query = queries["cleanup"];
		result = state.con.Query(cleanup_query);
		while (result) {
			if (result->HasError()) {
				result->ThrowError();
			}
			result = std::move(result->next);
		}
	}
}

string InterpretedBenchmark::GetDatabasePath() {
	auto fs = FileSystem::CreateLocal();
	if (!cache_db.empty()) {
		return fs->JoinPath(BenchmarkRunner::DUCKDB_BENCHMARK_DIRECTORY, cache_db);
	}
	if (in_memory) {
		return "";
	}
	auto db_path = fs->JoinPath(BenchmarkRunner::DUCKDB_BENCHMARK_DIRECTORY, DEFAULT_DB_PATH);
	DeleteDatabase(db_path);
	return db_path;
}

string InterpretedBenchmark::VerifyInternal(BenchmarkState *state_p, MaterializedQueryResult &result) {
	auto &state = (InterpretedBenchmarkState &)*state_p;
	// compare the column count
	if (result_column_count >= 0 && (int64_t)result.ColumnCount() != result_column_count) {
		return StringUtil::Format("Error in result: expected %lld columns but got %lld\nObtained result: %s",
		                          (int64_t)result_column_count, (int64_t)result.ColumnCount(), result.ToString());
	}
	// compare row count
	if (result.RowCount() != result_values.size()) {
		return StringUtil::Format("Error in result: expected %lld rows but got %lld\nObtained result: %s",
		                          (int64_t)result_values.size(), (int64_t)result.RowCount(), result.ToString());
	}
	// compare values
	for (int64_t r = 0; r < (int64_t)result_values.size(); r++) {
		for (int64_t c = 0; c < result_column_count; c++) {
			auto value = result.GetValue(c, r);
			if (result_values[r][c] == "NULL" && value.IsNull()) {
				continue;
			}
			if (result_values[r][c] == value.ToString()) {
				continue;
			}
			if (result_values[r][c] == "(empty)" && (value.ToString() == "" || value.IsNull())) {
				continue;
			}

			Value verify_val(result_values[r][c]);
			try {
				verify_val = verify_val.CastAs(*state.con.context, value.type());
			} catch (...) {
			}
			if (!Value::ValuesAreEqual(*state.con.context, verify_val, value)) {
				return StringUtil::Format("Error in result on row %lld column %lld: expected value \"%s\" but got "
				                          "value \"%s\"\nObtained result:\n%s",
				                          r + 1, c + 1, verify_val.ToString().c_str(), value.ToString().c_str(),
				                          result.ToString().c_str());
			}
		}
	}
	return string();
}

string InterpretedBenchmark::Verify(BenchmarkState *state_p) {
	if (result_column_count == 0) {
		// no result specified
		return string();
	}
	auto &state = (InterpretedBenchmarkState &)*state_p;
	if (state.result->HasError()) {
		return state.result->GetError();
	}
	if (!result_query.empty()) {
		// we are running a result query
		// store the current result in a table called "__answer"
		auto &collection = state.result->Collection();
		auto &names = state.result->names;
		auto &types = state.result->types;
		// first create the (empty) table
		string create_tbl = "CREATE OR REPLACE TEMP TABLE __answer(";
		for (idx_t i = 0; i < names.size(); i++) {
			if (i > 0) {
				create_tbl += ", ";
			}
			create_tbl += KeywordHelper::WriteOptionallyQuoted(names[i]);
			create_tbl += " ";
			create_tbl += types[i].ToString();
		}
		create_tbl += ")";
		auto new_result = state.con.Query(create_tbl);
		if (new_result->HasError()) {
			return new_result->GetError();
		}
		// now append the result to the answer table
		auto table_info = state.con.TableInfo("__answer");
		if (table_info == nullptr) {
			throw std::runtime_error("Received a nullptr when querying table info of __answer");
		}
		state.con.Append(*table_info, collection);

		// finally run the result query and verify the result of that query
		new_result = state.con.Query(result_query);
		if (new_result->HasError()) {
			return new_result->GetError();
		}
		return VerifyInternal(state_p, *new_result);
	} else {
		return VerifyInternal(state_p, *state.result);
	}
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
