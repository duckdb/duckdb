#include "benchmark_runner.hpp"

#include "duckdb/common/profiler.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb.hpp"
#include "duckdb_benchmark.hpp"

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "re2/re2.h"

#include <fstream>
#include <sstream>
#include <thread>

using namespace duckdb;
using namespace std;

void BenchmarkRunner::RegisterBenchmark(Benchmark *benchmark) {
	GetInstance().benchmarks.push_back(benchmark);
}

Benchmark::Benchmark(bool register_benchmark, string name, string group) : name(name), group(group) {
	if (register_benchmark) {
		BenchmarkRunner::RegisterBenchmark(this);
	}
}

void BenchmarkRunner::SaveDatabase(DuckDB &db, string name) {
	auto &fs = *db.file_system;
	// check if the database directory exists; if not create it
	if (!fs.DirectoryExists(DUCKDB_BENCHMARK_DIRECTORY)) {
		fs.CreateDirectory(DUCKDB_BENCHMARK_DIRECTORY);
	}
	// first export the schema
	// create two files, "[name].sql" and "[name].list"
	// [name].sql contains the SQL used to re-create the tables
	// [name].list contains a list of the exported tables
	ofstream sql_file(fs.JoinPath(DUCKDB_BENCHMARK_DIRECTORY, name + ".sql"));
	ofstream list_file(fs.JoinPath(DUCKDB_BENCHMARK_DIRECTORY, name + ".list"));

	vector<string> table_list;
	Connection con(db);
	auto result = con.Query("SELECT name, sql FROM sqlite_master()");
	for (auto &row : *result) {
		auto table_name = row.GetValue<string>(0);
		auto table_sql = row.GetValue<string>(1);
		table_list.push_back(table_name);

		list_file << table_name << std::endl;
		sql_file << table_sql << std::endl;
	}
	sql_file.close();
	list_file.close();

	// now for each table, write it to a separate file "[name]_[tablename].csv"
	for (auto &table : table_list) {
		auto target_path = fs.JoinPath(DUCKDB_BENCHMARK_DIRECTORY, name + "_" + table + ".csv");
		result = con.Query("COPY " + table + " TO '" + target_path + "'");
		if (!result->success) {
			throw Exception("Failed to save database: " + result->error);
		}
	}
}

bool BenchmarkRunner::TryLoadDatabase(DuckDB &db, string name) {
	auto &fs = *db.file_system;
	if (!fs.DirectoryExists(DUCKDB_BENCHMARK_DIRECTORY)) {
		return false;
	}
	auto sql_fname = fs.JoinPath(DUCKDB_BENCHMARK_DIRECTORY, name + ".sql");
	auto list_fname = fs.JoinPath(DUCKDB_BENCHMARK_DIRECTORY, name + ".list");
	// check if the [name].list and [name].sql files exist
	if (!fs.FileExists(list_fname) || !fs.FileExists(sql_fname)) {
		return false;
	}
	Connection con(db);
	// the files exist, load the data into the database
	// first load the entire SQL and execute it
	ifstream sql_file(sql_fname);
	std::stringstream buffer;
	buffer << sql_file.rdbuf();
	auto result = con.Query(buffer.str());
	if (!result->success) {
		throw Exception("Failed to load database: " + result->error);
	}
	// now read the tables line by line
	ifstream list_file(list_fname);
	string table_name;
	while (getline(list_file, table_name)) {
		// for each table, copy the files
		auto target_path = fs.JoinPath(DUCKDB_BENCHMARK_DIRECTORY, name + "_" + table_name + ".csv");
		result = con.Query("COPY " + table_name + " FROM '" + target_path + "'");
		if (!result->success) {
			throw Exception("Failed to load database: " + result->error);
		}
	}
	return true;
}

volatile bool is_active = false;
volatile bool timeout = false;

void sleep_thread(Benchmark *benchmark, BenchmarkState *state, int timeout_duration) {
	// timeout is given in seconds
	// we wait 10ms per iteration, so timeout * 100 gives us the amount of
	// iterations
	if (timeout_duration < 0) {
		return;
	}
	for (size_t i = 0; i < (size_t)(timeout_duration * 100) && is_active; i++) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	if (is_active) {
		timeout = true;
		benchmark->Interrupt(state);
	}
}

void BenchmarkRunner::Log(string message) {
	fprintf(stderr, "%s", message.c_str());
}

void BenchmarkRunner::LogLine(string message) {
	fprintf(stderr, "%s\n", message.c_str());
}

void BenchmarkRunner::LogResult(string message) {
	LogLine(message);
	if (out_file.good()) {
		out_file << message << endl;
		out_file.flush();
	}
}

void BenchmarkRunner::LogOutput(string message) {
	if (log_file.good()) {
		log_file << message << endl;
		log_file.flush();
	}
}

void BenchmarkRunner::RunBenchmark(Benchmark *benchmark) {
	Profiler profiler;
	LogLine(string(benchmark->name.size() + 6, '-'));
	LogLine("|| " + benchmark->name + " ||");
	LogLine(string(benchmark->name.size() + 6, '-'));
	auto state = benchmark->Initialize();
	auto nruns = benchmark->NRuns();
	for (size_t i = 0; i < nruns + 1; i++) {
		bool hotrun = i > 0;
		if (hotrun) {
			Log(StringUtil::Format("%d/%d...", i, nruns));
		} else {
			Log("Cold run...");
		}
		if (hotrun && benchmark->RequireReinit()) {
			state = benchmark->Initialize();
		}
		is_active = true;
		timeout = false;
		thread interrupt_thread(sleep_thread, benchmark, state.get(), benchmark->Timeout());

		profiler.Start();
		benchmark->Run(state.get());
		profiler.End();

		benchmark->Cleanup(state.get());

		is_active = false;
		interrupt_thread.join();
		if (hotrun) {
			LogOutput(benchmark->GetLogOutput(state.get()));
			if (timeout) {
				// write timeout
				LogResult("TIMEOUT");
				break;
			} else {
				// write time
				auto verify = benchmark->Verify(state.get());
				if (!verify.empty()) {
					LogResult("INCORRECT");
					LogLine("INCORRECT RESULT: " + verify);
					LogOutput("INCORRECT RESULT: " + verify);
					break;
				} else {
					LogResult(to_string(profiler.Elapsed()));
				}
			}
		} else {
			LogLine("DONE");
		}
	}
	benchmark->Finalize();
}

void BenchmarkRunner::RunBenchmarks() {
	LogLine("Starting benchmark run.");
	for (auto &benchmark : benchmarks) {
		RunBenchmark(benchmark);
	}
}

void print_help() {
	fprintf(stderr, "Usage: benchmark_runner\n");
	fprintf(stderr, "              --list         Show a list of all benchmarks\n");
	fprintf(stderr, "              --out=[file]   Move benchmark output to file\n");
	fprintf(stderr, "              --log=[file]   Move log output to file\n");
	fprintf(stderr, "              --info         Prints info about the benchmark\n");
	fprintf(stderr, "              --group        Prints group name of the benchmark\n");
	fprintf(stderr, "              --query        Prints query of the benchmark\n");
	fprintf(stderr, "              [name_pattern] Run only the benchmark which names match the specified name pattern, "
	                "e.g., DS.* for TPC-DS benchmarks\n");
}

enum class BenchmarkMetaType { NONE, INFO, GROUP, QUERY };

struct BenchmarkConfiguration {
	std::string name_pattern{};
	BenchmarkMetaType meta = BenchmarkMetaType::NONE;
};

enum ConfigurationError { None, BenchmarkNotFound, InfoWithoutBenchmarkName };

/**
 * Builds a configuration based on the passed arguments.
 */
BenchmarkConfiguration parse_arguments(const int arg_counter, char const *const *arg_values) {
	auto &instance = BenchmarkRunner::GetInstance();
	auto &benchmarks = instance.benchmarks;
	BenchmarkConfiguration configuration;
	for (int arg_index = 1; arg_index < arg_counter; ++arg_index) {
		string arg = arg_values[arg_index];
		if (arg == "--list") {
			// list names of all benchmarks
			for (auto &benchmark : benchmarks) {
				if (StringUtil::StartsWith(benchmark->name, "sqlite_")) {
					continue;
				}
				fprintf(stdout, "%s\n", benchmark->name.c_str());
			}
			exit(0);
		} else if (arg == "--info") {
			// write info of benchmark
			configuration.meta = BenchmarkMetaType::INFO;
		} else if (arg == "--group") {
			// write group of benchmark
			configuration.meta = BenchmarkMetaType::GROUP;
		} else if (arg == "--query") {
			// write group of benchmark
			configuration.meta = BenchmarkMetaType::QUERY;
		} else if (StringUtil::StartsWith(arg, "--out=") || StringUtil::StartsWith(arg, "--log=")) {
			auto splits = StringUtil::Split(arg, '=');
			if (splits.size() != 2) {
				print_help();
				exit(1);
			}
			auto &file = StringUtil::StartsWith(arg, "--out=") ? instance.out_file : instance.log_file;
			file.open(splits[1]);
			if (!file.good()) {
				fprintf(stderr, "Could not open file %s for writing\n", splits[1].c_str());
				exit(1);
			}
		} else {
			if (!configuration.name_pattern.empty()) {
				fprintf(stderr, "Only one benchmark can be specified.\n");
				print_help();
				exit(1);
			}
			configuration.name_pattern = arg;
		}
	}
	return configuration;
}

/**
 * Runs the benchmarks specified by the configuration if possible.
 * Returns an configuration error code.
 */
ConfigurationError run_benchmarks(const BenchmarkConfiguration &configuration) {
	auto &instance = BenchmarkRunner::GetInstance();
	auto &benchmarks = instance.benchmarks;
	if (!configuration.name_pattern.empty()) {
		// run only benchmarks which names matches the
		// passed name pattern.
		std::vector<int> benchmark_indices{};
		benchmark_indices.reserve(benchmarks.size());
		for (idx_t index = 0; index < benchmarks.size(); ++index) {
			if (RE2::FullMatch(benchmarks[index]->name, configuration.name_pattern)) {
				benchmark_indices.emplace_back(index);
			}
		}
		benchmark_indices.shrink_to_fit();
		if (benchmark_indices.empty()) {
			return ConfigurationError::BenchmarkNotFound;
		}
		if (configuration.meta == BenchmarkMetaType::INFO) {
			// print info of benchmarks
			for (const auto &benchmark_index : benchmark_indices) {
				auto info = benchmarks[benchmark_index]->GetInfo();
				fprintf(stdout, "%s\n", info.c_str());
			}
		} else if (configuration.meta == BenchmarkMetaType::GROUP) {
			// print group of benchmarks
			for (const auto &benchmark_index : benchmark_indices) {
				fprintf(stdout, "%s\n", benchmarks[benchmark_index]->group.c_str());
			}
		} else if (configuration.meta == BenchmarkMetaType::QUERY) {
			for (const auto &benchmark_index : benchmark_indices) {
				auto duckdb_benchmark = dynamic_cast<DuckDBBenchmark *>(benchmarks[benchmark_index]);
				if (!duckdb_benchmark) {
					continue;
				}
				fprintf(stdout, "%s\n", duckdb_benchmark->GetQuery().c_str());
			}
		} else {
			for (const auto &benchmark_index : benchmark_indices) {
				instance.RunBenchmark(benchmarks[benchmark_index]);
			}
		}
	} else {
		if (configuration.meta != BenchmarkMetaType::NONE) {
			return ConfigurationError::InfoWithoutBenchmarkName;
		}
		// default: run all benchmarks
		instance.RunBenchmarks();
	}
	return ConfigurationError::None;
}

void print_error_message(const ConfigurationError &error) {
	switch (error) {
	case ConfigurationError::BenchmarkNotFound:
		fprintf(stderr, "Benchmark to run could not be found.\n");
		break;
	case ConfigurationError::InfoWithoutBenchmarkName:
		fprintf(stderr, "Info requires benchmark name pattern.\n");
		break;
	case ConfigurationError::None:
		break;
	}
	print_help();
}

int main(int argc, char **argv) {
	BenchmarkConfiguration configuration = parse_arguments(argc, argv);
	const auto configuration_error = run_benchmarks(configuration);
	if (configuration_error != ConfigurationError::None) {
		print_error_message(configuration_error);
		exit(1);
	}
}
