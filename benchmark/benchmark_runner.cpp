#include "benchmark_runner.hpp"

#include "common/profiler.hpp"
#include "common/string_util.hpp"

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

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
	fprintf(stderr, "              --list       Show a list of all benchmarks\n");
	fprintf(stderr, "              --out=[file] Move benchmark output to file\n");
	fprintf(stderr, "              --log=[file] Move log output to file\n");
	fprintf(stderr, "              --info       Prints info about the benchmark\n");
	fprintf(stderr, "              [name]       Run only the benchmark of the "
	                "specified name\n");
}

int main(int argc, char **argv) {
	auto &instance = BenchmarkRunner::GetInstance();
	auto &benchmarks = instance.benchmarks;
	int benchmark_index = -1;
	bool info = false;

	for (int i = 1; i < argc; i++) {
		string arg = argv[i];
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
			info = true;
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
			if (benchmark_index >= 0) {
				fprintf(stderr, "ERROR: Can only specify one benchmark.\n");
				print_help();
				exit(1);
			}
			// run only specific benchmark
			// check if the benchmark exists
			for (size_t i = 0; i < benchmarks.size(); i++) {
				if (benchmarks[i]->name == arg) {
					benchmark_index = i;
					break;
				}
			}
			if (benchmark_index < 0) {
				// benchmark to run could not be found
				print_help();
				exit(1);
			}
		}
	}
	if (benchmark_index < 0) {
		if (info) {
			fprintf(stderr, "ERROR: Info requires benchmark name.\n");
			print_help();
			exit(1);
		}
		// default: run all benchmarks
		instance.RunBenchmarks();
	} else {
		if (info) {
			// print info of benchmark
			auto info = benchmarks[benchmark_index]->GetInfo();
			fprintf(stdout, "%s\n", info.c_str());
		} else {
			instance.RunBenchmark(benchmarks[benchmark_index]);
		}
	}
}
