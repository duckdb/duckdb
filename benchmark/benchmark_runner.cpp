
#include "benchmark_runner.hpp"
#include "common/profiler.hpp"
#include "common/string_util.hpp"

#include <thread>

using namespace duckdb;
using namespace std;

void BenchmarkRunner::RegisterBenchmark(Benchmark *benchmark) {
	GetInstance().benchmarks.push_back(benchmark);
}

Benchmark::Benchmark(std::string name, std::string group)
    : name(name), group(group) {
	BenchmarkRunner::RegisterBenchmark(this);
}

volatile bool is_active = false;
bool timeout = false;

void sleep_thread(Benchmark *benchmark, BenchmarkState *state, int timeout) {
	// timeout is given in seconds
	// we wait 10ms per iteration, so timeout * 100 gives us the amount of
	// iterations
	for (size_t i = 0; i < timeout * 100 && is_active; i++) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	if (is_active) {
		timeout = true;
		benchmark->Interrupt(state);
	}
}

void BenchmarkRunner::Log(std::string message) {
	fprintf(stderr, "%s", message.c_str());
}

void BenchmarkRunner::LogLine(std::string message) {
	fprintf(stderr, "%s\n", message.c_str());
}

void BenchmarkRunner::RunBenchmark(Benchmark *benchmark) {
	Profiler profiler;
	LogLine(string(benchmark->name.size() + 6, '-'));
	LogLine("|| " + benchmark->name + " ||");
	LogLine(string(benchmark->name.size() + 6, '-'));
	Log("Cold run...");
	auto state = benchmark->Initialize();
	benchmark->Run(state.get());
	auto verify = benchmark->Verify(state.get());
	if (!verify.empty()) {
		LogLine("INCORRECT RESULT: " + verify);
		return;
	}
	LogLine("DONE");
	auto nruns = benchmark->NRuns();
	for (size_t i = 0; i < nruns; i++) {
		Log(StringUtil::Format("%d/%d...", i + 1, nruns));
		if (benchmark->RequireReinit()) {
			state = benchmark->Initialize();
		}
		is_active = true;
		timeout = false;
		thread interrupt_thread(sleep_thread, benchmark, state.get(),
		                        benchmark->Timeout());

		profiler.Start();
		benchmark->Run(state.get());
		profiler.End();

		is_active = false;
		interrupt_thread.join();
		if (timeout) {
			// write timeout
			LogLine("TIMEOUT");
			break;
		} else {
			// write time
			LogLine(StringUtil::Format("DONE (%fs)", profiler.Elapsed()));
		}
		auto verify = benchmark->Verify(state.get());
		if (!verify.empty()) {
			LogLine("INCORRECT RESULT: " + verify);
			break;
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
	fprintf(stderr, "              --list   Show a list of all benchmarks\n");
	fprintf(stderr, "              [name]   Run only the benchmark of the "
	                "specified name\n");
}

int main(int argc, char **argv) {
	auto &instance = BenchmarkRunner::GetInstance();
	auto &benchmarks = instance.benchmarks;
	if (argc > 1) {
		string arg = argv[1];
		if (arg == "--list") {
			// list names of all benchmarks
			for (auto &benchmark : benchmarks) {
				fprintf(stderr, "%s\n", benchmark->name.c_str());
			}
			exit(0);
		} else {
			// run only specific benchmark
			// check if the benchmark exists
			for (size_t i = 0; i < benchmarks.size(); i++) {
				if (benchmarks[i]->name == arg) {
					instance.RunBenchmark(benchmarks[i]);
					exit(0);
				}
			}
			// benchmark to run could not be found
			print_help();
			exit(1);
		}
	}
	// default: run all benchmarks
	instance.RunBenchmarks();
}
