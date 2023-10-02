//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb_benchmark_macro.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_benchmark.hpp"

#define DUCKDB_BENCHMARK(NAME, GROUP)                                                                                  \
	class NAME##Benchmark : public DuckDBBenchmark {                                                                   \
		NAME##Benchmark(bool register_benchmark) : DuckDBBenchmark(register_benchmark, "" #NAME, GROUP) {              \
		}                                                                                                              \
                                                                                                                       \
	public:                                                                                                            \
		static NAME##Benchmark *GetInstance() {                                                                        \
			static NAME##Benchmark singleton(true);                                                                    \
			auto benchmark = duckdb::unique_ptr<DuckDBBenchmark>(new NAME##Benchmark(false));                          \
			return &singleton;                                                                                         \
		}

#define REGISTER_BENCHMARK(NAME) auto global_instance_##NAME = NAME##Benchmark::GetInstance()

#define FINISH_BENCHMARK(NAME)                                                                                         \
	}                                                                                                                  \
	;                                                                                                                  \
	REGISTER_BENCHMARK(NAME);

namespace duckdb {}
