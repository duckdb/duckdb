//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/cycle_counter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/chrono.hpp"

namespace duckdb {

//! The cycle counter can be used to measure elapsed cycles for a function, expression and ...
//! Optimized by sampling mechanism. Once per 100 times.
//! //Todo Can be optimized further by calling RDTSC once per sample
class CycleCounter {
	friend struct ExpressionInfo;
	friend struct ExpressionRootInfo;
	static constexpr int SAMPLING_RATE = 50;

public:
	CycleCounter() {
	}
	// Next_sample determines if a sample needs to be taken, if so start the profiler
	void BeginSample() {
		if (current_count >= next_sample) {
			tmp = Tick();
		}
	}

	// End the sample
	void EndSample(int chunk_size) {
		if (current_count >= next_sample) {
			time += Tick() - tmp;
		}
		if (current_count >= next_sample) {
			next_sample = SAMPLING_RATE;
			++sample_count;
			sample_tuples_count += chunk_size;
			current_count = 0;
		} else {
			++current_count;
		}
		tuples_count += chunk_size;
	}

private:
	uint64_t Tick() const;
	// current number on RDT register
	uint64_t tmp;
	// Elapsed cycles
	uint64_t time = 0;
	//! Count the number of time the executor called since last sampling
	uint64_t current_count = 0;
	//! Show the next sample
	uint64_t next_sample = 0;
	//! Count the number of samples
	uint64_t sample_count = 0;
	//! Count the number of tuples sampled
	uint64_t sample_tuples_count = 0;
	//! Count the number of ALL tuples
	uint64_t tuples_count = 0;
};

} // namespace duckdb
