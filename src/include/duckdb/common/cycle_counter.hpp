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

//! The cycle counter can be used to measure elapsed cycles
class CycleCounter {
public:
	//! Starts the timer
	void Start() {
		finished = false;
		start = Tick();
	}
	//! Finishes timing
	void End() {
		end = Tick();
		finished = true;
	}

	uint64_t Elapsed() const {
		return end - start;
	}

private:
	uint64_t Tick() const;
	uint64_t start;
	uint64_t end;
	bool finished = false;
};

} // namespace duckdb
