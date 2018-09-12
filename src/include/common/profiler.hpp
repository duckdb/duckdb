//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/profiler.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <chrono>

#include "common/helper.hpp"

namespace duckdb {

//! The profiler can be used to measure elapsed time
class Profiler {
  public:
	//! Starts the timer
	void Start() { start = std::chrono::system_clock::now(); }
	//! Finishes timing
	void End() { end = std::chrono::system_clock::now(); }

	//! Returns the elapsed time in seconds
	double Elapsed() const {
		return std::chrono::duration_cast<std::chrono::duration<double>>(end -
		                                                                 start)
		    .count();
	}

  private:
	std::chrono::time_point<std::chrono::system_clock> start;
	std::chrono::time_point<std::chrono::system_clock> end;
};
} // namespace duckdb
