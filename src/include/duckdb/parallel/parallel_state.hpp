//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/parallel_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

struct ParallelState {
	virtual ~ParallelState() {
	}
};

} // namespace duckdb
