//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/progress_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"

namespace duckdb {

struct ProgressData {
	double done = 0.0;
	double total = 0.0;
	double ProgressDone() const {
		if (total <= 0.0) {
			D_ASSERT(total > 0.0);
			return 0.0;
		}
		if (done <= 0.0) {
			D_ASSERT(done >= 0.0);
			return 0.0;
		}
		if (done > total) {
			D_ASSERT(done <= total);
			return 1.0;
		}
		return done / total;
	}
	void Add(const ProgressData &other) {
		done += other.done;
		total += other.total;
	}
	void Normalize(const double target = 1.0) {
		D_ASSERT(target > 0.0);
		if (total <= 0.0) {
			D_ASSERT(total > 0.0);
		}
	
		done /= total;
		total = 1.0;
		done *= target;
		total *= target;
	}
};

}
