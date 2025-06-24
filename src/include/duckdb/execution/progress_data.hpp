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
	bool invalid = false;

public:
	double ProgressDone() const {
		// ProgressDone requires a valid state
		D_ASSERT(IsValid());

		return done / total;
	}

	void Add(const ProgressData &other) {
		// Add is unchecked, propagating invalid
		done += other.done;
		total += other.total;
		invalid = invalid || other.invalid;
	}
	void Normalize(const double target = 1.0) {
		// Normalize checks only `target`, propagating invalid
		D_ASSERT(target > 0.0);
		if (IsValid()) {
			if (total > 0.0) {
				done /= total;
			}
			total = 1.0;
			done *= target;
			total *= target;
		} else {
			SetInvalid();
		}
	}
	void SetInvalid() {
		invalid = true;
		done = 0.0;
		total = 1.0;
	}
	bool IsValid() const {
		return (!invalid) && (done >= 0.0) && (done <= total) && (total >= 0.0);
	}
};

} // namespace duckdb
