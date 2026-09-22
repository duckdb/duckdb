//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/progress_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/constants.hpp"

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

//! Keeps an estimated progress from decreasing, for operators whose total is only known once they finish.
//! The highest fraction reported so far is kept as fixed-point, and is only touched when progress is requested.
struct MonotonicProgress {
public:
	//! Returns the given progress, raised to the highest fraction that was returned before
	ProgressData Update(const ProgressData &progress) {
		if (!progress.IsValid() || progress.total <= 0) {
			return progress;
		}
		auto fraction = static_cast<idx_t>(progress.done / progress.total * static_cast<double>(PRECISION));
		fraction = fraction > PRECISION ? PRECISION : fraction;
		auto previous = max_fraction.load(std::memory_order_relaxed);
		while (fraction > previous &&
		       !max_fraction.compare_exchange_weak(previous, fraction, std::memory_order_relaxed)) {
		}
		auto result_fraction = fraction > previous ? fraction : previous;
		ProgressData result;
		result.done = static_cast<double>(result_fraction) / static_cast<double>(PRECISION) * progress.total;
		result.total = progress.total;
		return result;
	}
	void Reset() {
		max_fraction.store(0, std::memory_order_relaxed);
	}

private:
	static constexpr idx_t PRECISION = 1000000000;
	atomic<idx_t> max_fraction {0};
};

} // namespace duckdb
