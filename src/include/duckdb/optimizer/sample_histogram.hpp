//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/sample_histogram.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/enums/expression_type.hpp"

namespace duckdb {

//! SampleHistogram builds an equi-depth histogram from a column of sample data.
//! It is used for filter selectivity estimation in the optimizer.
class SampleHistogram {
public:
	static constexpr idx_t DEFAULT_NUM_BUCKETS = 64;

	explicit SampleHistogram(LogicalType type, idx_t num_buckets = DEFAULT_NUM_BUCKETS);

	//! Build the histogram from a vector of values extracted from the sample
	void Build(Vector &data, idx_t count);

	//! Estimate selectivity for a comparison filter (returns 0.0 to 1.0)
	double EstimateSelectivity(ExpressionType comparison_type, const Value &constant) const;

	//! Estimate selectivity for a range filter (BETWEEN lower AND upper)
	double EstimateBetweenSelectivity(const Value &lower, const Value &upper) const;

	//! Estimate selectivity for an IN filter
	double EstimateInSelectivity(const vector<Value> &values) const;

	//! Whether the histogram has been successfully built
	bool IsValid() const;

	//! Get the type of values in this histogram
	const LogicalType &GetType() const;

private:
	//! Find the fractional bucket position of a value (0.0 = before all data, 1.0 = after all data)
	double GetBucketPosition(const Value &val) const;

	LogicalType type;
	idx_t num_buckets;
	//! Equi-depth bucket boundaries (sorted). boundaries[i] is the upper bound of bucket i.
	vector<Value> boundaries;
	//! Total count of non-NULL values used to build the histogram
	idx_t total_count;
	//! Count of NULL values in the sample
	idx_t null_count;
	//! Approximate distinct count from the sample
	idx_t distinct_count;
	//! Whether this histogram has been successfully built
	bool valid;
};

} // namespace duckdb
