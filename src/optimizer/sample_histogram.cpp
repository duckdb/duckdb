#include "duckdb/optimizer/sample_histogram.hpp"

#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <cmath>

namespace duckdb {

SampleHistogram::SampleHistogram(LogicalType type_p, idx_t num_buckets_p)
    : type(std::move(type_p)), num_buckets(num_buckets_p), total_count(0), null_count(0), distinct_count(0),
      valid(false) {
}

void SampleHistogram::Build(Vector &data, idx_t count) {
	valid = false;
	boundaries.clear();
	total_count = 0;
	null_count = 0;
	distinct_count = 0;

	if (count == 0) {
		return;
	}

	// Extract non-NULL values into a list
	vector<Value> values;
	values.reserve(count);

	UnifiedVectorFormat vdata;
	data.ToUnifiedFormat(count, vdata);

	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			null_count++;
			continue;
		}
		values.push_back(data.GetValue(i));
	}

	total_count = values.size();
	if (total_count == 0) {
		// All NULLs
		valid = true;
		return;
	}

	// Sort the values
	std::sort(values.begin(), values.end(), [](const Value &a, const Value &b) { return a < b; });

	// Count distinct values
	distinct_count = 1;
	for (idx_t i = 1; i < values.size(); i++) {
		if (values[i] != values[i - 1]) {
			distinct_count++;
		}
	}

	// Build equi-depth boundaries
	// Use min(num_buckets, distinct_count) actual buckets
	idx_t actual_buckets = MinValue<idx_t>(num_buckets, total_count);
	if (actual_buckets == 0) {
		return;
	}

	boundaries.reserve(actual_buckets);
	for (idx_t i = 0; i < actual_buckets; i++) {
		// The upper bound of bucket i is at position ((i+1) * total_count / actual_buckets) - 1
		idx_t boundary_idx = ((i + 1) * total_count / actual_buckets) - 1;
		boundary_idx = MinValue<idx_t>(boundary_idx, total_count - 1);
		boundaries.push_back(values[boundary_idx]);
	}

	valid = true;
}

double SampleHistogram::GetBucketPosition(const Value &val) const {
	if (boundaries.empty()) {
		return 0.5;
	}

	// Binary search for the bucket containing val
	// Find first boundary >= val
	idx_t lo = 0;
	idx_t hi = boundaries.size();
	while (lo < hi) {
		idx_t mid = (lo + hi) / 2;
		if (boundaries[mid] < val) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}

	// lo is now the index of the first boundary >= val
	if (lo == 0) {
		// val <= boundaries[0] (first bucket)
		// Could be at the very start
		if (val < boundaries[0]) {
			return 0.0;
		}
		// val == boundaries[0]
		return 1.0 / static_cast<double>(boundaries.size());
	}

	if (lo >= boundaries.size()) {
		// val > all boundaries
		return 1.0;
	}

	// val is between boundaries[lo-1] and boundaries[lo]
	// Interpolate within this bucket
	double bucket_start = static_cast<double>(lo) / static_cast<double>(boundaries.size());
	double bucket_end = static_cast<double>(lo + 1) / static_cast<double>(boundaries.size());

	// For now, simple linear interpolation within the bucket
	// Since equi-depth buckets have approximately equal counts, this is reasonable
	return (bucket_start + bucket_end) / 2.0;
}

double SampleHistogram::EstimateSelectivity(ExpressionType comparison_type, const Value &constant) const {
	if (!valid || total_count == 0) {
		return -1.0; // Signal that we have no estimate
	}

	// Cast the constant to the histogram's type to ensure correct comparisons
	Value casted_constant = constant;
	if (casted_constant.type() != type) {
		if (!casted_constant.DefaultTryCastAs(type)) {
			return -1.0; // Cannot cast, no estimate available
		}
	}

	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL: {
		// Uniform assumption within the sample: 1/distinct_count
		if (distinct_count > 0) {
			return 1.0 / static_cast<double>(distinct_count);
		}
		return 1.0;
	}
	case ExpressionType::COMPARE_LESSTHAN: {
		double pos = GetBucketPosition(casted_constant);
		// Strictly less than: same as the position (fraction of values below)
		return MaxValue(pos, 0.0);
	}
	case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
		double pos = GetBucketPosition(casted_constant);
		// Less than or equal: position plus half a bucket for the equal part
		// In practice, for continuous data this is very close to just pos
		double half_bucket = 0.5 / static_cast<double>(boundaries.size());
		return MinValue(pos + half_bucket, 1.0);
	}
	case ExpressionType::COMPARE_GREATERTHAN: {
		double pos = GetBucketPosition(casted_constant);
		return MaxValue(1.0 - pos, 0.0);
	}
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		double pos = GetBucketPosition(casted_constant);
		double half_bucket = 0.5 / static_cast<double>(boundaries.size());
		return MinValue(1.0 - pos + half_bucket, 1.0);
	}
	default:
		return -1.0; // No estimate available
	}
}

double SampleHistogram::EstimateBetweenSelectivity(const Value &lower, const Value &upper) const {
	if (!valid || total_count == 0) {
		return -1.0;
	}

	// Cast bounds to histogram type
	Value casted_lower = lower;
	Value casted_upper = upper;
	if (!casted_lower.DefaultTryCastAs(type) || !casted_upper.DefaultTryCastAs(type)) {
		return -1.0;
	}

	double lower_pos = GetBucketPosition(casted_lower);
	double upper_pos = GetBucketPosition(casted_upper);

	double selectivity = upper_pos - lower_pos;
	// Add a small amount for the boundary values themselves
	double half_bucket = 0.5 / static_cast<double>(boundaries.size());
	selectivity += half_bucket;

	return MaxValue(MinValue(selectivity, 1.0), 0.0);
}

double SampleHistogram::EstimateInSelectivity(const vector<Value> &values) const {
	if (!valid || total_count == 0 || distinct_count == 0) {
		return -1.0;
	}

	// Each value in the IN list has selectivity 1/distinct_count
	// But values may overlap, so cap at 1.0
	double per_value = 1.0 / static_cast<double>(distinct_count);
	double selectivity = static_cast<double>(values.size()) * per_value;
	return MinValue(selectivity, 1.0);
}

bool SampleHistogram::IsValid() const {
	return valid;
}

const LogicalType &SampleHistogram::GetType() const {
	return type;
}

} // namespace duckdb
