#include "duckdb/storage/statistics/distinct_statistics.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <math.h>

namespace duckdb {

DistinctStatistics::DistinctStatistics() : log(make_uniq<HyperLogLog>()), sample_count(0), total_count(0) {
}

DistinctStatistics::DistinctStatistics(unique_ptr<HyperLogLog> log, idx_t sample_count, idx_t total_count)
    : log(std::move(log)), sample_count(sample_count), total_count(total_count) {
}

unique_ptr<DistinctStatistics> DistinctStatistics::Copy() const {
	return make_uniq<DistinctStatistics>(log->Copy(), sample_count, total_count);
}

void DistinctStatistics::Merge(const DistinctStatistics &other) {
	log->Merge(*other.log);
	sample_count += other.sample_count;
	total_count += other.total_count;
}

void DistinctStatistics::UpdateSample(Vector &new_data, idx_t count, Vector &hashes) {
	total_count += count;
	const auto original_count = count;
	const auto sample_rate = new_data.GetType().IsIntegral() ? INTEGRAL_SAMPLE_RATE : BASE_SAMPLE_RATE;
	// Sample up to 'sample_rate' of STANDARD_VECTOR_SIZE of this vector (at least 1)
	count = MaxValue<idx_t>(LossyNumericCast<idx_t>(sample_rate * static_cast<double>(STANDARD_VECTOR_SIZE)), 1);
	// But never more than the original count
	count = MinValue<idx_t>(count, original_count);

	UpdateInternal(new_data, count, hashes);
}

void DistinctStatistics::Update(Vector &new_data, idx_t count, Vector &hashes) {
	total_count += count;
	UpdateInternal(new_data, count, hashes);
}

void DistinctStatistics::UpdateInternal(Vector &new_data, idx_t count, Vector &hashes) {
	sample_count += count;
	VectorOperations::Hash(new_data, hashes, count);

	log->Update(new_data, hashes, count);
}

string DistinctStatistics::ToString() const {
	return StringUtil::Format("[Approx Unique: %llu]", GetCount());
}

idx_t DistinctStatistics::GetCount() const {
	if (sample_count == 0 || total_count == 0) {
		return 0;
	}

	double u = static_cast<double>(MinValue<idx_t>(log->Count(), sample_count));
	double s = static_cast<double>(sample_count.load());
	double n = static_cast<double>(total_count.load());

	// Assume this proportion of the the sampled values occurred only once
	double u1 = pow(u / s, 2) * u;

	// Estimate total uniques using Good Turing Estimation
	idx_t estimate = LossyNumericCast<idx_t>(u + u1 / s * (n - s));
	return MinValue<idx_t>(estimate, total_count);
}

bool DistinctStatistics::TypeIsSupported(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::LIST:
	case PhysicalType::STRUCT:
	case PhysicalType::ARRAY:
		return false; // We don't support nested types
	case PhysicalType::BIT:
	case PhysicalType::BOOL:
		return false; // Doesn't make much sense
	default:
		return true;
	}
}

} // namespace duckdb
