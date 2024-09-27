#include "duckdb/storage/statistics/distinct_statistics.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <math.h>

namespace duckdb {

DistinctStatistics::DistinctStatistics()
    : log(make_uniq<HyperLogLog>()), sample_count(0), total_count(0),
      hash_vec(LogicalType::HASH, STANDARD_VECTOR_SIZE) {
}

DistinctStatistics::DistinctStatistics(unique_ptr<HyperLogLog> log, idx_t sample_count, idx_t total_count)
    : log(std::move(log)), sample_count(sample_count), total_count(total_count),
      hash_vec(LogicalType::HASH, STANDARD_VECTOR_SIZE) {
}

unique_ptr<DistinctStatistics> DistinctStatistics::Copy() const {
	lock_guard<mutex> guard(lock);
	return make_uniq<DistinctStatistics>(log->Copy(), sample_count, total_count);
}

void DistinctStatistics::Merge(const DistinctStatistics &other) {
	log->Merge(*other.log);
	sample_count += other.sample_count;
	total_count += other.total_count;
}

void DistinctStatistics::Update(Vector &v, idx_t count, bool sample) {
	total_count += count;
	if (sample) {
		const auto original_count = count;
		const auto sample_rate = v.GetType().IsIntegral() ? INTEGRAL_SAMPLE_RATE : BASE_SAMPLE_RATE;
		// Sample up to 'sample_rate' of STANDARD_VECTOR_SIZE of this vector (at least 1)
		count = MaxValue<idx_t>(LossyNumericCast<idx_t>(sample_rate * static_cast<double>(STANDARD_VECTOR_SIZE)), 1);
		// But never more than the original count
		count = MinValue<idx_t>(count, original_count);
	}
	sample_count += count;

	lock_guard<mutex> guard(lock);
	VectorOperations::Hash(v, hash_vec, count);

	UnifiedVectorFormat vdata;
	v.ToUnifiedFormat(count, vdata);

	log->Update(v, hash_vec, count);
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
