#include "duckdb/storage/statistics/distinct_statistics.hpp"

#include "duckdb/common/string_util.hpp"

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
	log = log->Merge(*other.log);
	sample_count += other.sample_count;
	total_count += other.total_count;
}

void DistinctStatistics::Update(Vector &v, idx_t count, bool sample) {
	UnifiedVectorFormat vdata;
	v.ToUnifiedFormat(count, vdata);
	Update(vdata, v.GetType(), count, sample);
}

void DistinctStatistics::Update(UnifiedVectorFormat &vdata, const LogicalType &type, idx_t count, bool sample) {
	if (count == 0) {
		return;
	}

	total_count += count;
	if (sample) {
		count = MinValue<idx_t>(idx_t(SAMPLE_RATE * MaxValue<idx_t>(STANDARD_VECTOR_SIZE, count)), count);
	}
	sample_count += count;

	uint64_t indices[STANDARD_VECTOR_SIZE];
	uint8_t counts[STANDARD_VECTOR_SIZE];

	HyperLogLog::ProcessEntries(vdata, type, indices, counts, count);
	log->AddToLog(vdata, count, indices, counts);
}

string DistinctStatistics::ToString() const {
	return StringUtil::Format("[Approx Unique: %s]", to_string(GetCount()));
}

idx_t DistinctStatistics::GetCount() const {
	if (sample_count == 0 || total_count == 0) {
		return 0;
	}

	double u = MinValue<idx_t>(log->Count(), sample_count);
	double s = sample_count;
	double n = total_count;

	// Assume this proportion of the the sampled values occurred only once
	double u1 = pow(u / s, 2) * u;

	// Estimate total uniques using Good Turing Estimation
	idx_t estimate = NumericCast<idx_t>(u + u1 / s * (n - s));
	return MinValue<idx_t>(estimate, total_count);
}

bool DistinctStatistics::TypeIsSupported(const LogicalType &type) {
	auto physical_type = type.InternalType();
	return physical_type != PhysicalType::LIST && physical_type != PhysicalType::STRUCT &&
	       physical_type != PhysicalType::ARRAY;
}

} // namespace duckdb
