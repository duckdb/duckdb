#include "duckdb/storage/statistics/distinct_statistics.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/string_util.hpp"

#include <math.h>

namespace duckdb {

DistinctStatistics::DistinctStatistics()
    : BaseStatistics(LogicalType::INVALID, StatisticsType::LOCAL_STATS), log(make_unique<HyperLogLog>()),
      sample_count(0), total_count(0) {
}

DistinctStatistics::DistinctStatistics(unique_ptr<HyperLogLog> log, idx_t sample_count, idx_t total_count)
    : BaseStatistics(LogicalType::INVALID, StatisticsType::LOCAL_STATS), log(move(log)), sample_count(sample_count),
      total_count(total_count) {
}

unique_ptr<BaseStatistics> DistinctStatistics::Copy() const {
	return make_unique<DistinctStatistics>(log->Copy(), sample_count, total_count);
}

void DistinctStatistics::Merge(const BaseStatistics &other_p) {
	BaseStatistics::Merge(other_p);
	auto &other = (const DistinctStatistics &)other_p;
	log->Merge(*other.log);
	sample_count += other.sample_count;
	total_count += other.total_count;
}

void DistinctStatistics::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	Serialize(writer);
	writer.Finalize();
}

void DistinctStatistics::Serialize(FieldWriter &writer) const {
	writer.WriteField<idx_t>(sample_count);
	writer.WriteField<idx_t>(total_count);
	log->Serialize(writer);
}

unique_ptr<DistinctStatistics> DistinctStatistics::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto result = Deserialize(reader);
	reader.Finalize();
	return result;
}

unique_ptr<DistinctStatistics> DistinctStatistics::Deserialize(FieldReader &reader) {
	auto sample_count = reader.ReadRequired<idx_t>();
	auto total_count = reader.ReadRequired<idx_t>();
	return make_unique<DistinctStatistics>(HyperLogLog::Deserialize(reader), sample_count, total_count);
}

void DistinctStatistics::Update(Vector &v, idx_t count) {
	VectorData vdata;
	v.Orrify(count, vdata);
	Update(vdata, v.GetType(), count);
}

void DistinctStatistics::Update(VectorData &vdata, const LogicalType &type, idx_t count) {
	if (count == 0) {
		return;
	}
	total_count += count;
	count = MinValue<idx_t>(idx_t(SAMPLE_RATE * MaxValue<idx_t>(STANDARD_VECTOR_SIZE, count)), count);
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
	idx_t estimate = u + u1 / s * (n - s);
	return MinValue<idx_t>(estimate, total_count);
}

} // namespace duckdb
