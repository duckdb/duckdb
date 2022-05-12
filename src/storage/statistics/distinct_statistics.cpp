#include "duckdb/storage/statistics/distinct_statistics.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/string_util.hpp"

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
	count = MaxValue<idx_t>(idx_t(SAMPLE_RATE * double(count)), 1);
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
	// Estimate HLL count because we use sampling
	double hll_count = log->Count();
	double unique_proportion = hll_count / double(sample_count);
	double actual_sample_rate = double(sample_count) / double(total_count);
	double multiplier = double(1) + unique_proportion * (double(1) / actual_sample_rate - double(1));
	return idx_t(multiplier * hll_count);
}

} // namespace duckdb
