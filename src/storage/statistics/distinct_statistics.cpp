#include "duckdb/storage/statistics/distinct_statistics.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

DistinctStatistics::DistinctStatistics()
    : BaseStatistics(LogicalType::INVALID, StatisticsType::LOCAL_STATS), log(make_unique<HyperLogLog>()) {
}

DistinctStatistics::DistinctStatistics(unique_ptr<HyperLogLog> log)
    : BaseStatistics(LogicalType::INVALID, StatisticsType::LOCAL_STATS), log(move(log)) {
}

unique_ptr<BaseStatistics> DistinctStatistics::Copy() const {
	return make_unique<DistinctStatistics>(log->Copy());
}

void DistinctStatistics::Merge(const BaseStatistics &other_p) {
	BaseStatistics::Merge(other_p);
	auto &other = (const DistinctStatistics &)other_p;
	log->Merge(*other.log);
}

void DistinctStatistics::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	Serialize(writer);
	writer.Finalize();
}

void DistinctStatistics::Serialize(FieldWriter &writer) const {
	log->Serialize(writer);
}

unique_ptr<DistinctStatistics> DistinctStatistics::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto result = Deserialize(reader);
	reader.Finalize();
	return result;
}

unique_ptr<DistinctStatistics> DistinctStatistics::Deserialize(FieldReader &reader) {
	return make_unique<DistinctStatistics>(HyperLogLog::Deserialize(reader));
}

void DistinctStatistics::Update(Vector &v, idx_t count) {
	VectorData vdata;
	v.Orrify(count, vdata);
	Update(vdata, v.GetType(), count);
}

void DistinctStatistics::Update(VectorData &vdata, const LogicalType &type, idx_t count) {
	uint64_t indices[STANDARD_VECTOR_SIZE];
	uint8_t counts[STANDARD_VECTOR_SIZE];

	D_ASSERT(count != 0);
	count = idx_t(SAMPLE_RATE * double(count)) + 1;
	HyperLogLog::ProcessEntries(vdata, type, indices, counts, count);
	log->AddToLog(vdata, count, indices, counts);
}

string DistinctStatistics::ToString() const {
	return StringUtil::Format("[Approx Unique: %s]", to_string(log->Count()));
}

} // namespace duckdb
