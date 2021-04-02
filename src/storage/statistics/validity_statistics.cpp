#include "duckdb/storage/statistics/validity_statistics.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

ValidityStatistics::ValidityStatistics(bool has_null)
    : BaseStatistics(LogicalType(LogicalTypeId::VALIDITY)), has_null(has_null) {
}

unique_ptr<BaseStatistics> ValidityStatistics::Combine(const unique_ptr<BaseStatistics> &lstats,
                                                       const unique_ptr<BaseStatistics> &rstats) {
	if (!lstats && !rstats) {
		return nullptr;
	} else if (!lstats) {
		return rstats->Copy();
	} else if (!rstats) {
		return lstats->Copy();
	} else {
		auto &l = (ValidityStatistics &)*lstats;
		auto &r = (ValidityStatistics &)*rstats;
		return make_unique<ValidityStatistics>(l.has_null || r.has_null);
	}
}

void ValidityStatistics::Merge(const BaseStatistics &other_p) {
	auto &other = (ValidityStatistics &)other_p;
	has_null = has_null || other.has_null;
}

unique_ptr<BaseStatistics> ValidityStatistics::Copy() {
	return make_unique<ValidityStatistics>(has_null);
}

void ValidityStatistics::Serialize(Serializer &serializer) {
	BaseStatistics::Serialize(serializer);
	serializer.Write<bool>(has_null);
}

unique_ptr<BaseStatistics> ValidityStatistics::Deserialize(Deserializer &source) {
	bool has_null = source.Read<bool>();
	return make_unique<ValidityStatistics>(has_null);
}

void ValidityStatistics::Verify(Vector &vector, idx_t count) {
	if (VectorOperations::HasNull(vector, count)) {
		throw InternalException(
		    "Statistics mismatch: vector labeled as not having NULL values, but vector contains null values: %s",
		    vector.ToString(count));
	}
}

string ValidityStatistics::ToString() {
	return has_null ? "[Has Null: true]" : "[Has Null: false]";
}

} // namespace duckdb
