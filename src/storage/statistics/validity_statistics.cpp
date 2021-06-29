#include "duckdb/storage/statistics/validity_statistics.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

ValidityStatistics::ValidityStatistics(bool has_null, bool has_no_null)
    : BaseStatistics(LogicalType(LogicalTypeId::VALIDITY)), has_null(has_null), has_no_null(has_no_null) {
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
		return make_unique<ValidityStatistics>(l.has_null || r.has_null, l.has_no_null || r.has_no_null);
	}
}

bool ValidityStatistics::IsConstant() {
	if (!has_null) {
		return true;
	}
	if (!has_no_null) {
		return true;
	}
	return false;
}

void ValidityStatistics::Merge(const BaseStatistics &other_p) {
	auto &other = (ValidityStatistics &)other_p;
	has_null = has_null || other.has_null;
	has_no_null = has_no_null || other.has_no_null;
}

unique_ptr<BaseStatistics> ValidityStatistics::Copy() {
	return make_unique<ValidityStatistics>(has_null, has_no_null);
}

void ValidityStatistics::Serialize(Serializer &serializer) {
	BaseStatistics::Serialize(serializer);
	serializer.Write<bool>(has_null);
	serializer.Write<bool>(has_no_null);
}

unique_ptr<BaseStatistics> ValidityStatistics::Deserialize(Deserializer &source) {
	bool has_null = source.Read<bool>();
	bool has_no_null = source.Read<bool>();
	return make_unique<ValidityStatistics>(has_null, has_no_null);
}

void ValidityStatistics::Verify(Vector &vector, idx_t count) {
	if (!has_no_null) {
		if (VectorOperations::HasNotNull(vector, count)) {
			throw InternalException(
				"Statistics mismatch: vector labeled as having only NULL values, but vector contains valid values: %s",
				vector.ToString(count));
		}
	}
	if (!has_null) {
		if (VectorOperations::HasNull(vector, count)) {
			throw InternalException(
				"Statistics mismatch: vector labeled as not having NULL values, but vector contains null values: %s",
				vector.ToString(count));
		}
	}
}

string ValidityStatistics::ToString() {
	return has_null ? "[Has Null: true]" : "[Has Null: false]";
}

} // namespace duckdb
