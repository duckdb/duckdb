#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/statistics/distinct_statistics.hpp"
#include "duckdb/storage/statistics/list_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/storage/statistics/struct_statistics.hpp"
#include "duckdb/storage/statistics/validity_statistics.hpp"

namespace duckdb {

BaseStatistics::BaseStatistics(LogicalType type) : type(move(type)) {
}

BaseStatistics::~BaseStatistics() {
}

bool BaseStatistics::CanHaveNull() const {
	if (!validity_stats) {
		// we don't know
		// solid maybe
		return true;
	}
	return ((ValidityStatistics &)*validity_stats).has_null;
}

bool BaseStatistics::CanHaveNoNull() const {
	if (!validity_stats) {
		// we don't know
		// solid maybe
		return true;
	}
	return ((ValidityStatistics &)*validity_stats).has_no_null;
}

void MergeInternal(unique_ptr<BaseStatistics> &orig, const unique_ptr<BaseStatistics> &other) {
	if (other) {
		if (orig) {
			orig->Merge(*other);
		} else {
			orig = other->Copy();
		}
	}
}

void BaseStatistics::Merge(const BaseStatistics &other) {
	D_ASSERT(type == other.type);
	MergeInternal(validity_stats, other.validity_stats);
	MergeInternal(distinct_stats, other.distinct_stats);
}

unique_ptr<BaseStatistics> BaseStatistics::CreateEmpty(LogicalType type) {
	switch (type.InternalType()) {
	case PhysicalType::BIT:
		return make_unique<ValidityStatistics>(false, false);
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return make_unique<NumericStatistics>(move(type));
	case PhysicalType::VARCHAR:
		return make_unique<StringStatistics>(move(type));
	case PhysicalType::STRUCT:
		return make_unique<StructStatistics>(move(type));
	case PhysicalType::LIST:
		return make_unique<ListStatistics>(move(type));
	case PhysicalType::INTERVAL:
	default:
		auto base_stats = make_unique<BaseStatistics>(move(type));
		base_stats->validity_stats = make_unique<ValidityStatistics>(false);
		base_stats->distinct_stats = make_unique<DistinctStatistics>();
		return base_stats;
	}
}

unique_ptr<BaseStatistics> BaseStatistics::Copy() const {
	auto statistics = make_unique<BaseStatistics>(type);
	if (validity_stats) {
		statistics->validity_stats = validity_stats->Copy();
	}
	if (distinct_stats) {
		statistics->distinct_stats = distinct_stats->Copy();
	}
	return statistics;
}

void BaseStatistics::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	Serialize(writer);
	writer.Finalize();
}

void BaseStatistics::Serialize(FieldWriter &writer) const {
	// Required
	if (!validity_stats) {
		auto validity_stats_temp = make_unique<ValidityStatistics>(CanHaveNull(), CanHaveNoNull());
		writer.WriteSerializable<ValidityStatistics>((ValidityStatistics &)*validity_stats_temp);
	} else {
		writer.WriteSerializable<ValidityStatistics>((ValidityStatistics &)*validity_stats);
	}
	// Optional
	writer.WriteOptional<BaseStatistics>(distinct_stats);
}

void BaseStatistics::DeserializeBase(FieldReader &reader) {
	// Required
	validity_stats = reader.ReadRequiredSerializable<ValidityStatistics>();
	// Optional
	distinct_stats = reader.ReadOptional<DistinctStatistics>(nullptr);
}

unique_ptr<BaseStatistics> BaseStatistics::Deserialize(Deserializer &source, LogicalType type) {
	FieldReader reader(source);
	unique_ptr<BaseStatistics> result;
	switch (type.InternalType()) {
	case PhysicalType::BIT:
		result = ValidityStatistics::Deserialize(reader);
		break;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		result = NumericStatistics::Deserialize(reader, move(type));
		break;
	case PhysicalType::VARCHAR:
		result = StringStatistics::Deserialize(reader, move(type));
		break;
	case PhysicalType::STRUCT:
		result = StructStatistics::Deserialize(reader, move(type));
		break;
	case PhysicalType::LIST:
		result = ListStatistics::Deserialize(reader, move(type));
		break;
	case PhysicalType::INTERVAL:
		result = make_unique<BaseStatistics>(move(type));
		break;
	default:
		throw InternalException("Unimplemented type for statistics deserialization");
	}
	result->DeserializeBase(reader);
	return result;
}

string BaseStatistics::ToString() const {
	return StringUtil::Format("Base Statistics %s %s", validity_stats ? validity_stats->ToString() : "[]",
	                          distinct_stats ? distinct_stats->ToString() : "[]");
}

void BaseStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	D_ASSERT(vector.GetType() == this->type);
	if (validity_stats) {
		validity_stats->Verify(vector, sel, count);
	}
}

void BaseStatistics::Verify(Vector &vector, idx_t count) const {
	SelectionVector owned_sel;
	auto sel = FlatVector::IncrementalSelectionVector(count, owned_sel);
	Verify(vector, *sel, count);
}

} // namespace duckdb
