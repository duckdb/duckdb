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

BaseStatistics::BaseStatistics(LogicalType type, bool global) : type(move(type)), global(global) {
}

BaseStatistics::~BaseStatistics() {
}

void BaseStatistics::InitializeBase() {
	validity_stats = make_unique<ValidityStatistics>(false);
	if (global) {
		distinct_stats = make_unique<DistinctStatistics>();
	}
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

void BaseStatistics::UpdateDistinctStatistics(Vector &v, idx_t count) {
	if (type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::LIST || type.id() == LogicalTypeId::MAP) {
		// We don't do nested types (yet)
		return;
	}
	D_ASSERT(distinct_stats);
	auto &d_stats = (DistinctStatistics &)*distinct_stats;
	d_stats.Update(v, count);
}

void BaseStatistics::UpdateDistinctStatistics(VectorData &vdata, PhysicalType ptype, idx_t count) {
	D_ASSERT(distinct_stats);
	auto &d_stats = (DistinctStatistics &)*distinct_stats;
	d_stats.Update(vdata, ptype, count);
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
	if (global) {
		MergeInternal(distinct_stats, other.distinct_stats);
	}
}

unique_ptr<BaseStatistics> BaseStatistics::CreateEmpty(LogicalType type, bool global) {
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
		return make_unique<NumericStatistics>(move(type), global);
	case PhysicalType::VARCHAR:
		return make_unique<StringStatistics>(move(type), global);
	case PhysicalType::STRUCT:
		return make_unique<StructStatistics>(move(type), global);
	case PhysicalType::LIST:
		return make_unique<ListStatistics>(move(type), global);
	case PhysicalType::INTERVAL:
	default:
		auto result = make_unique<BaseStatistics>(move(type), global);
		result->InitializeBase();
		return result;
	}
}

unique_ptr<BaseStatistics> BaseStatistics::Copy() const {
	auto result = make_unique<BaseStatistics>(type, global);
	result->CopyBase(*this);
	return result;
}

void BaseStatistics::CopyBase(const BaseStatistics &orig) {
	if (orig.validity_stats) {
		validity_stats = orig.validity_stats->Copy();
	}
	if (orig.distinct_stats) {
		D_ASSERT(global && orig.global);
		distinct_stats = orig.distinct_stats->Copy();
	}
}

void BaseStatistics::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	Serialize(writer);
	writer.Finalize();
}

void BaseStatistics::Serialize(FieldWriter &writer) const {
	// Required
	writer.WriteField<bool>(global);
	if (!validity_stats) {
		auto validity_stats_temp = make_unique<ValidityStatistics>(CanHaveNull(), CanHaveNoNull());
		writer.WriteSerializable<ValidityStatistics>((ValidityStatistics &)*validity_stats_temp);
	} else {
		writer.WriteSerializable<ValidityStatistics>((ValidityStatistics &)*validity_stats);
	}
	// Optional
	D_ASSERT((global && distinct_stats) || (!global && !distinct_stats));
	writer.WriteOptional<BaseStatistics>(distinct_stats);
}

void BaseStatistics::DeserializeBase(FieldReader &reader) {
	// Required
	global = reader.ReadRequired<bool>();
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
		result = make_unique<BaseStatistics>(move(type), false);
		result->DeserializeBase(reader);
		break;
	default:
		throw InternalException("Unimplemented type for statistics deserialization");
	}
	return result;
}

string BaseStatistics::ToString() const {
	return StringUtil::Format("%s%s", validity_stats ? validity_stats->ToString() : "",
	                          distinct_stats ? distinct_stats->ToString() : "");
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
