#include "duckdb/storage/statistics/list_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/storage/statistics/struct_statistics.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/storage/statistics/validity_statistics.hpp"
#include "duckdb/common/types/vector.hpp"

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

void BaseStatistics::Merge(const BaseStatistics &other) {
	D_ASSERT(type == other.type);
	if (other.validity_stats) {
		if (validity_stats) {
			validity_stats->Merge(*other.validity_stats);
		} else {
			validity_stats = other.validity_stats->Copy();
		}
	}
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
		return base_stats;
	}
}

unique_ptr<BaseStatistics> BaseStatistics::Copy() const {
	auto statistics = make_unique<BaseStatistics>(type);
	if (validity_stats) {
		statistics->validity_stats = validity_stats->Copy();
	}
	return statistics;
}

void BaseStatistics::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<bool>(CanHaveNull());
	writer.WriteField<bool>(CanHaveNoNull());
	Serialize(writer);
	writer.Finalize();
}

void BaseStatistics::Serialize(FieldWriter &writer) const {
}

unique_ptr<BaseStatistics> BaseStatistics::Deserialize(Deserializer &source, LogicalType type) {
	FieldReader reader(source);
	bool can_have_null = reader.ReadRequired<bool>();
	bool can_have_no_null = reader.ReadRequired<bool>();
	unique_ptr<BaseStatistics> result;
	switch (type.InternalType()) {
	case PhysicalType::BIT:
		return ValidityStatistics::Deserialize(reader);
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
	result->validity_stats = make_unique<ValidityStatistics>(can_have_null, can_have_no_null);
	return result;
}

string BaseStatistics::ToString() const {
	return StringUtil::Format("Base Statistics %s", validity_stats ? validity_stats->ToString() : "[]");
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
