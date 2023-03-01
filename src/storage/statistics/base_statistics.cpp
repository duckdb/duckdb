#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/statistics/list_statistics.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/storage/statistics/struct_statistics.hpp"

namespace duckdb {

BaseStatistics::BaseStatistics(LogicalType type) : type(std::move(type)), distinct_count(0) {
}

BaseStatistics::~BaseStatistics() {
}

void BaseStatistics::InitializeBase() {
	has_null = false;
	has_no_null = true;
}

bool BaseStatistics::CanHaveNull() const {
	return has_null;
}

bool BaseStatistics::CanHaveNoNull() const {
	return has_no_null;
}

bool BaseStatistics::IsConstant() const {
	if (type.id() == LogicalTypeId::VALIDITY) {
		// validity mask
		if (CanHaveNull() && !CanHaveNoNull()) {
			return true;
		}
		if (!CanHaveNull() && CanHaveNoNull()) {
			return true;
		}
		return false;
	}
	if (NumericStats::IsNumeric(*this)) {
		return NumericStats::IsConstant(*this);
	}
	return false;
}

void BaseStatistics::Merge(const BaseStatistics &other) {
	has_null = has_null || other.has_null;
	has_no_null = has_no_null || other.has_no_null;
	if (NumericStats::IsNumeric(other)) {
		NumericStats::Merge(*this, other);
	}
}

idx_t BaseStatistics::GetDistinctCount() {
	return distinct_count;
}

unique_ptr<BaseStatistics> BaseStatistics::CreateEmpty(LogicalType type) {
	unique_ptr<BaseStatistics> result;
	switch (type.InternalType()) {
	case PhysicalType::BIT:
		result = make_unique<BaseStatistics>(LogicalTypeId::VALIDITY);
		result->Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
		result->Set(StatsInfo::CANNOT_HAVE_VALID_VALUES);
		return result;
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
		result = NumericStats::CreateEmpty(std::move(type));
		break;
	case PhysicalType::VARCHAR:
		result = make_unique<StringStatistics>(std::move(type));
		break;
	case PhysicalType::STRUCT:
		result = make_unique<StructStatistics>(std::move(type));
		break;
	case PhysicalType::LIST:
		result = make_unique<ListStatistics>(std::move(type));
		break;
	case PhysicalType::INTERVAL:
	default:
		result = make_unique<BaseStatistics>(std::move(type));
	}
	result->InitializeBase();
	return result;
}

unique_ptr<BaseStatistics> BaseStatistics::Copy() const {
	auto result = make_unique<BaseStatistics>(type);
	result->CopyBase(*this);
	result->numeric_data = numeric_data;
	return result;
}

void BaseStatistics::CopyBase(const BaseStatistics &other) {
	has_null = other.has_null;
	has_no_null = other.has_no_null;
	distinct_count = other.distinct_count;
}

void BaseStatistics::Set(StatsInfo info) {
	switch (info) {
	case StatsInfo::CAN_HAVE_NULL_VALUES:
		has_null = true;
		break;
	case StatsInfo::CANNOT_HAVE_NULL_VALUES:
		has_null = false;
		break;
	case StatsInfo::CAN_HAVE_VALID_VALUES:
		has_no_null = true;
		break;
	case StatsInfo::CANNOT_HAVE_VALID_VALUES:
		has_no_null = false;
		break;
	case StatsInfo::CAN_HAVE_NULL_AND_VALID_VALUES:
		has_null = true;
		has_no_null = true;
		break;
	default:
		throw InternalException("Unrecognized StatsInfo for BaseStatistics::Set");
	}
}

void BaseStatistics::CombineValidity(BaseStatistics &left, BaseStatistics &right) {
	has_null = left.has_null || right.has_null;
	has_no_null = left.has_no_null || right.has_no_null;
}

void BaseStatistics::CopyValidity(BaseStatistics &stats) {
	has_null = stats.has_null;
	has_no_null = stats.has_no_null;
}

void BaseStatistics::CopyValidity(BaseStatistics *stats) {
	if (!stats) {
		has_null = true;
		has_no_null = true;
		return;
	}
	CopyValidity(*stats);
}

void BaseStatistics::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<bool>(has_null);
	writer.WriteField<bool>(has_no_null);
	Serialize(writer);
	writer.Finalize();
}

void BaseStatistics::SetDistinctCount(idx_t count) {
	this->distinct_count = count;
}

void BaseStatistics::Serialize(FieldWriter &writer) const {
	if (NumericStats::IsNumeric(*this)) {
		NumericStats::Serialize(*this, writer);
	}
}

unique_ptr<BaseStatistics> BaseStatistics::Deserialize(Deserializer &source, LogicalType type) {
	FieldReader reader(source);
	bool has_null = reader.ReadRequired<bool>();
	bool has_no_null = reader.ReadRequired<bool>();
	unique_ptr<BaseStatistics> result;
	switch (type.InternalType()) {
	case PhysicalType::BIT:
		result = make_unique<BaseStatistics>(LogicalTypeId::VALIDITY);
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
		result = NumericStats::Deserialize(reader, std::move(type));
		break;
	case PhysicalType::VARCHAR:
		result = StringStatistics::Deserialize(reader, std::move(type));
		break;
	case PhysicalType::STRUCT:
		result = StructStatistics::Deserialize(reader, std::move(type));
		break;
	case PhysicalType::LIST:
		result = ListStatistics::Deserialize(reader, std::move(type));
		break;
	case PhysicalType::INTERVAL:
		result = make_unique<BaseStatistics>(std::move(type));
		break;
	default:
		throw InternalException("Unimplemented type for statistics deserialization");
	}
	result->has_null = has_null;
	result->has_no_null = has_no_null;
	reader.Finalize();
	return result;
}

string BaseStatistics::ToString() const {
	auto has_n = has_null ? "true" : "false";
	auto has_n_n = has_no_null ? "true" : "false";
	string result =
	    StringUtil::Format("%s%s", StringUtil::Format("[Has Null: %s, Has No Null: %s]", has_n, has_n_n),
	                       distinct_count > 0 ? StringUtil::Format("[Approx Unique: %lld]", distinct_count) : "");
	if (NumericStats::IsNumeric(*this)) {
		result = NumericStats::ToString(*this) + result;
	}
	return result;
}

void BaseStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	D_ASSERT(vector.GetType() == this->type);
	if (has_null && has_no_null) {
		// nothing to verify
		return;
	}
	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(count, vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		bool row_is_valid = vdata.validity.RowIsValid(index);
		if (row_is_valid && !has_no_null) {
			throw InternalException(
			    "Statistics mismatch: vector labeled as having only NULL values, but vector contains valid values: %s",
			    vector.ToString(count));
		}
		if (!row_is_valid && !has_null) {
			throw InternalException(
			    "Statistics mismatch: vector labeled as not having NULL values, but vector contains null values: %s",
			    vector.ToString(count));
		}
	}
}

void BaseStatistics::Verify(Vector &vector, idx_t count) const {
	auto sel = FlatVector::IncrementalSelectionVector();
	Verify(vector, *sel, count);
}

} // namespace duckdb
