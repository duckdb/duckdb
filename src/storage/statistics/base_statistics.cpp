#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"

namespace duckdb {

BaseStatistics::BaseStatistics() : type(LogicalType::INVALID) {
}

BaseStatistics::BaseStatistics(LogicalType type) {
	Construct(*this, std::move(type));
}

void BaseStatistics::Construct(BaseStatistics &stats, LogicalType type) {
	stats.distinct_count = 0;
	stats.type = std::move(type);
	if (ListStats::IsList(stats)) {
		ListStats::Construct(stats);
	}
	if (StructStats::IsStruct(stats)) {
		StructStats::Construct(stats);
	}
}

BaseStatistics::~BaseStatistics() {
}

BaseStatistics::BaseStatistics(BaseStatistics &&other) noexcept {
	std::swap(type, other.type);
	has_null = other.has_null;
	has_no_null = other.has_no_null;
	distinct_count = other.distinct_count;
	stats_union = other.stats_union;
	std::swap(child_stats, other.child_stats);
}

BaseStatistics &BaseStatistics::operator=(BaseStatistics &&other) noexcept {
	std::swap(type, other.type);
	has_null = other.has_null;
	has_no_null = other.has_no_null;
	distinct_count = other.distinct_count;
	stats_union = other.stats_union;
	std::swap(child_stats, other.child_stats);
	return *this;
}

void BaseStatistics::InitializeUnknown() {
	has_null = true;
	has_no_null = true;
}

void BaseStatistics::InitializeEmpty() {
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
	if (StringStats::IsString(other)) {
		StringStats::Merge(*this, other);
	}
	if (ListStats::IsList(other)) {
		ListStats::Merge(*this, other);
	}
	if (StructStats::IsStruct(other)) {
		StructStats::Merge(*this, other);
	}
}

idx_t BaseStatistics::GetDistinctCount() {
	return distinct_count;
}

BaseStatistics BaseStatistics::CreateUnknownType(LogicalType type) {
	switch (type.InternalType()) {
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
		return NumericStats::CreateUnknown(std::move(type));
	case PhysicalType::VARCHAR:
		return StringStats::CreateUnknown(std::move(type));
	case PhysicalType::STRUCT:
		return StructStats::CreateUnknown(std::move(type));
	case PhysicalType::LIST:
		return ListStats::CreateUnknown(std::move(type));
	case PhysicalType::BIT:
	case PhysicalType::INTERVAL:
	default:
		return BaseStatistics(std::move(type));
	}
}

BaseStatistics BaseStatistics::CreateEmptyType(LogicalType type) {
	switch (type.InternalType()) {
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
		return NumericStats::CreateEmpty(std::move(type));
	case PhysicalType::VARCHAR:
		return StringStats::CreateEmpty(std::move(type));
	case PhysicalType::STRUCT:
		return StructStats::CreateEmpty(std::move(type));
	case PhysicalType::LIST:
		return ListStats::CreateEmpty(std::move(type));
	case PhysicalType::BIT:
	case PhysicalType::INTERVAL:
	default:
		return BaseStatistics(std::move(type));
	}
}

BaseStatistics BaseStatistics::CreateUnknown(LogicalType type) {
	auto result = CreateUnknownType(std::move(type));
	result.InitializeUnknown();
	return result;
}

BaseStatistics BaseStatistics::CreateEmpty(LogicalType type) {
	auto result = CreateEmptyType(std::move(type));
	result.InitializeEmpty();
	return result;
}

void BaseStatistics::Copy(const BaseStatistics &other) {
	D_ASSERT(GetType() == other.GetType());
	CopyBase(other);
	stats_union = other.stats_union;
	if (ListStats::IsList(*this)) {
		ListStats::Copy(*this, other);
	} else if (StructStats::IsStruct(*this)) {
		StructStats::Copy(*this, other);
	}
}

BaseStatistics BaseStatistics::Copy() const {
	BaseStatistics result(type);
	result.Copy(*this);
	return result;
}

unique_ptr<BaseStatistics> BaseStatistics::ToUnique() const {
	auto result = unique_ptr<BaseStatistics>(new BaseStatistics(type));
	result->Copy(*this);
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
	} else if (StringStats::IsString(*this)) {
		StringStats::Serialize(*this, writer);
	} else if (ListStats::IsList(*this)) {
		ListStats::Serialize(*this, writer);
	} else if (StructStats::IsStruct(*this)) {
		StructStats::Serialize(*this, writer);
	}
}
BaseStatistics BaseStatistics::DeserializeType(FieldReader &reader, LogicalType type) {
	switch (type.InternalType()) {
	case PhysicalType::BIT:
		return BaseStatistics(LogicalTypeId::VALIDITY);
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
		return NumericStats::Deserialize(reader, std::move(type));
	case PhysicalType::VARCHAR:
		return StringStats::Deserialize(reader, std::move(type));
	case PhysicalType::STRUCT:
		return StructStats::Deserialize(reader, std::move(type));
	case PhysicalType::LIST:
		return ListStats::Deserialize(reader, std::move(type));
	case PhysicalType::INTERVAL:
		return BaseStatistics(std::move(type));
	default:
		throw InternalException("Unimplemented type for statistics deserialization");
	}
}

BaseStatistics BaseStatistics::Deserialize(Deserializer &source, LogicalType type) {
	FieldReader reader(source);
	bool has_null = reader.ReadRequired<bool>();
	bool has_no_null = reader.ReadRequired<bool>();
	auto result = DeserializeType(reader, type);
	result.has_null = has_null;
	result.has_no_null = has_no_null;
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
	} else if (StringStats::IsString(*this)) {
		result = StringStats::ToString(*this) + result;
	} else if (ListStats::IsList(*this)) {
		result = ListStats::ToString(*this) + result;
	} else if (StructStats::IsStruct(*this)) {
		result = StructStats::ToString(*this) + result;
	}
	return result;
}

void BaseStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	D_ASSERT(vector.GetType() == this->type);
	if (NumericStats::IsNumeric(*this)) {
		NumericStats::Verify(*this, vector, sel, count);
	} else if (StringStats::IsString(*this)) {
		StringStats::Verify(*this, vector, sel, count);
	} else if (ListStats::IsList(*this)) {
		ListStats::Verify(*this, vector, sel, count);
	} else if (StructStats::IsStruct(*this)) {
		StructStats::Verify(*this, vector, sel, count);
	}
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

BaseStatistics BaseStatistics::FromConstantType(const Value &input) {
	switch (input.type().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE: {
		auto result = NumericStats::CreateEmpty(input.type());
		NumericStats::SetMin(result, input);
		NumericStats::SetMax(result, input);
		return result;
	}
	case PhysicalType::VARCHAR: {
		auto result = StringStats::CreateEmpty(input.type());
		if (!input.IsNull()) {
			auto &string_value = StringValue::Get(input);
			StringStats::Update(result, string_t(string_value));
		}
		return result;
	}
	case PhysicalType::STRUCT: {
		auto result = StructStats::CreateEmpty(input.type());
		auto &child_types = StructType::GetChildTypes(input.type());
		if (input.IsNull()) {
			for (idx_t i = 0; i < child_types.size(); i++) {
				StructStats::SetChildStats(result, i, FromConstant(Value(child_types[i].second)));
			}
		} else {
			auto &struct_children = StructValue::GetChildren(input);
			for (idx_t i = 0; i < child_types.size(); i++) {
				StructStats::SetChildStats(result, i, FromConstant(struct_children[i]));
			}
		}
		return result;
	}
	case PhysicalType::LIST: {
		auto result = ListStats::CreateEmpty(input.type());
		auto &child_stats = ListStats::GetChildStats(result);
		if (!input.IsNull()) {
			auto &list_children = ListValue::GetChildren(input);
			for (auto &child_element : list_children) {
				child_stats.Merge(FromConstant(child_element));
			}
		}
		return result;
	}
	default: {
		return BaseStatistics(input.type());
	}
	}
}

BaseStatistics BaseStatistics::FromConstant(const Value &input) {
	auto result = FromConstantType(input);
	result.SetDistinctCount(1);
	if (input.IsNull()) {
		result.Set(StatsInfo::CAN_HAVE_NULL_VALUES);
		result.Set(StatsInfo::CANNOT_HAVE_VALID_VALUES);
	} else {
		result.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
		result.Set(StatsInfo::CAN_HAVE_VALID_VALUES);
	}
	return result;
}

} // namespace duckdb
