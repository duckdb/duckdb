#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/common/string_map_set.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Extra Type Info
//===--------------------------------------------------------------------===//
ExtraTypeInfo::ExtraTypeInfo(ExtraTypeInfoType type) : type(type) {
}
ExtraTypeInfo::ExtraTypeInfo(ExtraTypeInfoType type, string alias) : type(type), alias(std::move(alias)) {
}
ExtraTypeInfo::~ExtraTypeInfo() {
}

bool ExtraTypeInfo::Equals(ExtraTypeInfo *other_p) const {
	if (type == ExtraTypeInfoType::INVALID_TYPE_INFO || type == ExtraTypeInfoType::STRING_TYPE_INFO ||
	    type == ExtraTypeInfoType::GENERIC_TYPE_INFO) {
		if (!other_p) {
			if (!alias.empty()) {
				return false;
			}
			//! We only need to compare aliases when both types have them in this case
			return true;
		}
		if (alias != other_p->alias) {
			return false;
		}
		return true;
	}
	if (!other_p) {
		return false;
	}
	if (type != other_p->type) {
		return false;
	}
	return alias == other_p->alias && EqualsInternal(other_p);
}

void ExtraTypeInfo::Serialize(FieldWriter &writer) const {
}

void ExtraTypeInfo::Serialize(ExtraTypeInfo *info, FieldWriter &writer) {
	if (!info) {
		writer.WriteField<ExtraTypeInfoType>(ExtraTypeInfoType::INVALID_TYPE_INFO);
		writer.WriteString(string());
	} else {
		writer.WriteField<ExtraTypeInfoType>(info->type);
		info->Serialize(writer);
		writer.WriteString(info->alias);
	}
}

shared_ptr<ExtraTypeInfo> ExtraTypeInfo::Deserialize(FieldReader &reader) {
	auto type = reader.ReadRequired<ExtraTypeInfoType>();
	shared_ptr<ExtraTypeInfo> extra_info;
	switch (type) {
	case ExtraTypeInfoType::INVALID_TYPE_INFO: {
		auto alias = reader.ReadField<string>(string());
		if (!alias.empty()) {
			return make_shared<ExtraTypeInfo>(type, alias);
		}
		return nullptr;
	}
	case ExtraTypeInfoType::GENERIC_TYPE_INFO: {
		extra_info = make_shared<ExtraTypeInfo>(type);
	} break;
	case ExtraTypeInfoType::DECIMAL_TYPE_INFO:
		extra_info = DecimalTypeInfo::Deserialize(reader);
		break;
	case ExtraTypeInfoType::STRING_TYPE_INFO:
		extra_info = StringTypeInfo::Deserialize(reader);
		break;
	case ExtraTypeInfoType::LIST_TYPE_INFO:
		extra_info = ListTypeInfo::Deserialize(reader);
		break;
	case ExtraTypeInfoType::STRUCT_TYPE_INFO:
		extra_info = StructTypeInfo::Deserialize(reader);
		break;
	case ExtraTypeInfoType::USER_TYPE_INFO:
		extra_info = UserTypeInfo::Deserialize(reader);
		break;
	case ExtraTypeInfoType::ENUM_TYPE_INFO:
		extra_info = EnumTypeInfo::Deserialize(reader);
		break;
	case ExtraTypeInfoType::AGGREGATE_STATE_TYPE_INFO:
		extra_info = AggregateStateTypeInfo::Deserialize(reader);
		break;
	default:
		throw InternalException("Unimplemented type info in ExtraTypeInfo::Deserialize");
	}
	auto alias = reader.ReadField<string>(string());
	extra_info->alias = alias;
	return extra_info;
}

bool ExtraTypeInfo::EqualsInternal(ExtraTypeInfo *other_p) const {
	// Do nothing
	return true;
}

//===--------------------------------------------------------------------===//
// Decimal Type Info
//===--------------------------------------------------------------------===//
DecimalTypeInfo::DecimalTypeInfo() : ExtraTypeInfo(ExtraTypeInfoType::DECIMAL_TYPE_INFO) {
}

DecimalTypeInfo::DecimalTypeInfo(uint8_t width_p, uint8_t scale_p)
    : ExtraTypeInfo(ExtraTypeInfoType::DECIMAL_TYPE_INFO), width(width_p), scale(scale_p) {
	D_ASSERT(width_p >= scale_p);
}

void DecimalTypeInfo::Serialize(FieldWriter &writer) const {
	writer.WriteField<uint8_t>(width);
	writer.WriteField<uint8_t>(scale);
}

shared_ptr<ExtraTypeInfo> DecimalTypeInfo::Deserialize(FieldReader &reader) {
	auto width = reader.ReadRequired<uint8_t>();
	auto scale = reader.ReadRequired<uint8_t>();
	return make_shared<DecimalTypeInfo>(width, scale);
}

bool DecimalTypeInfo::EqualsInternal(ExtraTypeInfo *other_p) const {
	auto &other = other_p->Cast<DecimalTypeInfo>();
	return width == other.width && scale == other.scale;
}

//===--------------------------------------------------------------------===//
// String Type Info
//===--------------------------------------------------------------------===//
StringTypeInfo::StringTypeInfo() : ExtraTypeInfo(ExtraTypeInfoType::STRING_TYPE_INFO) {
}

StringTypeInfo::StringTypeInfo(string collation_p)
    : ExtraTypeInfo(ExtraTypeInfoType::STRING_TYPE_INFO), collation(std::move(collation_p)) {
}

void StringTypeInfo::Serialize(FieldWriter &writer) const {
	writer.WriteString(collation);
}

shared_ptr<ExtraTypeInfo> StringTypeInfo::Deserialize(FieldReader &reader) {
	auto collation = reader.ReadRequired<string>();
	return make_shared<StringTypeInfo>(std::move(collation));
}

bool StringTypeInfo::EqualsInternal(ExtraTypeInfo *other_p) const {
	// collation info has no impact on equality
	return true;
}

//===--------------------------------------------------------------------===//
// List Type Info
//===--------------------------------------------------------------------===//
ListTypeInfo::ListTypeInfo() : ExtraTypeInfo(ExtraTypeInfoType::LIST_TYPE_INFO) {
}

ListTypeInfo::ListTypeInfo(LogicalType child_type_p)
    : ExtraTypeInfo(ExtraTypeInfoType::LIST_TYPE_INFO), child_type(std::move(child_type_p)) {
}

void ListTypeInfo::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(child_type);
}

shared_ptr<ExtraTypeInfo> ListTypeInfo::Deserialize(FieldReader &reader) {
	auto child_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	return make_shared<ListTypeInfo>(std::move(child_type));
}

bool ListTypeInfo::EqualsInternal(ExtraTypeInfo *other_p) const {
	auto &other = other_p->Cast<ListTypeInfo>();
	return child_type == other.child_type;
}

//===--------------------------------------------------------------------===//
// Struct Type Info
//===--------------------------------------------------------------------===//
StructTypeInfo::StructTypeInfo() : ExtraTypeInfo(ExtraTypeInfoType::STRUCT_TYPE_INFO) {
}

StructTypeInfo::StructTypeInfo(child_list_t<LogicalType> child_types_p)
    : ExtraTypeInfo(ExtraTypeInfoType::STRUCT_TYPE_INFO), child_types(std::move(child_types_p)) {
}

void StructTypeInfo::Serialize(FieldWriter &writer) const {
	writer.WriteField<uint32_t>(child_types.size());
	auto &serializer = writer.GetSerializer();
	for (idx_t i = 0; i < child_types.size(); i++) {
		serializer.WriteString(child_types[i].first);
		child_types[i].second.Serialize(serializer);
	}
}

shared_ptr<ExtraTypeInfo> StructTypeInfo::Deserialize(FieldReader &reader) {
	child_list_t<LogicalType> child_list;
	auto child_types_size = reader.ReadRequired<uint32_t>();
	auto &source = reader.GetSource();
	for (uint32_t i = 0; i < child_types_size; i++) {
		auto name = source.Read<string>();
		auto type = LogicalType::Deserialize(source);
		child_list.emplace_back(std::move(name), std::move(type));
	}
	return make_shared<StructTypeInfo>(std::move(child_list));
}

bool StructTypeInfo::EqualsInternal(ExtraTypeInfo *other_p) const {
	auto &other = other_p->Cast<StructTypeInfo>();
	return child_types == other.child_types;
}

//===--------------------------------------------------------------------===//
// Aggregate State Type Info
//===--------------------------------------------------------------------===//
AggregateStateTypeInfo::AggregateStateTypeInfo() : ExtraTypeInfo(ExtraTypeInfoType::AGGREGATE_STATE_TYPE_INFO) {
}

AggregateStateTypeInfo::AggregateStateTypeInfo(aggregate_state_t state_type_p)
    : ExtraTypeInfo(ExtraTypeInfoType::AGGREGATE_STATE_TYPE_INFO), state_type(std::move(state_type_p)) {
}

void AggregateStateTypeInfo::Serialize(FieldWriter &writer) const {
	auto &serializer = writer.GetSerializer();
	writer.WriteString(state_type.function_name);
	state_type.return_type.Serialize(serializer);
	writer.WriteField<uint32_t>(state_type.bound_argument_types.size());
	for (idx_t i = 0; i < state_type.bound_argument_types.size(); i++) {
		state_type.bound_argument_types[i].Serialize(serializer);
	}
}

shared_ptr<ExtraTypeInfo> AggregateStateTypeInfo::Deserialize(FieldReader &reader) {
	auto &source = reader.GetSource();

	auto function_name = reader.ReadRequired<string>();
	auto return_type = LogicalType::Deserialize(source);
	auto bound_argument_types_size = reader.ReadRequired<uint32_t>();
	vector<LogicalType> bound_argument_types;

	for (uint32_t i = 0; i < bound_argument_types_size; i++) {
		auto type = LogicalType::Deserialize(source);
		bound_argument_types.push_back(std::move(type));
	}
	return make_shared<AggregateStateTypeInfo>(
	    aggregate_state_t(std::move(function_name), std::move(return_type), std::move(bound_argument_types)));
}

bool AggregateStateTypeInfo::EqualsInternal(ExtraTypeInfo *other_p) const {
	auto &other = other_p->Cast<AggregateStateTypeInfo>();
	return state_type.function_name == other.state_type.function_name &&
	       state_type.return_type == other.state_type.return_type &&
	       state_type.bound_argument_types == other.state_type.bound_argument_types;
}

//===--------------------------------------------------------------------===//
// User Type Info
//===--------------------------------------------------------------------===//
UserTypeInfo::UserTypeInfo() : ExtraTypeInfo(ExtraTypeInfoType::USER_TYPE_INFO) {
}

UserTypeInfo::UserTypeInfo(string name_p)
    : ExtraTypeInfo(ExtraTypeInfoType::USER_TYPE_INFO), user_type_name(std::move(name_p)) {
}

void UserTypeInfo::Serialize(FieldWriter &writer) const {
	writer.WriteString(user_type_name);
}

shared_ptr<ExtraTypeInfo> UserTypeInfo::Deserialize(FieldReader &reader) {
	auto enum_name = reader.ReadRequired<string>();
	return make_shared<UserTypeInfo>(std::move(enum_name));
}

bool UserTypeInfo::EqualsInternal(ExtraTypeInfo *other_p) const {
	auto &other = other_p->Cast<UserTypeInfo>();
	return other.user_type_name == user_type_name;
}

//===--------------------------------------------------------------------===//
// Enum Type Info
//===--------------------------------------------------------------------===//
PhysicalType EnumTypeInfo::DictType(idx_t size) {
	if (size <= NumericLimits<uint8_t>::Maximum()) {
		return PhysicalType::UINT8;
	} else if (size <= NumericLimits<uint16_t>::Maximum()) {
		return PhysicalType::UINT16;
	} else if (size <= NumericLimits<uint32_t>::Maximum()) {
		return PhysicalType::UINT32;
	} else {
		throw InternalException("Enum size must be lower than " + std::to_string(NumericLimits<uint32_t>::Maximum()));
	}
}

template <class T>
struct EnumTypeInfoTemplated : public EnumTypeInfo {
	explicit EnumTypeInfoTemplated(Vector &values_insert_order_p, idx_t size_p)
	    : EnumTypeInfo(values_insert_order_p, size_p) {
		D_ASSERT(values_insert_order_p.GetType().InternalType() == PhysicalType::VARCHAR);

		UnifiedVectorFormat vdata;
		values_insert_order.ToUnifiedFormat(size_p, vdata);

		auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
		for (idx_t i = 0; i < size_p; i++) {
			auto idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(idx)) {
				throw InternalException("Attempted to create ENUM type with NULL value");
			}
			if (values.count(data[idx]) > 0) {
				throw InvalidInputException("Attempted to create ENUM type with duplicate value %s",
				                            data[idx].GetString());
			}
			values[data[idx]] = i;
		}
	}

	static shared_ptr<EnumTypeInfoTemplated> Deserialize(FieldReader &reader, uint32_t size) {
		Vector values_insert_order(LogicalType::VARCHAR, size);
		values_insert_order.Deserialize(size, reader.GetSource());
		return make_shared<EnumTypeInfoTemplated>(values_insert_order, size);
	}

	static shared_ptr<EnumTypeInfoTemplated> FormatDeserialize(FormatDeserializer &source, uint32_t size) {
		Vector values_insert_order(LogicalType::VARCHAR, size);
		values_insert_order.FormatDeserialize(source, size);
		return make_shared<EnumTypeInfoTemplated>(values_insert_order, size);
	}

	const string_map_t<T> &GetValues() const {
		return values;
	}

	EnumTypeInfoTemplated(const EnumTypeInfoTemplated &) = delete;
	EnumTypeInfoTemplated &operator=(const EnumTypeInfoTemplated &) = delete;

private:
	string_map_t<T> values;
};

EnumTypeInfo::EnumTypeInfo(Vector &values_insert_order_p, idx_t dict_size_p)
    : ExtraTypeInfo(ExtraTypeInfoType::ENUM_TYPE_INFO), values_insert_order(values_insert_order_p),
      dict_type(EnumDictType::VECTOR_DICT), dict_size(dict_size_p) {
}

const EnumDictType &EnumTypeInfo::GetEnumDictType() const {
	return dict_type;
}

const Vector &EnumTypeInfo::GetValuesInsertOrder() const {
	return values_insert_order;
}

const idx_t &EnumTypeInfo::GetDictSize() const {
	return dict_size;
}

LogicalType EnumTypeInfo::CreateType(Vector &ordered_data, idx_t size) {
	// Generate EnumTypeInfo
	shared_ptr<ExtraTypeInfo> info;
	auto enum_internal_type = EnumTypeInfo::DictType(size);
	switch (enum_internal_type) {
	case PhysicalType::UINT8:
		info = make_shared<EnumTypeInfoTemplated<uint8_t>>(ordered_data, size);
		break;
	case PhysicalType::UINT16:
		info = make_shared<EnumTypeInfoTemplated<uint16_t>>(ordered_data, size);
		break;
	case PhysicalType::UINT32:
		info = make_shared<EnumTypeInfoTemplated<uint32_t>>(ordered_data, size);
		break;
	default:
		throw InternalException("Invalid Physical Type for ENUMs");
	}
	// Generate Actual Enum Type
	return LogicalType(LogicalTypeId::ENUM, info);
}

template <class T>
int64_t TemplatedGetPos(const string_map_t<T> &map, const string_t &key) {
	auto it = map.find(key);
	if (it == map.end()) {
		return -1;
	}
	return it->second;
}

int64_t EnumType::GetPos(const LogicalType &type, const string_t &key) {
	auto info = type.AuxInfo();
	switch (type.InternalType()) {
	case PhysicalType::UINT8:
		return TemplatedGetPos(info->Cast<EnumTypeInfoTemplated<uint8_t>>().GetValues(), key);
	case PhysicalType::UINT16:
		return TemplatedGetPos(info->Cast<EnumTypeInfoTemplated<uint16_t>>().GetValues(), key);
	case PhysicalType::UINT32:
		return TemplatedGetPos(info->Cast<EnumTypeInfoTemplated<uint32_t>>().GetValues(), key);
	default:
		throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
	}
}

string_t EnumType::GetString(const LogicalType &type, idx_t pos) {
	D_ASSERT(pos < EnumType::GetSize(type));
	return FlatVector::GetData<string_t>(EnumType::GetValuesInsertOrder(type))[pos];
}

shared_ptr<ExtraTypeInfo> EnumTypeInfo::Deserialize(FieldReader &reader) {
	// deserialize the enum data
	auto enum_size = reader.ReadRequired<uint32_t>();
	auto enum_internal_type = EnumTypeInfo::DictType(enum_size);
	switch (enum_internal_type) {
	case PhysicalType::UINT8:
		return EnumTypeInfoTemplated<uint8_t>::Deserialize(reader, enum_size);
	case PhysicalType::UINT16:
		return EnumTypeInfoTemplated<uint16_t>::Deserialize(reader, enum_size);
	case PhysicalType::UINT32:
		return EnumTypeInfoTemplated<uint32_t>::Deserialize(reader, enum_size);
	default:
		throw InternalException("Invalid Physical Type for ENUMs");
	}
}

shared_ptr<ExtraTypeInfo> EnumTypeInfo::FormatDeserialize(FormatDeserializer &deserializer) {
	auto enum_size = deserializer.ReadProperty<idx_t>(200, "enum_size");
	auto enum_internal_type = EnumTypeInfo::DictType(enum_size);
	switch (enum_internal_type) {
	case PhysicalType::UINT8:
		return EnumTypeInfoTemplated<uint8_t>::FormatDeserialize(deserializer, enum_size);
	case PhysicalType::UINT16:
		return EnumTypeInfoTemplated<uint16_t>::FormatDeserialize(deserializer, enum_size);
	case PhysicalType::UINT32:
		return EnumTypeInfoTemplated<uint32_t>::FormatDeserialize(deserializer, enum_size);
	default:
		throw InternalException("Invalid Physical Type for ENUMs");
	}
}

// Equalities are only used in enums with different catalog entries
bool EnumTypeInfo::EqualsInternal(ExtraTypeInfo *other_p) const {
	auto &other = other_p->Cast<EnumTypeInfo>();
	if (dict_type != other.dict_type) {
		return false;
	}
	D_ASSERT(dict_type == EnumDictType::VECTOR_DICT);
	// We must check if both enums have the same size
	if (other.dict_size != dict_size) {
		return false;
	}
	auto other_vector_ptr = FlatVector::GetData<string_t>(other.values_insert_order);
	auto this_vector_ptr = FlatVector::GetData<string_t>(values_insert_order);

	// Now we must check if all strings are the same
	for (idx_t i = 0; i < dict_size; i++) {
		if (!Equals::Operation(other_vector_ptr[i], this_vector_ptr[i])) {
			return false;
		}
	}
	return true;
}

void EnumTypeInfo::Serialize(FieldWriter &writer) const {
	if (dict_type != EnumDictType::VECTOR_DICT) {
		throw InternalException("Cannot serialize non-vector dictionary ENUM types");
	}
	auto dict_size = GetDictSize();
	// Store Dictionary Size
	writer.WriteField<uint32_t>(dict_size);
	// Store Vector Order By Insertion
	((Vector &)GetValuesInsertOrder()).Serialize(dict_size, writer.GetSerializer()); // NOLINT - FIXME
}

void EnumTypeInfo::FormatSerialize(FormatSerializer &serializer) const {
	ExtraTypeInfo::FormatSerialize(serializer);
	serializer.WriteProperty(200, "dict_size", dict_size);
	((Vector &)values_insert_order).FormatSerialize(serializer, dict_size); // NOLINT - FIXME
}

} // namespace duckdb
