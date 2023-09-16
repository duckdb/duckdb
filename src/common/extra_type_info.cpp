#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
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

	static shared_ptr<EnumTypeInfoTemplated> Deserialize(Deserializer &deserializer, uint32_t size) {
		Vector values_insert_order(LogicalType::VARCHAR, size);
		auto strings = FlatVector::GetData<string_t>(values_insert_order);

		deserializer.ReadList(201, "values", [&](Deserializer::List &list, idx_t i) {
			strings[i] = StringVector::AddStringOrBlob(values_insert_order, list.ReadElement<string>());
		});
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

shared_ptr<ExtraTypeInfo> EnumTypeInfo::Deserialize(Deserializer &deserializer) {
	auto values_count = deserializer.ReadProperty<idx_t>(200, "values_count");
	auto enum_internal_type = EnumTypeInfo::DictType(values_count);
	switch (enum_internal_type) {
	case PhysicalType::UINT8:
		return EnumTypeInfoTemplated<uint8_t>::Deserialize(deserializer, values_count);
	case PhysicalType::UINT16:
		return EnumTypeInfoTemplated<uint16_t>::Deserialize(deserializer, values_count);
	case PhysicalType::UINT32:
		return EnumTypeInfoTemplated<uint32_t>::Deserialize(deserializer, values_count);
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

void EnumTypeInfo::Serialize(Serializer &serializer) const {
	ExtraTypeInfo::Serialize(serializer);

	// Enums are special in that we serialize their values as a list instead of dumping the whole vector
	auto strings = FlatVector::GetData<string_t>(values_insert_order);
	serializer.WriteProperty(200, "values_count", dict_size);
	serializer.WriteList(201, "values", dict_size,
	                     [&](Serializer::List &list, idx_t i) { list.WriteElement(strings[i]); });
}

} // namespace duckdb
