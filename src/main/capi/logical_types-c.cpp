#include "duckdb/main/capi/capi_internal.hpp"

static bool AssertLogicalTypeId(duckdb_logical_type type, duckdb::LogicalTypeId type_id) {
	if (!type) {
		return false;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	if (ltype.id() != type_id) {
		return false;
	}
	return true;
}

static bool AssertInternalType(duckdb_logical_type type, duckdb::PhysicalType physical_type) {
	if (!type) {
		return false;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	if (ltype.InternalType() != physical_type) {
		return false;
	}
	return true;
}

duckdb_logical_type duckdb_create_logical_type(duckdb_type type) {
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(duckdb::ConvertCTypeToCPP(type)));
}

duckdb_logical_type duckdb_create_list_type(duckdb_logical_type type) {
	if (!type) {
		return nullptr;
	}
	duckdb::LogicalType *ltype = new duckdb::LogicalType;
	*ltype = duckdb::LogicalType::LIST(*reinterpret_cast<duckdb::LogicalType *>(type));
	return reinterpret_cast<duckdb_logical_type>(ltype);
}

duckdb_logical_type duckdb_create_array_type(duckdb_logical_type type, idx_t array_size) {
	if (!type) {
		return nullptr;
	}
	if (array_size >= duckdb::ArrayType::MAX_ARRAY_SIZE) {
		return nullptr;
	}
	duckdb::LogicalType *ltype = new duckdb::LogicalType;
	*ltype = duckdb::LogicalType::ARRAY(*reinterpret_cast<duckdb::LogicalType *>(type), array_size);
	return reinterpret_cast<duckdb_logical_type>(ltype);
}

duckdb_logical_type duckdb_create_union_type(duckdb_logical_type *member_types_p, const char **member_names,
                                             idx_t member_count) {
	if (!member_types_p || !member_names) {
		return nullptr;
	}
	duckdb::LogicalType **member_types = reinterpret_cast<duckdb::LogicalType **>(member_types_p);
	duckdb::LogicalType *mtype = new duckdb::LogicalType;
	duckdb::child_list_t<duckdb::LogicalType> members;

	for (idx_t i = 0; i < member_count; i++) {
		members.push_back(make_pair(member_names[i], *member_types[i]));
	}
	*mtype = duckdb::LogicalType::UNION(members);
	return reinterpret_cast<duckdb_logical_type>(mtype);
}

duckdb_logical_type duckdb_create_struct_type(duckdb_logical_type *member_types_p, const char **member_names,
                                              idx_t member_count) {
	if (!member_types_p || !member_names) {
		return nullptr;
	}
	duckdb::LogicalType **member_types = (duckdb::LogicalType **)member_types_p;
	for (idx_t i = 0; i < member_count; i++) {
		if (!member_names[i] || !member_types[i]) {
			return nullptr;
		}
	}

	duckdb::LogicalType *mtype = new duckdb::LogicalType;
	duckdb::child_list_t<duckdb::LogicalType> members;

	for (idx_t i = 0; i < member_count; i++) {
		members.push_back(make_pair(member_names[i], *member_types[i]));
	}
	*mtype = duckdb::LogicalType::STRUCT(members);
	return reinterpret_cast<duckdb_logical_type>(mtype);
}

duckdb_logical_type duckdb_create_enum_type(const char **member_names, idx_t member_count) {
	if (!member_names) {
		return nullptr;
	}
	duckdb::Vector enum_vector(duckdb::LogicalType::VARCHAR, member_count);
	auto enum_vector_ptr = duckdb::FlatVector::GetData<duckdb::string_t>(enum_vector);

	for (idx_t i = 0; i < member_count; i++) {
		if (!member_names[i]) {
			return nullptr;
		}
		enum_vector_ptr[i] = duckdb::StringVector::AddStringOrBlob(enum_vector, member_names[i]);
	}

	duckdb::LogicalType *mtype = new duckdb::LogicalType;
	*mtype = duckdb::LogicalType::ENUM(enum_vector, member_count);
	return reinterpret_cast<duckdb_logical_type>(mtype);
}

duckdb_logical_type duckdb_create_map_type(duckdb_logical_type key_type, duckdb_logical_type value_type) {
	if (!key_type || !value_type) {
		return nullptr;
	}
	duckdb::LogicalType *mtype = new duckdb::LogicalType;
	*mtype = duckdb::LogicalType::MAP(*reinterpret_cast<duckdb::LogicalType *>(key_type),
	                                  *reinterpret_cast<duckdb::LogicalType *>(value_type));
	return reinterpret_cast<duckdb_logical_type>(mtype);
}

duckdb_logical_type duckdb_create_decimal_type(uint8_t width, uint8_t scale) {
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(duckdb::LogicalType::DECIMAL(width, scale)));
}

duckdb_type duckdb_get_type_id(duckdb_logical_type type) {
	if (!type) {
		return DUCKDB_TYPE_INVALID;
	}
	auto ltype = reinterpret_cast<duckdb::LogicalType *>(type);
	return duckdb::ConvertCPPTypeToC(*ltype);
}

void duckdb_destroy_logical_type(duckdb_logical_type *type) {
	if (type && *type) {
		auto ltype = reinterpret_cast<duckdb::LogicalType *>(*type);
		delete ltype;
		*type = nullptr;
	}
}

uint8_t duckdb_decimal_width(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::DECIMAL)) {
		return 0;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	return duckdb::DecimalType::GetWidth(ltype);
}

uint8_t duckdb_decimal_scale(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::DECIMAL)) {
		return 0;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	return duckdb::DecimalType::GetScale(ltype);
}

duckdb_type duckdb_decimal_internal_type(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::DECIMAL)) {
		return DUCKDB_TYPE_INVALID;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	switch (ltype.InternalType()) {
	case duckdb::PhysicalType::INT16:
		return DUCKDB_TYPE_SMALLINT;
	case duckdb::PhysicalType::INT32:
		return DUCKDB_TYPE_INTEGER;
	case duckdb::PhysicalType::INT64:
		return DUCKDB_TYPE_BIGINT;
	case duckdb::PhysicalType::INT128:
		return DUCKDB_TYPE_HUGEINT;
	default:
		return DUCKDB_TYPE_INVALID;
	}
}

duckdb_type duckdb_enum_internal_type(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::ENUM)) {
		return DUCKDB_TYPE_INVALID;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	switch (ltype.InternalType()) {
	case duckdb::PhysicalType::UINT8:
		return DUCKDB_TYPE_UTINYINT;
	case duckdb::PhysicalType::UINT16:
		return DUCKDB_TYPE_USMALLINT;
	case duckdb::PhysicalType::UINT32:
		return DUCKDB_TYPE_UINTEGER;
	default:
		return DUCKDB_TYPE_INVALID;
	}
}

uint32_t duckdb_enum_dictionary_size(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::ENUM)) {
		return 0;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	return duckdb::NumericCast<uint32_t>(duckdb::EnumType::GetSize(ltype));
}

char *duckdb_enum_dictionary_value(duckdb_logical_type type, idx_t index) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::ENUM)) {
		return nullptr;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	auto &vector = duckdb::EnumType::GetValuesInsertOrder(ltype);
	auto value = vector.GetValue(index);
	return strdup(duckdb::StringValue::Get(value).c_str());
}

duckdb_logical_type duckdb_list_type_child_type(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::LIST) &&
	    !AssertLogicalTypeId(type, duckdb::LogicalTypeId::MAP)) {
		return nullptr;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	if (ltype.id() != duckdb::LogicalTypeId::LIST && ltype.id() != duckdb::LogicalTypeId::MAP) {
		return nullptr;
	}
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(duckdb::ListType::GetChildType(ltype)));
}

duckdb_logical_type duckdb_array_type_child_type(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::ARRAY)) {
		return nullptr;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	if (ltype.id() != duckdb::LogicalTypeId::ARRAY) {
		return nullptr;
	}
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(duckdb::ArrayType::GetChildType(ltype)));
}

idx_t duckdb_array_type_array_size(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::ARRAY)) {
		return 0;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	if (ltype.id() != duckdb::LogicalTypeId::ARRAY) {
		return 0;
	}
	return duckdb::ArrayType::GetSize(ltype);
}

duckdb_logical_type duckdb_map_type_key_type(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::MAP)) {
		return nullptr;
	}
	auto &mtype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	if (mtype.id() != duckdb::LogicalTypeId::MAP) {
		return nullptr;
	}
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(duckdb::MapType::KeyType(mtype)));
}

duckdb_logical_type duckdb_map_type_value_type(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::MAP)) {
		return nullptr;
	}
	auto &mtype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	if (mtype.id() != duckdb::LogicalTypeId::MAP) {
		return nullptr;
	}
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(duckdb::MapType::ValueType(mtype)));
}

idx_t duckdb_struct_type_child_count(duckdb_logical_type type) {
	if (!AssertInternalType(type, duckdb::PhysicalType::STRUCT)) {
		return 0;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	return duckdb::StructType::GetChildCount(ltype);
}

idx_t duckdb_union_type_member_count(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::UNION)) {
		return 0;
	}
	idx_t member_count = duckdb_struct_type_child_count(type);
	if (member_count != 0) {
		member_count--;
	}
	return member_count;
}

char *duckdb_union_type_member_name(duckdb_logical_type type, idx_t index) {
	if (!AssertInternalType(type, duckdb::PhysicalType::STRUCT)) {
		return nullptr;
	}
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::UNION)) {
		return nullptr;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	return strdup(duckdb::UnionType::GetMemberName(ltype, index).c_str());
}

duckdb_logical_type duckdb_union_type_member_type(duckdb_logical_type type, idx_t index) {
	if (!AssertInternalType(type, duckdb::PhysicalType::STRUCT)) {
		return nullptr;
	}
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::UNION)) {
		return nullptr;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	return reinterpret_cast<duckdb_logical_type>(
	    new duckdb::LogicalType(duckdb::UnionType::GetMemberType(ltype, index)));
}

char *duckdb_struct_type_child_name(duckdb_logical_type type, idx_t index) {
	if (!AssertInternalType(type, duckdb::PhysicalType::STRUCT)) {
		return nullptr;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	return strdup(duckdb::StructType::GetChildName(ltype, index).c_str());
}

char *duckdb_logical_type_get_alias(duckdb_logical_type type) {
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	return ltype.HasAlias() ? strdup(ltype.GetAlias().c_str()) : nullptr;
}

duckdb_logical_type duckdb_struct_type_child_type(duckdb_logical_type type, idx_t index) {
	if (!AssertInternalType(type, duckdb::PhysicalType::STRUCT)) {
		return nullptr;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	if (ltype.InternalType() != duckdb::PhysicalType::STRUCT) {
		return nullptr;
	}
	return reinterpret_cast<duckdb_logical_type>(
	    new duckdb::LogicalType(duckdb::StructType::GetChildType(ltype, index)));
}
