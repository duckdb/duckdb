#include "duckdb/main/capi_internal.hpp"

duckdb_logical_type duckdb_create_logical_type(duckdb_type type) {
	return new duckdb::LogicalType(duckdb::ConvertCTypeToCPP(type));
}

duckdb_logical_type duckdb_create_list_type(duckdb_logical_type type) {
	if (!type) {
		return nullptr;
	}
	duckdb::LogicalType *ltype = new duckdb::LogicalType;
	*ltype = duckdb::LogicalType::LIST(*(duckdb::LogicalType *)type);
	return ltype;
}

duckdb_logical_type duckdb_create_map_type(duckdb_logical_type key_type, duckdb_logical_type value_type) {
	if (!key_type || !value_type) {
		return nullptr;
	}
	duckdb::LogicalType *mtype = new duckdb::LogicalType;
	*mtype = duckdb::LogicalType::MAP(*(duckdb::LogicalType *)key_type, *(duckdb::LogicalType *)value_type);
	return mtype;
}

duckdb_logical_type duckdb_create_decimal_type(uint8_t width, uint8_t scale) {
	return new duckdb::LogicalType(duckdb::LogicalType::DECIMAL(width, scale));
}

duckdb_type duckdb_get_type_id(duckdb_logical_type type) {
	if (!type) {
		return DUCKDB_TYPE_INVALID;
	}
	auto ltype = (duckdb::LogicalType *)type;
	return duckdb::ConvertCPPTypeToC(*ltype);
}

void duckdb_destroy_logical_type(duckdb_logical_type *type) {
	if (type && *type) {
		auto ltype = (duckdb::LogicalType *)*type;
		delete ltype;
		*type = nullptr;
	}
}

uint8_t duckdb_decimal_width(duckdb_logical_type type) {
	if (!type) {
		return 0;
	}
	auto &ltype = *((duckdb::LogicalType *)type);
	if (ltype.id() != duckdb::LogicalTypeId::DECIMAL) {
		return 0;
	}
	return duckdb::DecimalType::GetWidth(ltype);
}

uint8_t duckdb_decimal_scale(duckdb_logical_type type) {
	if (!type) {
		return 0;
	}
	auto &ltype = *((duckdb::LogicalType *)type);
	if (ltype.id() != duckdb::LogicalTypeId::DECIMAL) {
		return 0;
	}
	return duckdb::DecimalType::GetScale(ltype);
}

duckdb_type duckdb_decimal_internal_type(duckdb_logical_type type) {
	if (!type) {
		return DUCKDB_TYPE_INVALID;
	}
	auto &ltype = *((duckdb::LogicalType *)type);
	if (ltype.id() != duckdb::LogicalTypeId::DECIMAL) {
		return DUCKDB_TYPE_INVALID;
	}
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
	if (!type) {
		return DUCKDB_TYPE_INVALID;
	}
	auto &ltype = *((duckdb::LogicalType *)type);
	if (ltype.id() != duckdb::LogicalTypeId::ENUM) {
		return DUCKDB_TYPE_INVALID;
	}
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
	if (!type) {
		return 0;
	}
	auto &ltype = *((duckdb::LogicalType *)type);
	if (ltype.id() != duckdb::LogicalTypeId::ENUM) {
		return 0;
	}
	return duckdb::EnumType::GetSize(ltype);
}

char *duckdb_enum_dictionary_value(duckdb_logical_type type, idx_t index) {
	if (!type) {
		return nullptr;
	}
	auto &ltype = *((duckdb::LogicalType *)type);
	if (ltype.id() != duckdb::LogicalTypeId::ENUM) {
		return nullptr;
	}
	auto &vector = duckdb::EnumType::GetValuesInsertOrder(ltype);
	auto value = vector.GetValue(index);
	return strdup(duckdb::StringValue::Get(value).c_str());
}

duckdb_logical_type duckdb_list_type_child_type(duckdb_logical_type type) {
	if (!type) {
		return nullptr;
	}
	auto &ltype = *((duckdb::LogicalType *)type);
	if (ltype.id() != duckdb::LogicalTypeId::LIST) {
		return nullptr;
	}
	return new duckdb::LogicalType(duckdb::ListType::GetChildType(ltype));
}

duckdb_logical_type duckdb_map_type_key_type(duckdb_logical_type type) {
	if (!type) {
		return nullptr;
	}
	auto &mtype = *((duckdb::LogicalType *)type);
	if (mtype.id() != duckdb::LogicalTypeId::MAP) {
		return nullptr;
	}
	return new duckdb::LogicalType(duckdb::MapType::KeyType(mtype));
}

duckdb_logical_type duckdb_map_type_value_type(duckdb_logical_type type) {
	if (!type) {
		return nullptr;
	}
	auto &mtype = *((duckdb::LogicalType *)type);
	if (mtype.id() != duckdb::LogicalTypeId::MAP) {
		return nullptr;
	}
	return new duckdb::LogicalType(duckdb::MapType::ValueType(mtype));
}

idx_t duckdb_struct_type_child_count(duckdb_logical_type type) {
	if (!type) {
		return 0;
	}
	auto &ltype = *((duckdb::LogicalType *)type);
	if (ltype.InternalType() != duckdb::PhysicalType::STRUCT) {
		return 0;
	}
	return duckdb::StructType::GetChildCount(ltype);
}

char *duckdb_struct_type_child_name(duckdb_logical_type type, idx_t index) {
	if (!type) {
		return nullptr;
	}
	auto &ltype = *((duckdb::LogicalType *)type);
	if (ltype.InternalType() != duckdb::PhysicalType::STRUCT) {
		return nullptr;
	}
	return strdup(duckdb::StructType::GetChildName(ltype, index).c_str());
}

duckdb_logical_type duckdb_struct_type_child_type(duckdb_logical_type type, idx_t index) {
	if (!type) {
		return nullptr;
	}
	auto &ltype = *((duckdb::LogicalType *)type);
	if (ltype.InternalType() != duckdb::PhysicalType::STRUCT) {
		return nullptr;
	}
	return new duckdb::LogicalType(duckdb::StructType::GetChildType(ltype, index));
}
