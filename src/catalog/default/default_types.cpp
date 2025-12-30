#include "duckdb/catalog/default/default_types.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/catalog/default/builtin_types/types.hpp"

#include <duckdb/common/types/decimal.hpp>

namespace duckdb {

namespace {

//----------------------------------------------------------------------------------------------------------------------
// DECIMAL Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindDecimalType(const BindLogicalTypeInput &input) {
	auto &modifiers = input.modifiers;

	if (modifiers.size() != 2) {
		throw BinderException("DECIMAL type requires two type modifiers: width and scale");
	}

	auto width_value = modifiers[0].GetValue();
	auto scale_value = modifiers[1].GetValue();

	uint8_t width;
	uint8_t scale;

	if (width_value.TryCastAs(input.context, LogicalTypeId::UTINYINT)) {
		width = width_value.GetValueUnsafe<uint8_t>();
	} else {
		throw BinderException("DECIMAL type width must be a UTINYINT");
	}

	if (scale_value.TryCastAs(input.context, LogicalTypeId::UTINYINT)) {
		scale = scale_value.GetValueUnsafe<uint8_t>();
	} else {
		throw BinderException("DECIMAL type scale must be a UTINYINT");
	}

	if (width < 1 || width > Decimal::MAX_WIDTH_DECIMAL) {
		throw BinderException("DECIMAL type width must be between 1 and %d", Decimal::MAX_WIDTH_DECIMAL);
	}

	if (scale > width) {
		throw BinderException("DECIMAL type scale cannot be greater than width");
	}
	return LogicalType::DECIMAL(width, scale);
}

//----------------------------------------------------------------------------------------------------------------------
// TIMESTAMP Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindTimestampType(const BindLogicalTypeInput &input) {
	auto &modifiers = input.modifiers;

	if (modifiers.empty()) {
		return LogicalType::TIMESTAMP;
	}

	if (modifiers.size() > 1) {
		throw BinderException("TIMESTAMP type takes at most one type modifier");
	}

	auto precision_value = modifiers[0].GetValue();
	uint8_t precision;
	if (precision_value.TryCastAs(input.context, LogicalTypeId::UTINYINT)) {
		precision = precision_value.GetValueUnsafe<uint8_t>();
	} else {
		throw BinderException("TIMESTAMP type precision must be a UTINYINT");
	}

	if (precision > 9) {
		throw BinderException("TIMESTAMP only supports until nano-second precision (9)");
	}
	if (precision == 0) {
		return LogicalType::TIMESTAMP_S;
	}
	if (precision <= 3) {
		return LogicalType::TIMESTAMP_MS;
	}
	if (precision <= 6) {
		return LogicalType::TIMESTAMP;
	}
	return LogicalType::TIMESTAMP_NS;
}

//----------------------------------------------------------------------------------------------------------------------
// VARCHAR Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindVarcharType(const BindLogicalTypeInput &input) {
	// Varchar type can have a single modifier indicating the length, but we ignore it for now
	auto &modifiers = input.modifiers;
	if (modifiers.size() > 1) {
		throw BinderException("VARCHAR type takes at most one type modifier");
	}
	return LogicalType::VARCHAR;
}

//----------------------------------------------------------------------------------------------------------------------
// INTERVAL Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindIntervalType(const BindLogicalTypeInput &input) {
	// Interval type can have a single modifier indicating the leading field, but we ignore it for now
	auto &modifiers = input.modifiers;
	if (modifiers.size() > 1) {
		throw BinderException("INTERVAL type takes at most one type modifier");
	}
	return LogicalType::INTERVAL;
}

//----------------------------------------------------------------------------------------------------------------------
// ENUM Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindEnumType(const BindLogicalTypeInput &input) {
	auto &modifiers = input.modifiers;

	Vector enum_vector(LogicalType::VARCHAR, NumericCast<idx_t>(modifiers.size()));
	auto string_data = FlatVector::GetData<string_t>(enum_vector);
	idx_t pos = 0;

	for (auto &arg : modifiers) {
		auto &modifier = arg.GetValue();

		if (modifier.type() == LogicalTypeId::VARCHAR) {
			string_data[pos++] = StringVector::AddString(enum_vector, modifier.GetValue<string>());
		} else {
			Value str_value;
			if (modifier.TryCastAs(input.context, LogicalTypeId::VARCHAR, str_value, nullptr)) {
				string_data[pos++] = StringVector::AddString(enum_vector, str_value.GetValue<string>());
			} else {
				throw BinderException("ENUM type requires string type modifiers");
			}
		}
	}

	return LogicalType::ENUM(enum_vector, NumericCast<idx_t>(modifiers.size()));
}

//----------------------------------------------------------------------------------------------------------------------
// LIST Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindListType(const BindLogicalTypeInput &input) {
	auto &arguments = input.modifiers;
	if (arguments.size() != 1) {
		throw BinderException("LIST type requires exactly one type modifier");
	}
	auto &child_val = arguments[0].GetValue();
	if (child_val.type() != LogicalTypeId::TYPE) {
		throw BinderException("LIST type modifier must be a type, but got %s", child_val.ToString());
	}

	auto child_type = TypeValue::GetType(arguments[0].GetValue());
	return LogicalType::LIST(std::move(child_type));
}

//----------------------------------------------------------------------------------------------------------------------
// STRUCT Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindStructType(const BindLogicalTypeInput &input) {
	auto &arguments = input.modifiers;

	if (arguments.empty()) {
		throw BinderException("STRUCT type requires at least one child type");
	}

	auto all_name = true;
	auto all_anon = true;
	for (auto &arg : arguments) {
		if (arg.HasName()) {
			all_anon = false;
		} else {
			all_name = false;
		}

		// Also check if all arguments are types
		if (arg.GetValue().type() != LogicalTypeId::TYPE) {
			throw BinderException("STRUCT type arguments must be types");
		}
	}

	if (!all_name && !all_anon) {
		throw BinderException("STRUCT type arguments must either all have names or all be anonymous");
	}

	if (all_anon) {
		// Unnamed struct case
		child_list_t<LogicalType> children;
		for (auto &arg : arguments) {
			children.emplace_back("", TypeValue::GetType(arg.GetValue()));
		}
		return LogicalType::STRUCT(std::move(children));
	}

	// Named struct case
	D_ASSERT(all_name);
	child_list_t<LogicalType> children;
	case_insensitive_set_t name_collision_set;

	for (auto &arg : arguments) {
		auto &child_name = arg.GetName();
		if (name_collision_set.find(child_name) != name_collision_set.end()) {
			throw BinderException("Duplicate STRUCT type argument name \"%s\"", child_name);
		}
		name_collision_set.insert(child_name);
		children.emplace_back(child_name, TypeValue::GetType(arg.GetValue()));
	}

	return LogicalType::STRUCT(std::move(children));
}

//----------------------------------------------------------------------------------------------------------------------
// MAP Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindMapType(const BindLogicalTypeInput &input) {
	auto &arguments = input.modifiers;

	if (arguments.size() != 2) {
		throw BinderException("MAP type requires exactly two type modifiers: key type and value type");
	}

	auto &key_val = arguments[0].GetValue();
	auto &val_val = arguments[1].GetValue();

	if (key_val.type() != LogicalTypeId::TYPE || val_val.type() != LogicalTypeId::TYPE) {
		throw BinderException("MAP type modifiers must be types");
	}

	auto key_type = TypeValue::GetType(arguments[0].GetValue());
	auto val_type = TypeValue::GetType(arguments[1].GetValue());

	return LogicalType::MAP(std::move(key_type), std::move(val_type));
}

//----------------------------------------------------------------------------------------------------------------------
// UNION Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindUnionType(const BindLogicalTypeInput &input) {
	auto &arguments = input.modifiers;

	if (arguments.empty()) {
		throw BinderException("UNION type requires at least one type modifier");
	}

	if (arguments.size() > UnionType::MAX_UNION_MEMBERS) {
		throw BinderException("UNION type supports at most %d type modifiers", UnionType::MAX_UNION_MEMBERS);
	}

	child_list_t<LogicalType> children;
	case_insensitive_set_t name_collision_set;

	for (auto &arg : arguments) {
		if (!arg.HasName()) {
			throw BinderException("UNION type modifiers must have names");
		}
		if (arg.GetValue().type() != LogicalTypeId::TYPE) {
			throw BinderException("UNION type modifiers must be types");
		}

		auto &entry_name = arg.GetName();
		auto entry_type = TypeValue::GetType(arg.GetValue());

		if (name_collision_set.find(entry_name) != name_collision_set.end()) {
			throw BinderException("Duplicate UNION type member name \"%s\"", entry_name);
		}

		name_collision_set.insert(entry_name);
		children.emplace_back(entry_name, entry_type);
	}

	return LogicalType::UNION(std::move(children));
}

//----------------------------------------------------------------------------------------------------------------------
// All Types
//----------------------------------------------------------------------------------------------------------------------

struct DefaultType {
	const char *name;
	LogicalTypeId type;
	bind_logical_type_function_t bind_function;
};

using builtin_type_array = std::array<DefaultType, 78>;

const builtin_type_array BUILTIN_TYPES = {{{"decimal", LogicalTypeId::DECIMAL, BindDecimalType},
                                           {"dec", LogicalTypeId::DECIMAL, BindDecimalType},
                                           {"numeric", LogicalTypeId::DECIMAL, BindDecimalType},
                                           {"time", LogicalTypeId::TIME, nullptr},
                                           {"time_ns", LogicalTypeId::TIME_NS, nullptr},
                                           {"date", LogicalTypeId::DATE, nullptr},
                                           {"timestamp", LogicalTypeId::TIMESTAMP, BindTimestampType},
                                           {"datetime", LogicalTypeId::TIMESTAMP, BindTimestampType},
                                           {"timestamp_us", LogicalTypeId::TIMESTAMP, nullptr},
                                           {"timestamp_ms", LogicalTypeId::TIMESTAMP_MS, nullptr},
                                           {"timestamp_ns", LogicalTypeId::TIMESTAMP_NS, nullptr},
                                           {"timestamp_s", LogicalTypeId::TIMESTAMP_SEC, nullptr},
                                           {"timestamptz", LogicalTypeId::TIMESTAMP_TZ, nullptr},
                                           {"timetz", LogicalTypeId::TIME_TZ, nullptr},
                                           {"interval", LogicalTypeId::INTERVAL, BindIntervalType},
                                           {"varchar", LogicalTypeId::VARCHAR, BindVarcharType},
                                           {"bpchar", LogicalTypeId::VARCHAR, BindVarcharType},
                                           {"string", LogicalTypeId::VARCHAR, BindVarcharType},
                                           {"char", LogicalTypeId::VARCHAR, BindVarcharType},
                                           {"nvarchar", LogicalTypeId::VARCHAR, BindVarcharType},
                                           {"text", LogicalTypeId::VARCHAR, BindVarcharType},
                                           {"blob", LogicalTypeId::BLOB, nullptr},
                                           {"bytea", LogicalTypeId::BLOB, nullptr},
                                           {"varbinary", LogicalTypeId::BLOB, nullptr},
                                           {"binary", LogicalTypeId::BLOB, nullptr},
                                           {"hugeint", LogicalTypeId::HUGEINT, nullptr},
                                           {"int128", LogicalTypeId::HUGEINT, nullptr},
                                           {"uhugeint", LogicalTypeId::UHUGEINT, nullptr},
                                           {"uint128", LogicalTypeId::UHUGEINT, nullptr},
                                           {"bigint", LogicalTypeId::BIGINT, nullptr},
                                           {"oid", LogicalTypeId::BIGINT, nullptr},
                                           {"long", LogicalTypeId::BIGINT, nullptr},
                                           {"int8", LogicalTypeId::BIGINT, nullptr},
                                           {"int64", LogicalTypeId::BIGINT, nullptr},
                                           {"ubigint", LogicalTypeId::UBIGINT, nullptr},
                                           {"uint64", LogicalTypeId::UBIGINT, nullptr},
                                           {"integer", LogicalTypeId::INTEGER, nullptr},
                                           {"int", LogicalTypeId::INTEGER, nullptr},
                                           {"int4", LogicalTypeId::INTEGER, nullptr},
                                           {"signed", LogicalTypeId::INTEGER, nullptr},
                                           {"integral", LogicalTypeId::INTEGER, nullptr},
                                           {"int32", LogicalTypeId::INTEGER, nullptr},
                                           {"uinteger", LogicalTypeId::UINTEGER, nullptr},
                                           {"uint32", LogicalTypeId::UINTEGER, nullptr},
                                           {"smallint", LogicalTypeId::SMALLINT, nullptr},
                                           {"int2", LogicalTypeId::SMALLINT, nullptr},
                                           {"short", LogicalTypeId::SMALLINT, nullptr},
                                           {"int16", LogicalTypeId::SMALLINT, nullptr},
                                           {"usmallint", LogicalTypeId::USMALLINT, nullptr},
                                           {"uint16", LogicalTypeId::USMALLINT, nullptr},
                                           {"tinyint", LogicalTypeId::TINYINT, nullptr},
                                           {"int1", LogicalTypeId::TINYINT, nullptr},
                                           {"utinyint", LogicalTypeId::UTINYINT, nullptr},
                                           {"uint8", LogicalTypeId::UTINYINT, nullptr},
                                           {"struct", LogicalTypeId::STRUCT, BindStructType},
                                           {"row", LogicalTypeId::STRUCT, BindStructType},
                                           {"list", LogicalTypeId::LIST, BindListType},
                                           {"map", LogicalTypeId::MAP, BindMapType},
                                           {"union", LogicalTypeId::UNION, BindUnionType},
                                           {"bit", LogicalTypeId::BIT, nullptr},
                                           {"bitstring", LogicalTypeId::BIT, nullptr},
                                           {"variant", LogicalTypeId::VARIANT, nullptr},
                                           {"bignum", LogicalTypeId::BIGNUM, nullptr},
                                           {"varint", LogicalTypeId::BIGNUM, nullptr},
                                           {"boolean", LogicalTypeId::BOOLEAN, nullptr},
                                           {"bool", LogicalTypeId::BOOLEAN, nullptr},
                                           {"logical", LogicalTypeId::BOOLEAN, nullptr},
                                           {"uuid", LogicalTypeId::UUID, nullptr},
                                           {"guid", LogicalTypeId::UUID, nullptr},
                                           {"enum", LogicalTypeId::ENUM, BindEnumType},
                                           {"null", LogicalTypeId::SQLNULL, nullptr},
                                           {"float", LogicalTypeId::FLOAT, nullptr},
                                           {"real", LogicalTypeId::FLOAT, nullptr},
                                           {"float4", LogicalTypeId::FLOAT, nullptr},
                                           {"double", LogicalTypeId::DOUBLE, nullptr},
                                           {"float8", LogicalTypeId::DOUBLE, nullptr},
                                           {"geometry", LogicalTypeId::GEOMETRY, nullptr},
                                           {"type", LogicalTypeId::TYPE, nullptr}}};

optional_ptr<const DefaultType> TryGetDefaultTypeEntry(const string &name) {
	auto &internal_types = BUILTIN_TYPES;
	for (auto &type : internal_types) {
		if (StringUtil::CIEquals(name, type.name)) {
			return &type;
		}
	}
	return nullptr;
}

} // namespace

//----------------------------------------------------------------------------------------------------------------------
// Default Type Generator
//----------------------------------------------------------------------------------------------------------------------
LogicalTypeId DefaultTypeGenerator::GetDefaultType(const string &name) {
	auto &internal_types = BUILTIN_TYPES;
	for (auto &type : internal_types) {
		if (StringUtil::CIEquals(name, type.name)) {
			return type.type;
		}
	}
	return LogicalType::INVALID;
}

DefaultTypeGenerator::DefaultTypeGenerator(Catalog &catalog, SchemaCatalogEntry &schema)
    : DefaultGenerator(catalog), schema(schema) {
}

unique_ptr<CatalogEntry> DefaultTypeGenerator::CreateDefaultEntry(ClientContext &context, const string &entry_name) {
	if (schema.name != DEFAULT_SCHEMA) {
		return nullptr;
	}
	auto entry = TryGetDefaultTypeEntry(entry_name);
	if (!entry || entry->type == LogicalTypeId::INVALID) {
		return nullptr;
	}
	CreateTypeInfo info;
	info.name = entry_name;
	info.type = LogicalType(entry->type);
	info.internal = true;
	info.temporary = true;
	info.bind_function = entry->bind_function;
	return make_uniq_base<CatalogEntry, TypeCatalogEntry>(catalog, schema, info);
}

vector<string> DefaultTypeGenerator::GetDefaultEntries() {
	vector<string> result;
	if (schema.name != DEFAULT_SCHEMA) {
		return result;
	}
	auto &internal_types = BUILTIN_TYPES;
	for (auto &type : internal_types) {
		result.emplace_back(StringUtil::Lower(type.name));
	}
	return result;
}

} // namespace duckdb
