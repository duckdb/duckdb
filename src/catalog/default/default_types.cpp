#include "duckdb/catalog/default/default_types.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/common/types/geometry_crs.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

namespace {

//----------------------------------------------------------------------------------------------------------------------
// DECIMAL Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindDecimalType(BindLogicalTypeInput &input) {
	auto &modifiers = input.modifiers;

	uint8_t width = 18;
	uint8_t scale = 3;

	if (!modifiers.empty()) {
		auto width_value = modifiers[0].GetValue();
		if (width_value.IsNull()) {
			throw BinderException("DECIMAL type width cannot be NULL");
		}
		if (width_value.DefaultTryCastAs(LogicalTypeId::UTINYINT)) {
			width = width_value.GetValueUnsafe<uint8_t>();
			scale = 0; // reset scale to 0 if only width is provided
		} else {
			throw BinderException("DECIMAL type width must be between 1 and %d", Decimal::MAX_WIDTH_DECIMAL);
		}
	}

	if (modifiers.size() > 1) {
		auto scale_value = modifiers[1].GetValue();
		if (scale_value.IsNull()) {
			throw BinderException("DECIMAL type scale cannot be NULL");
		}
		if (scale_value.DefaultTryCastAs(LogicalTypeId::UTINYINT)) {
			scale = scale_value.GetValueUnsafe<uint8_t>();
		} else {
			throw BinderException("DECIMAL type scale must be between 0 and %d", Decimal::MAX_WIDTH_DECIMAL - 1);
		}
	}

	if (modifiers.size() > 2) {
		throw BinderException("DECIMAL type can take at most two type modifiers, width and scale");
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
LogicalType BindTimestampType(BindLogicalTypeInput &input) {
	auto &modifiers = input.modifiers;

	if (modifiers.empty()) {
		return LogicalType::TIMESTAMP;
	}

	if (modifiers.size() > 1) {
		throw BinderException("TIMESTAMP type takes at most one type modifier");
	}

	auto precision_value = modifiers[0].GetValue();
	if (precision_value.IsNull()) {
		throw BinderException("TIMESTAMP type precision cannot be NULL");
	}
	uint8_t precision;
	if (precision_value.DefaultTryCastAs(LogicalTypeId::UTINYINT)) {
		precision = precision_value.GetValueUnsafe<uint8_t>();
	} else {
		throw BinderException("TIMESTAMP type precision must be between 0 and 9");
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
LogicalType BindVarcharType(BindLogicalTypeInput &input) {
	// Varchar type can have a single modifier indicating the length, but we ignore it for now
	auto &modifiers = input.modifiers;

	if (!modifiers.empty() && modifiers.size() <= 2) {
		for (auto &mod : modifiers) {
			if (mod.IsNamed("collation") && mod.GetType() == LogicalType::VARCHAR && mod.IsNotNull()) {
				// Ignore all other modifiers and return collation type
				auto collation = StringValue::Get(mod.GetValue());

				if (!input.context) {
					throw BinderException("Cannot bind varchar with collation without a connection");
				}

				// Ensure this is a valid collation
				ExpressionBinder::TestCollation(*input.context, collation);

				return LogicalType::VARCHAR_COLLATION(collation);
			}
		}
	}

	if (modifiers.size() > 1) {
		throw BinderException("VARCHAR type takes at most one type modifier");
	}

	return LogicalType::VARCHAR;
}

//----------------------------------------------------------------------------------------------------------------------
// BIT Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindBitType(BindLogicalTypeInput &input) {
	// BIT type can have a single modifier indicating the length, but we ignore it for now
	auto &args = input.modifiers;
	if (args.size() > 1) {
		throw BinderException("BIT type takes at most one type modifier");
	}
	return LogicalType::BIT;
}

//----------------------------------------------------------------------------------------------------------------------
// INTERVAL Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindIntervalType(BindLogicalTypeInput &input) {
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
LogicalType BindEnumType(BindLogicalTypeInput &input) {
	auto &arguments = input.modifiers;

	if (arguments.empty()) {
		throw BinderException("ENUM type requires at least one argument");
	}

	Vector enum_vector(LogicalType::VARCHAR, NumericCast<idx_t>(arguments.size()));
	auto string_data = FlatVector::GetData<string_t>(enum_vector);

	for (idx_t arg_idx = 0; arg_idx < arguments.size(); arg_idx++) {
		auto &arg = arguments[arg_idx];
		if (arg.HasName()) {
			throw BinderException("ENUM type arguments cannot have names (argument %d has name \"%s\")", arg_idx + 1,
			                      arg.GetName());
		}

		if (arg.GetValue().type() != LogicalTypeId::VARCHAR) {
			throw BinderException("ENUM type requires a set of VARCHAR arguments");
		}

		if (arg.GetValue().IsNull()) {
			throw BinderException("ENUM type arguments cannot be NULL (argument %d is NULL)", arg_idx + 1);
		}

		string_data[arg_idx] = StringVector::AddString(enum_vector, StringValue::Get(arg.GetValue()));
	}

	return LogicalType::ENUM(enum_vector, NumericCast<idx_t>(arguments.size()));
}

//----------------------------------------------------------------------------------------------------------------------
// LIST Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindListType(BindLogicalTypeInput &input) {
	auto &arguments = input.modifiers;
	if (arguments.size() != 1) {
		throw BinderException("LIST type requires exactly one type modifier");
	}
	auto &child_val = arguments[0].GetValue();
	if (child_val.IsNull()) {
		throw BinderException("LIST type modifier cannot be NULL");
	}
	if (child_val.type() != LogicalTypeId::TYPE) {
		throw BinderException("LIST type modifier must be a type, but got %s", child_val.ToString());
	}

	auto child_type = TypeValue::GetType(arguments[0].GetValue());
	return LogicalType::LIST(child_type);
}

//----------------------------------------------------------------------------------------------------------------------
// ARRAY Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindArrayType(BindLogicalTypeInput &input) {
	auto &arguments = input.modifiers;
	if (arguments.size() != 2) {
		throw BinderException("ARRAY type requires exactly two type modifiers");
	}
	auto &elem_val = arguments[0].GetValue();
	if (elem_val.IsNull()) {
		throw BinderException("ARRAY type modifier cannot be NULL");
	}
	if (elem_val.type() != LogicalTypeId::TYPE) {
		throw BinderException("ARRAY type modifier must be a type, but got %s", elem_val.ToString());
	}

	auto size_val = arguments[1].GetValue();
	if (size_val.IsNull()) {
		throw BinderException("ARRAY type size modifier cannot be NULL");
	}
	if (!size_val.type().IsIntegral()) {
		throw BinderException("ARRAY type size modifier must be an integral type");
	}
	if (!size_val.DefaultTryCastAs(LogicalTypeId::BIGINT)) {
		throw BinderException("ARRAY type size modifier must be a BIGINT");
	}

	auto array_size = size_val.GetValueUnsafe<int64_t>();

	if (array_size < 1) {
		throw BinderException("ARRAY type size must be at least 1");
	}
	if (array_size > static_cast<int64_t>(ArrayType::MAX_ARRAY_SIZE)) {
		throw BinderException("ARRAY type size must be at most %d", ArrayType::MAX_ARRAY_SIZE);
	}

	auto child_type = TypeValue::GetType(arguments[0].GetValue());
	return LogicalType::ARRAY(child_type, UnsafeNumericCast<idx_t>(array_size));
}

//----------------------------------------------------------------------------------------------------------------------
// STRUCT Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindStructType(BindLogicalTypeInput &input) {
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

		// And not null!
		if (arg.GetValue().IsNull()) {
			throw BinderException("STRUCT type arguments cannot be NULL");
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
LogicalType BindMapType(BindLogicalTypeInput &input) {
	auto &arguments = input.modifiers;

	if (arguments.size() != 2) {
		throw BinderException("MAP type requires exactly two type modifiers: key type and value type");
	}

	auto &key_val = arguments[0].GetValue();
	auto &val_val = arguments[1].GetValue();

	if (key_val.type() != LogicalTypeId::TYPE || val_val.type() != LogicalTypeId::TYPE) {
		throw BinderException("MAP type modifiers must be types");
	}
	if (key_val.IsNull()) {
		throw BinderException("MAP type key type modifier cannot be NULL");
	}
	if (val_val.IsNull()) {
		throw BinderException("MAP type value type modifier cannot be NULL");
	}

	auto key_type = TypeValue::GetType(arguments[0].GetValue());
	auto val_type = TypeValue::GetType(arguments[1].GetValue());

	return LogicalType::MAP(std::move(key_type), std::move(val_type));
}

//----------------------------------------------------------------------------------------------------------------------
// UNION Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindUnionType(BindLogicalTypeInput &input) {
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
		if (arg.GetValue().IsNull()) {
			throw BinderException("UNION type modifiers cannot be NULL");
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
// VARIANT Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindVariantType(BindLogicalTypeInput &input) {
	// We need this function to make sure we always create a VARIANT type with ExtraTypeInfo
	auto &arguments = input.modifiers;

	if (!arguments.empty()) {
		throw BinderException("Type 'VARIANT' does not take any type parameters");
	}
	return LogicalType::VARIANT();
}

//----------------------------------------------------------------------------------------------------------------------
// GEOMETRY Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindGeometryType(BindLogicalTypeInput &input) {
	auto &arguments = input.modifiers;

	if (arguments.empty()) {
		return LogicalType::GEOMETRY();
	}

	if (arguments.size() > 1) {
		throw BinderException(
		    "GEOMETRY type takes a single optional type modifier with a coordinate system definition");
	}

	const auto &crs_value = arguments[0].GetValue();

	// Don't do any casting here - only accept string type directly
	if (crs_value.type() != LogicalTypeId::VARCHAR) {
		throw BinderException("GEOMETRY type modifier must be a string with a coordinate system definition");
	}
	if (crs_value.IsNull()) {
		throw BinderException("GEOMETRY type modifier cannot be NULL");
	}

	// FIXME: Use extension/ClientContext to expand incomplete/shorthand CRS definitions
	auto &crs = StringValue::Get(crs_value);

	if (!input.context) {
		throw BinderException("Cannot create GEOMETRY type with coordinate system without a connection");
	}

	const auto crs_result = CoordinateReferenceSystem::TryIdentify(*input.context, crs);
	if (!crs_result) {
		if (Settings::Get<IgnoreUnknownCrsSetting>(*input.context)) {
			// Ignored by user configuration - return generic GEOMETRY type
			return LogicalType::GEOMETRY();
		}

		throw BinderException(
		    "Encountered unrecognized coordinate system '%s' when trying to create GEOMETRY type\n"
		    "The coordinate system definition may be incomplete or invalid. Your options are as follows:\n"
		    "* Load an extension that can identify this coordinate system\n"
		    "* Provide a full coordinate system definition in e.g. \"PROJJSON\" or \"WKT2\" format\n"
		    "* Set the 'ignore_unknown_crs' configuration option to drop the coordinate system from the resulting "
		    "geometry type and make this error go away",
		    crs);
	}

	return LogicalType::GEOMETRY(crs_result->GetDefinition());
}

//----------------------------------------------------------------------------------------------------------------------
// All Types
//----------------------------------------------------------------------------------------------------------------------

struct DefaultType {
	const char *name;
	LogicalTypeId type;
	bind_logical_type_function_t bind_function;
};

using builtin_type_array = std::array<DefaultType, 81>;

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
                                           {"timestamp with time zone", LogicalTypeId::TIMESTAMP_TZ, nullptr},
                                           {"timetz", LogicalTypeId::TIME_TZ, nullptr},
                                           {"time with time zone", LogicalTypeId::TIME_TZ, nullptr},
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
                                           {"array", LogicalTypeId::ARRAY, BindArrayType},
                                           {"map", LogicalTypeId::MAP, BindMapType},
                                           {"union", LogicalTypeId::UNION, BindUnionType},
                                           {"bit", LogicalTypeId::BIT, BindBitType},
                                           {"bitstring", LogicalTypeId::BIT, BindBitType},
                                           {"variant", LogicalTypeId::VARIANT, BindVariantType},
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
                                           {"geometry", LogicalTypeId::GEOMETRY, BindGeometryType},
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

LogicalType DefaultTypeGenerator::TryDefaultBind(const string &name, const vector<pair<string, Value>> &params) {
	auto entry = TryGetDefaultTypeEntry(name);
	if (!entry) {
		return LogicalTypeId::INVALID;
	}

	if (!entry->bind_function) {
		if (params.empty()) {
			return LogicalType(entry->type);
		} else {
			throw InvalidInputException("Type '%s' does not take any type parameters", name);
		}
	}

	vector<TypeArgument> args;
	for (auto &param : params) {
		args.emplace_back(param.first, param.second);
	}

	BindLogicalTypeInput input {nullptr, LogicalType(entry->type), args};
	return entry->bind_function(input);
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
