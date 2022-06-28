#include "duckdb/catalog/default/default_types.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"

namespace duckdb {

struct DefaultType {
	const char *name;
	LogicalTypeId type;
};

static DefaultType internal_types[] = {{"int", LogicalTypeId::INTEGER},
                                       {"int4", LogicalTypeId::INTEGER},
                                       {"signed", LogicalTypeId::INTEGER},
                                       {"integer", LogicalTypeId::INTEGER},
                                       {"integral", LogicalTypeId::INTEGER},
                                       {"int32", LogicalTypeId::INTEGER},
                                       {"varchar", LogicalTypeId::VARCHAR},
                                       {"bpchar", LogicalTypeId::VARCHAR},
                                       {"text", LogicalTypeId::VARCHAR},
                                       {"string", LogicalTypeId::VARCHAR},
                                       {"char", LogicalTypeId::VARCHAR},
                                       {"nvarchar", LogicalTypeId::VARCHAR},
                                       {"bytea", LogicalTypeId::BLOB},
                                       {"blob", LogicalTypeId::BLOB},
                                       {"varbinary", LogicalTypeId::BLOB},
                                       {"binary", LogicalTypeId::BLOB},
                                       {"int8", LogicalTypeId::BIGINT},
                                       {"bigint", LogicalTypeId::BIGINT},
                                       {"int64", LogicalTypeId::BIGINT},
                                       {"long", LogicalTypeId::BIGINT},
                                       {"oid", LogicalTypeId::BIGINT},
                                       {"int2", LogicalTypeId::SMALLINT},
                                       {"smallint", LogicalTypeId::SMALLINT},
                                       {"short", LogicalTypeId::SMALLINT},
                                       {"int16", LogicalTypeId::SMALLINT},
                                       {"timestamp", LogicalTypeId::TIMESTAMP},
                                       {"datetime", LogicalTypeId::TIMESTAMP},
                                       {"timestamp_us", LogicalTypeId::TIMESTAMP},
                                       {"timestamp_ms", LogicalTypeId::TIMESTAMP_MS},
                                       {"timestamp_ns", LogicalTypeId::TIMESTAMP_NS},
                                       {"timestamp_s", LogicalTypeId::TIMESTAMP_SEC},
                                       {"bool", LogicalTypeId::BOOLEAN},
                                       {"boolean", LogicalTypeId::BOOLEAN},
                                       {"logical", LogicalTypeId::BOOLEAN},
                                       {"decimal", LogicalTypeId::DECIMAL},
                                       {"dec", LogicalTypeId::DECIMAL},
                                       {"numeric", LogicalTypeId::DECIMAL},
                                       {"real", LogicalTypeId::FLOAT},
                                       {"float4", LogicalTypeId::FLOAT},
                                       {"float", LogicalTypeId::FLOAT},
                                       {"double", LogicalTypeId::DOUBLE},
                                       {"float8", LogicalTypeId::DOUBLE},
                                       {"tinyint", LogicalTypeId::TINYINT},
                                       {"int1", LogicalTypeId::TINYINT},
                                       {"date", LogicalTypeId::DATE},
                                       {"time", LogicalTypeId::TIME},
                                       {"interval", LogicalTypeId::INTERVAL},
                                       {"hugeint", LogicalTypeId::HUGEINT},
                                       {"int128", LogicalTypeId::HUGEINT},
                                       {"uuid", LogicalTypeId::UUID},
                                       {"guid", LogicalTypeId::UUID},
                                       {"struct", LogicalTypeId::STRUCT},
                                       {"row", LogicalTypeId::STRUCT},
                                       {"list", LogicalTypeId::LIST},
                                       {"map", LogicalTypeId::MAP},
                                       {"utinyint", LogicalTypeId::UTINYINT},
                                       {"uint8", LogicalTypeId::UTINYINT},
                                       {"usmallint", LogicalTypeId::USMALLINT},
                                       {"uint16", LogicalTypeId::USMALLINT},
                                       {"uinteger", LogicalTypeId::UINTEGER},
                                       {"uint32", LogicalTypeId::UINTEGER},
                                       {"ubigint", LogicalTypeId::UBIGINT},
                                       {"uint64", LogicalTypeId::UBIGINT},
                                       {"timestamptz", LogicalTypeId::TIMESTAMP_TZ},
                                       {"timetz", LogicalTypeId::TIME_TZ},
                                       {"json", LogicalTypeId::JSON},
                                       {"null", LogicalTypeId::SQLNULL},
                                       {nullptr, LogicalTypeId::INVALID}};

LogicalTypeId DefaultTypeGenerator::GetDefaultType(const string &name) {
	auto lower_str = StringUtil::Lower(name);
	for (idx_t index = 0; internal_types[index].name != nullptr; index++) {
		if (internal_types[index].name == lower_str) {
			return internal_types[index].type;
		}
	}
	return LogicalTypeId::INVALID;
}

DefaultTypeGenerator::DefaultTypeGenerator(Catalog &catalog, SchemaCatalogEntry *schema)
    : DefaultGenerator(catalog), schema(schema) {
}

unique_ptr<CatalogEntry> DefaultTypeGenerator::CreateDefaultEntry(ClientContext &context, const string &entry_name) {
	if (schema->name != DEFAULT_SCHEMA) {
		return nullptr;
	}
	auto type_id = GetDefaultType(entry_name);
	if (type_id == LogicalTypeId::INVALID) {
		return nullptr;
	}
	CreateTypeInfo info;
	info.name = entry_name;
	info.type = LogicalType(type_id);
	info.internal = true;
	info.temporary = true;
	return make_unique_base<CatalogEntry, TypeCatalogEntry>(&catalog, schema, &info);
}

vector<string> DefaultTypeGenerator::GetDefaultEntries() {
	vector<string> result;
	if (schema->name != DEFAULT_SCHEMA) {
		return result;
	}
	for (idx_t index = 0; internal_types[index].name != nullptr; index++) {
		result.emplace_back(internal_types[index].name);
	}
	return result;
}

} // namespace duckdb
