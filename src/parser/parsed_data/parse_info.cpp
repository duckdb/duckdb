#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <functional>

namespace duckdb {

string ParseInfo::TypeToString(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		return "TABLE";
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		return "FUNCTION";
	case CatalogType::INDEX_ENTRY:
		return "INDEX";
	case CatalogType::SCHEMA_ENTRY:
		return "SCHEMA";
	case CatalogType::TYPE_ENTRY:
		return "TYPE";
	case CatalogType::VIEW_ENTRY:
		return "VIEW";
	case CatalogType::SEQUENCE_ENTRY:
		return "SEQUENCE";
	case CatalogType::MACRO_ENTRY:
		return "MACRO";
	case CatalogType::TABLE_MACRO_ENTRY:
		return "MACRO TABLE";
	case CatalogType::SECRET_ENTRY:
		return "SECRET";
	default:
		throw InternalException("ParseInfo::TypeToString for CatalogType with type: %s not implemented",
		                        EnumUtil::ToString(type));
	}
}

static string QualifierToStringInternal(const string &catalog, const string &schema, const string &name,
                                        const std::function<string(const string &)> &conversion) {
	string result;
	if (!catalog.empty()) {
		result += conversion(catalog) + ".";
		if (!schema.empty()) {
			result += conversion(schema) + ".";
		}
	} else if (!schema.empty() && schema != DEFAULT_SCHEMA) {
		result += conversion(schema) + ".";
	}
	result += conversion(name);
	return result;
}

string ParseInfo::QualifierToStringNoQuotes(const string &catalog, const string &schema, const string &name) {
	const auto conversion = [](const string &name) {
		return name;
	};
	return QualifierToStringInternal(catalog, schema, name, conversion);
}

string ParseInfo::QualifierToString(const string &catalog, const string &schema, const string &name) {
	const auto conversion = [](const string &name) {
		return KeywordHelper::WriteOptionallyQuoted(name);
	};
	return QualifierToStringInternal(catalog, schema, name, conversion);
}

} // namespace duckdb
