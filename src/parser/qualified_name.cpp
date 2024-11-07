#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

string QualifiedName::ToString() const {
	return ParseInfo::QualifierToString(catalog, schema, name);
}

QualifiedName QualifiedName::Parse(const string &input) {
	string catalog;
	string schema;
	string name;
	idx_t idx = 0;
	vector<string> entries;
	string entry;
normal:
	//! quote
	for (; idx < input.size(); idx++) {
		if (input[idx] == '"') {
			idx++;
			goto quoted;
		} else if (input[idx] == '.') {
			goto separator;
		}
		entry += input[idx];
	}
	goto end;
separator:
	entries.push_back(entry);
	entry = "";
	idx++;
	goto normal;
quoted:
	//! look for another quote
	for (; idx < input.size(); idx++) {
		if (input[idx] == '"') {
			//! unquote
			idx++;
			goto normal;
		}
		entry += input[idx];
	}
	throw ParserException("Unterminated quote in qualified name!");
end:
	if (entries.empty()) {
		catalog = INVALID_CATALOG;
		schema = INVALID_SCHEMA;
		name = entry;
	} else if (entries.size() == 1) {
		catalog = INVALID_CATALOG;
		schema = entries[0];
		name = entry;
	} else if (entries.size() == 2) {
		catalog = entries[0];
		schema = entries[1];
		name = entry;
	} else {
		throw ParserException("Expected catalog.entry, schema.entry or entry: too many entries found");
	}
	return QualifiedName {catalog, schema, name};
}

QualifiedColumnName::QualifiedColumnName() {
}
QualifiedColumnName::QualifiedColumnName(string column_p) : column(std::move(column_p)) {
}
QualifiedColumnName::QualifiedColumnName(string table_p, string column_p)
    : table(std::move(table_p)), column(std::move(column_p)) {
}
QualifiedColumnName::QualifiedColumnName(const BindingAlias &alias, string column_p)
    : catalog(alias.GetCatalog()), schema(alias.GetSchema()), table(alias.GetAlias()), column(std::move(column_p)) {
}

string QualifiedColumnName::ToString() const {
	string result;
	if (!catalog.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(catalog) + ".";
	}
	if (!schema.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(schema) + ".";
	}
	if (!table.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(table) + ".";
	}
	result += KeywordHelper::WriteOptionallyQuoted(column);
	return result;
}

bool QualifiedColumnName::IsQualified() const {
	return !catalog.empty() || !schema.empty() || !table.empty();
}

bool QualifiedColumnName::operator==(const QualifiedColumnName &rhs) const {
	return StringUtil::CIEquals(catalog, rhs.catalog) && StringUtil::CIEquals(schema, rhs.schema) &&
	       StringUtil::CIEquals(table, rhs.table) && StringUtil::CIEquals(column, rhs.column);
}

} // namespace duckdb
