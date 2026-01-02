#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/exception/parser_exception.hpp"

namespace duckdb {

string QualifiedName::ToString() const {
	return ParseInfo::QualifierToString(catalog, schema, name);
}

vector<string> QualifiedName::ParseComponents(const string &input) {
	vector<string> result;
	idx_t idx = 0;
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
	result.push_back(entry);
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
	throw ParserException("Unterminated quote in qualified name! (input: %s)", input);
end:
	if (!entry.empty()) {
		result.push_back(entry);
	}
	return result;
}

QualifiedName QualifiedName::Parse(const string &input) {
	string catalog;
	string schema;
	string name;

	auto entries = ParseComponents(input);
	if (entries.empty()) {
		catalog = INVALID_CATALOG;
		schema = INVALID_SCHEMA;
	} else if (entries.size() == 1) {
		catalog = INVALID_CATALOG;
		schema = INVALID_SCHEMA;
		name = entries[0];
	} else if (entries.size() == 2) {
		catalog = INVALID_CATALOG;
		schema = entries[0];
		name = entries[1];
	} else if (entries.size() == 3) {
		catalog = entries[0];
		schema = entries[1];
		name = entries[2];
	} else {
		throw ParserException("Expected catalog.entry, schema.entry or entry: too many entries found (input: %s)",
		                      input);
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

QualifiedColumnName QualifiedColumnName::Parse(string &input) {
	auto components = QualifiedName::ParseComponents(input);
	if (components.size() == 1) {
		return QualifiedColumnName(components[0]);
	} else if (components.size() == 2) {
		return QualifiedColumnName(components[0], components[1]);
	} else if (components.size() == 3) {
		QualifiedColumnName qname;
		qname.schema = components[0];
		qname.table = components[1];
		qname.column = components[2];
		return qname;
	} else if (components.size() == 4) {
		QualifiedColumnName qname;
		qname.catalog = components[0];
		qname.schema = components[1];
		qname.table = components[2];
		qname.column = components[3];
		return qname;
	} else {
		throw ParserException(
		    "Expected at most 4 entries (catalog.schema.table.column), but found %zu entries (input: %s)",
		    components.size(), input);
	}
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
