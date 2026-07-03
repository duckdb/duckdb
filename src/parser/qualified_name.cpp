#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/planner/binding_alias.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

void QualifiedName::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<vector<Identifier>>(100, "path", path);
}

QualifiedName QualifiedName::Deserialize(Deserializer &deserializer) {
	QualifiedName result;
	result.path = deserializer.ReadPropertyWithDefault<vector<Identifier>>(100, "path");
	return result;
}

string QualifiedName::ToString(QualifiedNameToStringMode mode) const {
	const auto &catalog = Catalog();
	const auto &schema = Schema();
	string result;
	if (!catalog.empty()) {
		result += SQLIdentifier(catalog) + ".";
		if (!schema.empty()) {
			result += SQLIdentifier(schema) + ".";
		}
	} else if (!schema.empty() &&
	           !(mode == QualifiedNameToStringMode::HIDE_DEFAULT_SCHEMA && schema == DEFAULT_SCHEMA)) {
		result += SQLIdentifier(schema) + ".";
	}
	result += SQLIdentifier(Name());
	return result;
}

vector<Identifier> QualifiedName::ParseComponents(const string &input) {
	vector<Identifier> result;
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
	result.push_back(Identifier(entry));
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
		result.push_back(Identifier(entry));
	}
	return result;
}

hash_t QualifiedName::Hash() const {
	hash_t result = Catalog().Hash();
	result = CombineHash(result, Schema().Hash());
	result = CombineHash(result, Name().Hash());
	return result;
}

bool QualifiedName::operator==(const QualifiedName &rhs) const {
	return Catalog() == rhs.Catalog() && Schema() == rhs.Schema() && Name() == rhs.Name();
}

bool QualifiedName::operator!=(const QualifiedName &rhs) const {
	return !(*this == rhs);
}

QualifiedName QualifiedName::Parse(const string &input) {
	auto entries = ParseComponents(input);
	if (entries.size() > 3) {
		throw ParserException("Expected catalog.entry, schema.entry or entry: too many entries found (input: %s)",
		                      input);
	}
	if (entries.empty()) {
		return QualifiedName();
	}
	// the last component is the name, anything before it is the schema path (at most [catalog, schema])
	Identifier name = std::move(entries.back());
	entries.pop_back();
	return QualifiedName(std::move(entries), std::move(name));
}

QualifiedColumnName::QualifiedColumnName() {
}
QualifiedColumnName::QualifiedColumnName(Identifier column_p) : column(std::move(column_p)) {
}
QualifiedColumnName::QualifiedColumnName(Identifier table_p, Identifier column_p)
    : table(std::move(table_p)), column(std::move(column_p)) {
}
QualifiedColumnName::QualifiedColumnName(const BindingAlias &alias, Identifier column_p)
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
		result += SQLIdentifier(catalog) + ".";
	}
	if (!schema.empty()) {
		result += SQLIdentifier(schema) + ".";
	}
	if (!table.empty()) {
		result += SQLIdentifier(table) + ".";
	}
	result += SQLIdentifier(column);
	return result;
}

bool QualifiedColumnName::IsQualified() const {
	return !catalog.empty() || !schema.empty() || !table.empty();
}

bool QualifiedColumnName::operator==(const QualifiedColumnName &rhs) const {
	return catalog == rhs.catalog && schema == rhs.schema && table == rhs.table && column == rhs.column;
}

} // namespace duckdb
