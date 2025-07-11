#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

ShowRef::ShowRef() : TableRef(TableReferenceType::SHOW_REF), show_type(ShowType::DESCRIBE) {
}

string ShowRef::ToString() const {
	string result;
	if (show_type == ShowType::SUMMARY) {
		result += "SUMMARIZE ";
	} else if (show_type == ShowType::SHOW_FROM) {
		result += "SHOW TABLES FROM ";
		string name = "";
		if (!catalog_name.empty()) {
			name += KeywordHelper::WriteOptionallyQuoted(catalog_name, '"');
			if (!schema_name.empty()) {
				name += ".";
			}
		}
		name += KeywordHelper::WriteOptionallyQuoted(schema_name, '"');
		result += name;
	} else {
		result += "DESCRIBE ";
	}
	if (query) {
		result += "(";
		result += query->ToString();
		result += ")";
	} else if (table_name != "__show_tables_expanded") {
		result += table_name;
	}
	return result;
}

bool ShowRef::Equals(const TableRef &other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<ShowRef>();
	if (other.query.get() != query.get()) {
		if (!other.query->Equals(query.get())) {
			return false;
		}
	}
	return table_name == other.table_name && show_type == other.show_type;
}

unique_ptr<TableRef> ShowRef::Copy() {
	auto copy = make_uniq<ShowRef>();

	copy->catalog_name = catalog_name;
	copy->schema_name = schema_name;
	copy->table_name = table_name;
	copy->query = query ? query->Copy() : nullptr;
	copy->show_type = show_type;
	CopyProperties(*copy);

	return std::move(copy);
}

} // namespace duckdb
