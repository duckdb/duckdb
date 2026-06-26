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
		if (!GetCatalogName().empty()) {
			name += SQLIdentifier(GetCatalogName());
			if (!GetSchemaName().empty()) {
				name += ".";
			}
		}
		name += SQLIdentifier(GetSchemaName());
		result += name;
	} else {
		result += "DESCRIBE ";
	}
	if (query) {
		result += "(";
		result += query->ToString();
		result += ")";
	} else if (GetTableName() != "__show_tables_expanded") {
		result += GetTableName().GetIdentifierName();
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
	return GetTableName() == other.GetTableName() && show_type == other.show_type;
}

unique_ptr<TableRef> ShowRef::Copy() {
	auto copy = make_uniq<ShowRef>();

	copy->qualified_name = qualified_name;
	copy->query = query ? query->Copy() : nullptr;
	copy->show_type = show_type;
	CopyProperties(*copy);

	return std::move(copy);
}

} // namespace duckdb
