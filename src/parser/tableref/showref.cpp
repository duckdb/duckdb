#include "duckdb/parser/tableref/showref.hpp"

namespace duckdb {

ShowRef::ShowRef() : TableRef(TableReferenceType::SHOW_REF), show_type(ShowType::DESCRIBE) {
}

string ShowRef::ToString() const {
	string result;
	if (show_type == ShowType::SUMMARY) {
		result += "SUMMARIZE ";
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

	copy->table_name = table_name;
	copy->query = query ? query->Copy() : nullptr;
	copy->show_type = show_type;
	CopyProperties(*copy);

	return std::move(copy);
}

} // namespace duckdb
