#include "duckdb/parser/tableref/showref.hpp"

namespace duckdb {

ShowRef::ShowRef() : TableRef(TableReferenceType::SHOW_REF) {
}

string ShowRef::ToString() const {
	string result;
	if (is_summary) {
		result += "SUMMARIZE ";
	} else {
		result += "DESCRIBE ";
	}
	result += query->ToString();
	return result;
}

bool ShowRef::Equals(const TableRef &other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<ShowRef>();
	return other.query->Equals(query.get()) && is_summary == other.is_summary;
}

unique_ptr<TableRef> ShowRef::Copy() {
	auto copy = make_uniq<ShowRef>();

	copy->query = query->Copy();
	copy->is_summary = is_summary;
	CopyProperties(*copy);

	return std::move(copy);
}

}
