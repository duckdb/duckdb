#include "duckdb/parser/tableref/emptytableref.hpp"

namespace duckdb {

string EmptyTableRef::ToString() const {
	return "";
}

bool EmptyTableRef::Equals(const TableRef &other) const {
	return TableRef::Equals(other);
}

unique_ptr<TableRef> EmptyTableRef::Copy() {
	return make_uniq<EmptyTableRef>();
}

} // namespace duckdb
