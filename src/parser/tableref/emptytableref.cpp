#include "duckdb/parser/tableref/emptytableref.hpp"

#include "duckdb/common/field_writer.hpp"

namespace duckdb {

string EmptyTableRef::ToString() const {
	return "";
}

bool EmptyTableRef::Equals(const TableRef *other) const {
	return TableRef::Equals(other);
}

unique_ptr<TableRef> EmptyTableRef::Copy() {
	return make_unique<EmptyTableRef>();
}

void EmptyTableRef::Serialize(FieldWriter &writer) const {
}

unique_ptr<TableRef> EmptyTableRef::Deserialize(FieldReader &reader) {
	return make_unique<EmptyTableRef>();
}

} // namespace duckdb
