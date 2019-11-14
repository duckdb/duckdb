#include "duckdb/parser/tableref/emptytableref.hpp"

#include "duckdb/common/serializer.hpp"

using namespace duckdb;
using namespace std;

bool EmptyTableRef::Equals(const TableRef *other_) const {
	return true; // they are always the same they only have a type
}

unique_ptr<TableRef> EmptyTableRef::Copy() {
	return make_unique<EmptyTableRef>();
}

void EmptyTableRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);
}

unique_ptr<TableRef> EmptyTableRef::Deserialize(Deserializer &source) {
	return make_unique<EmptyTableRef>();
}
