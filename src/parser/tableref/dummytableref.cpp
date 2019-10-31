#include "parser/tableref/dummytableref.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

bool DummyTableRef::Equals(const TableRef *other_) const {
	return true; // they are always the same they only have a type
}

unique_ptr<TableRef> DummyTableRef::Copy() {
	return make_unique<DummyTableRef>();
}

void DummyTableRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);
}

unique_ptr<TableRef> DummyTableRef::Deserialize(Deserializer &source) {
	return make_unique<DummyTableRef>();
}
