#include "duckdb/parser/tableref/delimgetref.hpp"

namespace duckdb {

string DelimGetRef::ToString() const {
	return "";
}

bool DelimGetRef::Equals(const TableRef &other) const {
	return TableRef::Equals(other);
}

unique_ptr<TableRef> DelimGetRef::Copy() {
	return make_uniq<DelimGetRef>();
}

void DelimGetRef::Serialize(Serializer &serializer) const {
	TableRef::Serialize(serializer);
}

unique_ptr<TableRef> DelimGetRef::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<DelimGetRef>(new DelimGetRef());
	return std::move(result);
}

} // namespace duckdb
