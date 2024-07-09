#include "duckdb/parser/tableref/delimgetref.hpp"

namespace duckdb {

string DelimGetRef::ToString() const {
	return "";
}

bool DelimGetRef::Equals(const TableRef &other) const {
	return TableRef::Equals(other);
}

unique_ptr<TableRef> DelimGetRef::Copy() {
	return make_uniq<DelimGetRef>(types);
}

void DelimGetRef::Serialize(Serializer &serializer) const {
	// FIXME: Serialize Types
	TableRef::Serialize(serializer);
}

unique_ptr<TableRef> DelimGetRef::Deserialize(Deserializer &deserializer) {
	// FIXME: Deserliaze Types
	vector<LogicalType> types;
	auto result = duckdb::unique_ptr<DelimGetRef>(new DelimGetRef(types));
	return std::move(result);
}

} // namespace duckdb
