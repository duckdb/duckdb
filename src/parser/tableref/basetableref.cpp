#include "duckdb/parser/tableref/basetableref.hpp"

#include "duckdb/common/serializer.hpp"

using namespace duckdb;
using namespace std;

bool BaseTableRef::Equals(const TableRef *other_) const {
	if (!TableRef::Equals(other_)) {
		return false;
	}
	auto other = (BaseTableRef *)other_;
	return other->schema_name == schema_name && other->table_name == table_name;
}

void BaseTableRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);

	serializer.WriteString(schema_name);
	serializer.WriteString(table_name);
}

unique_ptr<TableRef> BaseTableRef::Deserialize(Deserializer &source) {
	auto result = make_unique<BaseTableRef>();

	result->schema_name = source.Read<string>();
	result->table_name = source.Read<string>();

	return move(result);
}

unique_ptr<TableRef> BaseTableRef::Copy() {
	auto copy = make_unique<BaseTableRef>();

	copy->schema_name = schema_name;
	copy->table_name = table_name;
	copy->alias = alias;

	return move(copy);
}
