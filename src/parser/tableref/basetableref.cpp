#include "duckdb/parser/tableref/basetableref.hpp"

#include "duckdb/common/serializer.hpp"

namespace duckdb {
using namespace std;

bool BaseTableRef::Equals(const TableRef *other_) const {
	if (!TableRef::Equals(other_)) {
		return false;
	}
	auto other = (BaseTableRef *)other_;
	return other->schema_name == schema_name && other->table_name == table_name && column_name_alias == other->column_name_alias;
}

void BaseTableRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);

	serializer.WriteString(schema_name);
	serializer.WriteString(table_name);
	serializer.WriteStringVector(column_name_alias);
}

unique_ptr<TableRef> BaseTableRef::Deserialize(Deserializer &source) {
	auto result = make_unique<BaseTableRef>();

	result->schema_name = source.Read<string>();
	result->table_name = source.Read<string>();
	source.ReadStringVector(result->column_name_alias);

	return move(result);
}

unique_ptr<TableRef> BaseTableRef::Copy() {
	auto copy = make_unique<BaseTableRef>();

	copy->schema_name = schema_name;
	copy->table_name = table_name;
	copy->alias = alias;
	copy->column_name_alias = column_name_alias;

	return move(copy);
}
} // namespace duckdb
