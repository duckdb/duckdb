#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

TableFunctionRef::TableFunctionRef() : TableRef(TableReferenceType::TABLE_FUNCTION) {
}

string TableFunctionRef::ToString() const {
	return BaseToString(function->ToString(), column_name_alias);
}

bool TableFunctionRef::Equals(const TableRef *other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto other = (TableFunctionRef *)other_p;
	return function->Equals(other->function.get());
}

void TableFunctionRef::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*function);
	writer.WriteString(alias);
	writer.WriteList<string>(column_name_alias);
}

unique_ptr<TableRef> TableFunctionRef::Deserialize(FieldReader &reader) {
	auto result = make_unique<TableFunctionRef>();
	result->function = reader.ReadRequiredSerializable<ParsedExpression>();
	result->alias = reader.ReadRequired<string>();
	result->column_name_alias = reader.ReadRequiredList<string>();
	return move(result);
}

unique_ptr<TableRef> TableFunctionRef::Copy() {
	auto copy = make_unique<TableFunctionRef>();

	copy->function = function->Copy();
	copy->column_name_alias = column_name_alias;
	CopyProperties(*copy);

	return move(copy);
}

} // namespace duckdb
