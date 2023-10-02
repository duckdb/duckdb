#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

TableFunctionRef::TableFunctionRef() : TableRef(TableReferenceType::TABLE_FUNCTION) {
}

string TableFunctionRef::ToString() const {
	return BaseToString(function->ToString(), column_name_alias);
}

bool TableFunctionRef::Equals(const TableRef &other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<TableFunctionRef>();
	return function->Equals(*other.function);
}

unique_ptr<TableRef> TableFunctionRef::Copy() {
	auto copy = make_uniq<TableFunctionRef>();

	copy->function = function->Copy();
	copy->column_name_alias = column_name_alias;
	CopyProperties(*copy);

	return std::move(copy);
}

} // namespace duckdb
