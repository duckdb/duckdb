#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

TableFunctionRef::TableFunctionRef() : TableRef(TableReferenceType::TABLE_FUNCTION) {
}

string TableFunctionRef::ToString() const {
	return BaseToString(function->ToString(), column_name_alias, column_type_hint);
}

bool TableFunctionRef::Equals(const TableRef &other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<TableFunctionRef>();
	if (!function->Equals(*other.function)) {
		return false;
	}
	if (column_type_hint.size() != other.column_type_hint.size()) {
		return false;
	}
	for (idx_t i = 0; i < column_name_alias.size(); i++) {
		if (!StringUtil::CIEquals(column_name_alias[i], other.column_name_alias[i])) {
			return false;
		}

		if (!column_type_hint.empty()) {
			if (column_type_hint[i] != other.column_type_hint[i]) {
				return false;
			}
		}
	}
	return true;
}

unique_ptr<TableRef> TableFunctionRef::Copy() {
	auto copy = make_uniq<TableFunctionRef>();

	copy->function = function->Copy();
	copy->column_name_alias = column_name_alias;
	copy->column_type_hint = column_type_hint;
	CopyProperties(*copy);

	return std::move(copy);
}

} // namespace duckdb
