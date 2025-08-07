#include "duckdb/parser/tableref/basetableref.hpp"

#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

string BaseTableRef::ToString() const {
	string result;
	result += catalog_name.empty() ? "" : (KeywordHelper::WriteOptionallyQuoted(catalog_name) + ".");
	result += schema_name.empty() ? "" : (KeywordHelper::WriteOptionallyQuoted(schema_name) + ".");
	result += KeywordHelper::WriteOptionallyQuoted(table_name);
	result += AliasToString(column_name_alias);
	if (at_clause) {
		result += " " + at_clause->ToString();
	}
	result += SampleToString();
	return result;
}

bool BaseTableRef::Equals(const TableRef &other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BaseTableRef>();
	return other.catalog_name == catalog_name && other.schema_name == schema_name && other.table_name == table_name &&
	       column_name_alias == other.column_name_alias && AtClause::Equals(at_clause.get(), other.at_clause.get());
}

unique_ptr<TableRef> BaseTableRef::Copy() {
	auto copy = make_uniq<BaseTableRef>();

	copy->catalog_name = catalog_name;
	copy->schema_name = schema_name;
	copy->table_name = table_name;
	copy->column_name_alias = column_name_alias;
	copy->at_clause = at_clause ? at_clause->Copy() : nullptr;
	CopyProperties(*copy);

	return std::move(copy);
}

} // namespace duckdb
