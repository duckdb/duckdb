#include "duckdb/parser/parsed_data/refresh_materialized_view_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {
RefreshMaterializedViewInfo::RefreshMaterializedViewInfo(string catalog, string schema, string name)
    : CreateTableInfo(std::move(catalog), std::move(schema), std::move(name)) {
}

unique_ptr<CreateInfo> RefreshMaterializedViewInfo::Copy() const {
	auto result = make_uniq<RefreshMaterializedViewInfo>(catalog, schema, table);
	CopyProperties(*result);
	result->columns = columns.Copy();
	for (auto &constraint : constraints) {
		result->constraints.push_back(constraint->Copy());
	}
	if (query) {
		result->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	}
	return std::move(result);
}

string RefreshMaterializedViewInfo::ToString() const {
	string ret = "REFRESH MATERIALIZED VIEW ";
	ret += QualifierToString(temporary ? "" : catalog, schema, table);
	return ret;
}

} // namespace duckdb
