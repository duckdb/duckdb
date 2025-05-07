#include "duckdb/parser/statement/refresh_materialized_view_statement.hpp"

#include <duckdb/catalog/catalog_entry/materialized_view_catalog_entry.hpp>

namespace duckdb {

RefreshMaterializedViewStatement::RefreshMaterializedViewStatement()
    : SQLStatement(StatementType::REFRESH_MATERIALIZED_VIEW_STATEMENT) {
}

RefreshMaterializedViewStatement::RefreshMaterializedViewStatement(const RefreshMaterializedViewStatement &other)
    : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> RefreshMaterializedViewStatement::Copy() const {
	return unique_ptr<RefreshMaterializedViewStatement>(new RefreshMaterializedViewStatement(*this));
}

string RefreshMaterializedViewStatement::ToString() const {
	return info->ToString();
}

} // namespace duckdb
