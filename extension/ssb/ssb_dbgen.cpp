#include "ssb_dbgen.hpp"

#include "duckdb.hpp"
#include "ssb_tables.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#ifndef DUCKDB_NO_THREADS
#include "duckdb/common/thread.hpp"
#endif
#endif

extern "C" {
#include "ssbgen/include/driver.h"
}

namespace ssb {

template <class T>
static void CreateSSBTable(duckdb::ClientContext &context, std::string catalog_name, std::string schema) {

	auto info = duckdb::make_uniq<duckdb::CreateTableInfo>();
	info->catalog = catalog_name;
	info->schema = schema;
	info->table = T::Name;
	info->on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
	info->temporary = false;
	for (idx_t i = 0; i < T::ColumnCount; i++) {
		info->columns.AddColumn(duckdb::ColumnDefinition(T::Columns[i], T::Types[i]));
		info->constraints.push_back(duckdb::make_uniq<duckdb::NotNullConstraint>(duckdb::LogicalIndex(i)));
	}
	auto &catalog = duckdb::Catalog::GetCatalog(context, catalog_name);
	catalog.CreateTable(context, std::move(info));
}

void SSBGenWrapper::CreateSSBSchema(duckdb::ClientContext &context, std::string catalog, std::string schema) {
	CreateSSBTable<LineOrderInfo>(context, catalog, schema);
	CreateSSBTable<DateInfo>(context, catalog, schema);
	CreateSSBTable<SupplierInfo>(context, catalog, schema);
	CreateSSBTable<CustomerInfo>(context, catalog, schema);
	CreateSSBTable<PartInfo>(context, catalog, schema);
}
} // namespace ssb