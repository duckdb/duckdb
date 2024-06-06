#include "duckdb/execution/operator/persistent/physical_copy_database.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

namespace duckdb {

PhysicalCopyDatabase::PhysicalCopyDatabase(vector<LogicalType> types, idx_t estimated_cardinality,
                                           unique_ptr<CopyDatabaseInfo> info_p)
    : PhysicalOperator(PhysicalOperatorType::COPY_DATABASE, std::move(types), estimated_cardinality),
      info(std::move(info_p)) {
}

PhysicalCopyDatabase::~PhysicalCopyDatabase() {
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalCopyDatabase::GetData(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	auto &catalog = Catalog::GetCatalog(context.client, info->target_database);
	for (auto &create_info : info->entries) {
		switch (create_info->type) {
		case CatalogType::SCHEMA_ENTRY:
			catalog.CreateSchema(context.client, create_info->Cast<CreateSchemaInfo>());
			break;
		case CatalogType::VIEW_ENTRY:
			catalog.CreateView(context.client, create_info->Cast<CreateViewInfo>());
			break;
		case CatalogType::SEQUENCE_ENTRY:
			catalog.CreateSequence(context.client, create_info->Cast<CreateSequenceInfo>());
			break;
		case CatalogType::TYPE_ENTRY:
			catalog.CreateType(context.client, create_info->Cast<CreateTypeInfo>());
			break;
		case CatalogType::MACRO_ENTRY:
			catalog.CreateFunction(context.client, create_info->Cast<CreateMacroInfo>());
			break;
		case CatalogType::TABLE_ENTRY: {
			auto binder = Binder::CreateBinder(context.client);
			auto bound_info = binder->BindCreateTableInfo(std::move(create_info));
			catalog.CreateTable(context.client, *bound_info);
			break;
		}
		case CatalogType::INDEX_ENTRY:
		default:
			throw NotImplementedException("Entry type %s not supported in PhysicalCopyDatabase",
			                              CatalogTypeToString(create_info->type));
		}
	}
	return SourceResultType::FINISHED;
}

} // namespace duckdb
