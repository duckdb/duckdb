#include "duckdb/execution/operator/persistent/physical_copy_database.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

PhysicalCopyDatabase::PhysicalCopyDatabase(vector<LogicalType> types, idx_t estimated_cardinality, unique_ptr<CopyDatabaseInfo> info_p)
	: PhysicalOperator(PhysicalOperatorType::COPY_DATABASE, std::move(types), estimated_cardinality), info(std::move(info_p)) {
}

PhysicalCopyDatabase::~PhysicalCopyDatabase() {}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalCopyDatabase::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	auto &catalog = info->to_database;
	for(auto &schema : info->schemas) {
		catalog.CreateSchema(context.client, *schema);
	}
	for(auto &table : info->tables) {
		// bind the constraints to the table again
		auto binder = Binder::CreateBinder(context.client);
		auto bound_info = binder->BindCreateTableInfo(std::move(table));
		catalog.CreateTable(context.client, *bound_info);
	}


	return SourceResultType::FINISHED;
}


} // namespace duckdb
