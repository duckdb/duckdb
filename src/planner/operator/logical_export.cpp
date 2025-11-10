#include "duckdb/planner/operator/logical_export.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"

namespace duckdb {

LogicalExport::LogicalExport(CopyFunction function, unique_ptr<CopyInfo> copy_info,
                             unique_ptr<BoundExportData> exported_tables)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EXPORT), copy_info(std::move(copy_info)),
      function(std::move(function)), exported_tables(std::move(exported_tables)) {
}

LogicalExport::LogicalExport(ClientContext &context, unique_ptr<ParseInfo> copy_info_p,
                             unique_ptr<ParseInfo> exported_tables_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EXPORT),
      copy_info(unique_ptr_cast<ParseInfo, CopyInfo>(std::move(copy_info_p))),
      function(GetCopyFunction(context, *copy_info)),
      exported_tables(unique_ptr_cast<ParseInfo, BoundExportData>(std::move(exported_tables_p))) {
}

CopyFunction LogicalExport::GetCopyFunction(ClientContext &context, CopyInfo &info) {
	auto &copy_entry =
	    Catalog::GetEntry<CopyFunctionCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, info.format);
	return copy_entry.function;
}

} // namespace duckdb
