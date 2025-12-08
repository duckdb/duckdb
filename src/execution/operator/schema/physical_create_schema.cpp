#include "duckdb/execution/operator/schema/physical_create_schema.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception/binder_exception.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalCreateSchema::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
	if (catalog.IsSystemCatalog()) {
		throw BinderException("Cannot create schema in system catalog");
	}
	catalog.CreateSchema(context.client, *info);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
