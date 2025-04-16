#include "duckdb/execution/operator/schema/physical_create_matview.hpp"

namespace duckdb {

PhysicalCreateMatView::PhysicalCreateMatView(LogicalOperator &op, SchemaCatalogEntry &schema,
                                             unique_ptr<BoundCreateTableInfo> info, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_MATVIEW, op.types, estimated_cardinality), schema(schema),
      info(std::move(info)) {
}

SourceResultType PhysicalCreateMatView::GetData(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &catalog = schema.catalog;
	catalog.CreateTable(catalog.GetCatalogTransaction(context.client), schema, *info);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
