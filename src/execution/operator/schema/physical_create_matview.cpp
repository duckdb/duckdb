#include "duckdb/execution/operator/schema/physical_create_matview.hpp"

#include "duckdb/catalog/catalog_entry/matview_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/schema/physical_create_table_as.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

PhysicalCreateMatView::PhysicalCreateMatView(LogicalOperator &op, SchemaCatalogEntry *schema,
                                             unique_ptr<BoundCreateTableInfo> info, idx_t estimated_cardinality)
    : PhysicalCreateTableAs(op, schema, move(info), estimated_cardinality) {
	this->type = PhysicalOperatorType::CREATE_MATVIEW;
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CreateMatViewGlobalState : public CreateTableAsGlobalState {};

unique_ptr<GlobalSinkState> PhysicalCreateMatView::GetGlobalSinkState(ClientContext &context) const {
	auto sink = make_unique<CreateMatViewGlobalState>();
	auto &catalog = Catalog::GetCatalog(context);
	sink->table = (MatViewCatalogEntry *)catalog.CreateMatView(context, schema, info.get());
	return move(sink);
}

void PhysicalCreateMatView::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                    LocalSourceState &lstate) const {
	PhysicalCreateTableAs::GetData(context, chunk, gstate, lstate);
}

} // namespace duckdb
