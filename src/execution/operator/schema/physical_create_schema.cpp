#include "duckdb/execution/operator/schema/physical_create_schema.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateSchemaSourceState : public GlobalSourceState {
public:
	CreateSchemaSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateSchema::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<CreateSchemaSourceState>();
}

SourceResultType PhysicalCreateSchema::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<CreateSchemaSourceState>();
	if (state.finished) {
		return SourceResultType::FINISHED;
	}
	auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
	if (catalog.IsSystemCatalog()) {
		throw BinderException("Cannot create schema in system catalog");
	}
	catalog.CreateSchema(context.client, *info);
	state.finished = true;

	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
