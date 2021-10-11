#include "duckdb/execution/operator/schema/physical_create_type.hpp"

#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

PhysicalCreateType::PhysicalCreateType(unique_ptr<CreateTypeInfo> info, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_TYPE, {LogicalType::BIGINT}, estimated_cardinality),
      info(move(info)) {
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateTypeSourceState : public GlobalSourceState {
public:
	CreateTypeSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateType::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreateTypeSourceState>();
}

void PhysicalCreateType::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                 LocalSourceState &lstate) const {
	auto &state = (CreateTypeSourceState &)gstate;
	if (state.finished) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(context.client);
	catalog.CreateType(context.client, info.get());
	state.finished = true;
}

} // namespace duckdb
