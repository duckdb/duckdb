#include "duckdb/execution/operator/schema/physical_create_custom_type.hpp"

#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

PhysicalCreateCustomType::PhysicalCreateCustomType(unique_ptr<CreateCustomTypeInfo> info, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_CUSTOM_TYPE, {LogicalType::BIGINT}, estimated_cardinality),
      info(move(info)) {
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateCustomTypeSourceState : public GlobalSourceState {
public:
	CreateCustomTypeSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateCustomType::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreateCustomTypeSourceState>();
}

void PhysicalCreateCustomType::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                 LocalSourceState &lstate) const {
	auto &state = (CreateCustomTypeSourceState &)gstate;
	if (state.finished) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(context.client);
	// auto internal_type = CustomType::GetInternalType(info->type);
	catalog.CreateCustomType(context.client, info.get());
	state.finished = true;
}

} // namespace duckdb
