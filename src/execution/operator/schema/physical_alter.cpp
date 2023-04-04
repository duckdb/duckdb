#include "duckdb/execution/operator/schema/physical_alter.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class AlterSourceState : public GlobalSourceState {
public:
	AlterSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalAlter::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<AlterSourceState>();
}

void PhysicalAlter::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                            LocalSourceState &lstate) const {
	auto &state = gstate.Cast<AlterSourceState>();
	if (state.finished) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
	catalog.Alter(context.client, info.get());
	state.finished = true;
}

} // namespace duckdb
