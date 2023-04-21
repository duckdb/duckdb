#include "duckdb/execution/operator/schema/physical_detach.hpp"
#include "duckdb/parser/parsed_data/detach_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class DetachSourceState : public GlobalSourceState {
public:
	DetachSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalDetach::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<DetachSourceState>();
}

SourceResultType PhysicalDetach::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<DetachSourceState>();
	if (state.finished) {
		return SourceResultType::FINISHED;
	}
	auto &db_manager = DatabaseManager::Get(context.client);
	db_manager.DetachDatabase(context.client, info->name, info->if_exists);
	state.finished = true;

	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
