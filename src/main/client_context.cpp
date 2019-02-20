#include "main/client_context.hpp"

#include "main/database.hpp"

using namespace duckdb;
using namespace std;

ClientContext::ClientContext(DuckDB &database)
    : db(database), transaction(database.transaction_manager), prepared_statements(make_unique<CatalogSet>()) {
}

// TODO should we put this into execution context?
bool ClientContext::CleanupLazyResult() {
	execution_context.physical_plan = nullptr;

	if (transaction.HasActiveTransaction()) {
		ActiveTransaction().active_query = MAXIMUM_QUERY_ID;
		try {
			if (transaction.IsAutoCommit()) {
				if (execution_context.internal_result.GetSuccess()) {
					transaction.Commit();
				} else {
					transaction.Rollback();
				}
			}
		} catch (Exception &ex) {
			execution_context.internal_result.success = false;
		} catch (...) {
			execution_context.internal_result.success = false;
			execution_context.internal_result.error = "UNHANDLED EXCEPTION TYPE THROWN IN TRANSACTION COMMIT!";
		}
	}
	return execution_context.internal_result.success;
}

unique_ptr<DataChunk> ClientContext::FetchChunk() {
	auto chunk = make_unique<DataChunk>();

	if (execution_context.internal_result.success && !execution_context.physical_plan) {
		return chunk;
	}
	if (execution_context.first_chunk) {
		return move(execution_context.first_chunk);
	}
	execution_context.physical_plan->InitializeChunk(*chunk.get());
	execution_context.physical_plan->GetChunk(*this, *chunk.get(), execution_context.physical_state.get());
	return chunk;
}
