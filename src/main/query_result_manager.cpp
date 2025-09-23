#include "duckdb/main/query_result_manager.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

ManagedQueryResult::ManagedQueryResult(weak_ptr<DatabaseInstance> db_p, unique_ptr<ColumnDataCollection> collection_p)
    : db(std::move(db_p)), collection(std::move(collection_p)) {
}

ManagedQueryResult::~ManagedQueryResult() {
	auto db_ref = db.lock();
	if (db_ref) {
		db_ref->GetQueryResultManager().Remove(*this);
	}
}

void ManagedQueryResult::ValidateResult() {
	if (!collection) {
		throw ConnectionException("Trying to access a query result after the database instance has been closed");
	}
}

ColumnDataCollection &ManagedQueryResult::Collection() {
	ValidateResult();
	return *collection;
}

ColumnDataScanState &ManagedQueryResult::ScanState() {
	ValidateResult();
	return scan_state;
}

QueryResultManager::QueryResultManager(DatabaseInstance &db_p) : db(db_p.shared_from_this()) {
}

QueryResultManager::~QueryResultManager() {
	for (auto &open_result : open_results) {
		auto open_result_ref = open_result.second.lock();
		if (open_result_ref) {
			open_result_ref->scan_state.current_chunk_state.handles.clear();
			open_result_ref->collection.reset();
		}
	}
}

QueryResultManager &QueryResultManager::Get(ClientContext &context) {
	return context.db->GetQueryResultManager();
}

shared_ptr<ManagedQueryResult> QueryResultManager::Add(unique_ptr<ColumnDataCollection> collection) {
	D_ASSERT(collection->GetAllocatorType() == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);
	auto result = shared_ptr<ManagedQueryResult>(new ManagedQueryResult(db, std::move(collection)));
	lock_guard<mutex> guard(lock);
	open_results[*result] = weak_ptr<ManagedQueryResult>(result);
	return result;
}

void QueryResultManager::Remove(ManagedQueryResult &query_result) {
	lock_guard<mutex> guard(lock);
	open_results.erase(query_result);
}

} // namespace duckdb
