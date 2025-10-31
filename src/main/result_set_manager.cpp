#include "duckdb/main/result_set_manager.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

ManagedResultSet::ManagedResultSet(weak_ptr<DatabaseInstance> db_p, QueryResultMemoryType memory_type_p,
                                   unique_ptr<ColumnDataCollection> collection_p)
    : db(std::move(db_p)), memory_type(memory_type_p), collection(std::move(collection_p)) {
}

ManagedResultSet::~ManagedResultSet() {
	scan_state.current_chunk_state.handles.clear();
	auto db_ref = db.lock();
	if (db_ref) {
		db_ref->GetResultSetManager().Remove(*this);
	}
}

unique_ptr<PinnedResultSet> ManagedResultSet::Pin() {
	if (memory_type == QueryResultMemoryType::IN_MEMORY) {
		return unique_ptr<PinnedResultSet>(new PinnedResultSet(nullptr, shared_from_this(), *collection, scan_state));
	}
	D_ASSERT(memory_type == QueryResultMemoryType::BUFFER_MANAGED);
	auto db_ref = db.lock();
	if (!db_ref) {
		throw ConnectionException("Trying to access a query result after the database instance has been closed");
	}
	return unique_ptr<PinnedResultSet>(
	    new PinnedResultSet(std::move(db_ref), shared_from_this(), *collection, scan_state));
}

QueryResultMemoryType ManagedResultSet::GetMemoryType() const {
	return memory_type;
}

ResultSetManager::ResultSetManager(DatabaseInstance &db_p) : db(db_p.shared_from_this()) {
}

ResultSetManager::~ResultSetManager() {
	for (auto &open_result : open_results) {
		auto open_result_ref = open_result.second.lock();
		if (open_result_ref && open_result_ref->memory_type == QueryResultMemoryType::BUFFER_MANAGED) {
			open_result_ref->scan_state.current_chunk_state.handles.clear();
			open_result_ref->collection.reset();
		}
	}
}

ResultSetManager &ResultSetManager::Get(ClientContext &context) {
	return context.db->GetResultSetManager();
}

shared_ptr<ManagedResultSet> ResultSetManager::Add(unique_ptr<ColumnDataCollection> collection,
                                                   QueryResultMemoryType type) {
	D_ASSERT((type == QueryResultMemoryType::IN_MEMORY &&
	          collection->GetAllocatorType() == ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR) ||
	         (type == QueryResultMemoryType::BUFFER_MANAGED &&
	          collection->GetAllocatorType() == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR &&
	          RefersToSameObject(collection->GetBufferManager(), BufferManager::GetBufferManager(*db.lock()))));
	auto result = shared_ptr<ManagedResultSet>(new ManagedResultSet(db, type, std::move(collection)));
	lock_guard<mutex> guard(lock);
	open_results[*result] = weak_ptr<ManagedResultSet>(result);
	return result;
}

void ResultSetManager::Remove(ManagedResultSet &query_result) {
	lock_guard<mutex> guard(lock);
	open_results.erase(query_result);
}

} // namespace duckdb
