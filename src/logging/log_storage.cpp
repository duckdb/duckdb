#include "duckdb/logging/log_storage.hpp"
#include "duckdb/logging/logging.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

#include <iostream>

namespace duckdb {

unique_ptr<LogStorageScanState> LogStorage::CreateScanEntriesState() const {
	throw NotImplementedException("Not implemented for this LogStorage: CreateScanEntriesState");
}
bool LogStorage::ScanEntries(LogStorageScanState &state, DataChunk &result) const {
	throw NotImplementedException("Not implemented for this LogStorage: ScanEntries");
}
void LogStorage::InitializeScanEntries(LogStorageScanState &state) const {
	throw NotImplementedException("Not implemented for this LogStorage: InitializeScanEntries");
}
unique_ptr<LogStorageScanState> LogStorage::CreateScanContextsState() const {
	throw NotImplementedException("Not implemented for this LogStorage: CreateScanContextsState");
}
bool LogStorage::ScanContexts(LogStorageScanState &state, DataChunk &result) const {
	throw NotImplementedException("Not implemented for this LogStorage: ScanContexts");
}
void LogStorage::InitializeScanContexts(LogStorageScanState &state) const {
	throw NotImplementedException("Not implemented for this LogStorage: InitializeScanContexts");
}

StdOutLogStorage::StdOutLogStorage() {
}

StdOutLogStorage::~StdOutLogStorage() {
}

void StdOutLogStorage::WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type,
                                     const string &log_message, const RegisteredLoggingContext &context) {
	std::cout << StringUtil::Format(
	    "[LOG] %s, %s, %s, %s, %s, %s, %s, %s\n", Value::TIMESTAMP(timestamp).ToString(), log_type,
	    EnumUtil::ToString(level), log_message, EnumUtil::ToString(context.context.scope),
	    context.context.client_context.IsValid() ? to_string(context.context.client_context.GetIndex()) : "NULL",
	    context.context.transaction_id.IsValid() ? to_string(context.context.transaction_id.GetIndex()) : "NULL",
	    context.context.thread.IsValid() ? to_string(context.context.thread.GetIndex()) : "NULL");
}

void StdOutLogStorage::WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) {
	throw NotImplementedException("StdOutLogStorage::WriteLogEntries");
}

void StdOutLogStorage::Flush() {
	// NOP
}

InMemoryLogStorageScanState::InMemoryLogStorageScanState() {
}
InMemoryLogStorageScanState::~InMemoryLogStorageScanState() {
}

InMemoryLogStorage::InMemoryLogStorage(DatabaseInstance &db_p)
    : entry_buffer(make_uniq<DataChunk>()), log_context_buffer(make_uniq<DataChunk>()) {
	// LogEntry Schema
	vector<LogicalType> log_entry_schema = {
	    LogicalType::UBIGINT,   // context_id
	    LogicalType::TIMESTAMP, // timestamp
	    LogicalType::VARCHAR,   // log_type TODO: const vector where possible?
	    LogicalType::VARCHAR,   // level TODO: enumify
	    LogicalType::VARCHAR,   // message
	};

	// LogContext Schema
	vector<LogicalType> log_context_schema = {
	    LogicalType::UBIGINT, // context_id
	    LogicalType::VARCHAR, // scope TODO: enumify
	    LogicalType::UBIGINT, // client_context
	    LogicalType::UBIGINT, // transaction_id
	    LogicalType::UBIGINT, // thread
	};

	max_buffer_size = STANDARD_VECTOR_SIZE;
	entry_buffer->Initialize(Allocator::DefaultAllocator(), log_entry_schema, max_buffer_size);
	log_context_buffer->Initialize(Allocator::DefaultAllocator(), log_context_schema, max_buffer_size);
	log_entries = make_uniq<ColumnDataCollection>(db_p.GetBufferManager(), log_entry_schema);
	log_contexts = make_uniq<ColumnDataCollection>(db_p.GetBufferManager(), log_context_schema);
}

InMemoryLogStorage::~InMemoryLogStorage() {
}

void InMemoryLogStorage::WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type,
                                       const string &log_message, const RegisteredLoggingContext &context) {
	unique_lock<mutex> lck(lock);

	if (registered_contexts.find(context.context_id) == registered_contexts.end()) {
		WriteLoggingContext(context);
	}

	auto size = entry_buffer->size();
	auto context_id_data = FlatVector::GetData<idx_t>(entry_buffer->data[0]);
	auto timestamp_data = FlatVector::GetData<timestamp_t>(entry_buffer->data[1]);
	auto type_data = FlatVector::GetData<string_t>(entry_buffer->data[2]);
	auto level_data = FlatVector::GetData<string_t>(entry_buffer->data[3]);
	auto message_data = FlatVector::GetData<string_t>(entry_buffer->data[4]);

	context_id_data[size] = context.context_id;
	timestamp_data[size] = timestamp;
	type_data[size] = StringVector::AddString(entry_buffer->data[2], log_type);
	level_data[size] = StringVector::AddString(entry_buffer->data[3], EnumUtil::ToString(level));
	message_data[size] = StringVector::AddString(entry_buffer->data[4], log_message);

	entry_buffer->SetCardinality(size + 1);

	if (size + 1 >= max_buffer_size) {
		FlushInternal();
	}
}

void InMemoryLogStorage::WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) {
	log_entries->Append(chunk);
}

void InMemoryLogStorage::Flush() {
	unique_lock<mutex> lck(lock);
	FlushInternal();
}

void InMemoryLogStorage::FlushInternal() {
	if (entry_buffer->size() > 0) {
		log_entries->Append(*entry_buffer);
		entry_buffer->Reset();
	}

	if (log_context_buffer->size() > 0) {
		log_contexts->Append(*log_context_buffer);
		log_context_buffer->Reset();
	}
}

void InMemoryLogStorage::WriteLoggingContext(const RegisteredLoggingContext &context) {
	registered_contexts.insert(context.context_id);

	auto size = log_context_buffer->size();

	auto context_id_data = FlatVector::GetData<idx_t>(log_context_buffer->data[0]);
	context_id_data[size] = context.context_id;

	auto context_scope_data = FlatVector::GetData<string_t>(log_context_buffer->data[1]);
	context_scope_data[size] =
	    StringVector::AddString(log_context_buffer->data[1], EnumUtil::ToString(context.context.scope));

	if (context.context.client_context.IsValid()) {
		auto client_context_data = FlatVector::GetData<idx_t>(log_context_buffer->data[2]);
		client_context_data[size] = context.context.client_context.GetIndex();
	} else {
		FlatVector::Validity(log_context_buffer->data[2]).SetInvalid(size);
	}
	if (context.context.transaction_id.IsValid()) {
		auto client_context_data = FlatVector::GetData<idx_t>(log_context_buffer->data[3]);
		client_context_data[size] = context.context.transaction_id.GetIndex();
	} else {
		FlatVector::Validity(log_context_buffer->data[3]).SetInvalid(size);
	}
	if (context.context.thread.IsValid()) {
		auto thread_data = FlatVector::GetData<idx_t>(log_context_buffer->data[4]);
		thread_data[size] = context.context.thread.GetIndex();
	} else {
		FlatVector::Validity(log_context_buffer->data[4]).SetInvalid(size);
	}

	log_context_buffer->SetCardinality(size + 1);

	if (size + 1 >= max_buffer_size) {
		FlushInternal();
	}
}

bool InMemoryLogStorage::CanScan() {
	return true;
}

unique_ptr<LogStorageScanState> InMemoryLogStorage::CreateScanEntriesState() const {
	return make_uniq<InMemoryLogStorageScanState>();
}
bool InMemoryLogStorage::ScanEntries(LogStorageScanState &state, DataChunk &result) const {
	unique_lock<mutex> lck(lock);
	auto &in_mem_scan_state = state.Cast<InMemoryLogStorageScanState>();
	return log_entries->Scan(in_mem_scan_state.scan_state, result);
}

void InMemoryLogStorage::InitializeScanEntries(LogStorageScanState &state) const {
	unique_lock<mutex> lck(lock);
	auto &in_mem_scan_state = state.Cast<InMemoryLogStorageScanState>();
	log_entries->InitializeScan(in_mem_scan_state.scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
}

unique_ptr<LogStorageScanState> InMemoryLogStorage::CreateScanContextsState() const {
	return make_uniq<InMemoryLogStorageScanState>();
}
bool InMemoryLogStorage::ScanContexts(LogStorageScanState &state, DataChunk &result) const {
	unique_lock<mutex> lck(lock);
	auto &in_mem_scan_state = state.Cast<InMemoryLogStorageScanState>();
	return log_contexts->Scan(in_mem_scan_state.scan_state, result);
}

void InMemoryLogStorage::InitializeScanContexts(LogStorageScanState &state) const {
	unique_lock<mutex> lck(lock);
	auto &in_mem_scan_state = state.Cast<InMemoryLogStorageScanState>();
	log_contexts->InitializeScan(in_mem_scan_state.scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
}

} // namespace duckdb
