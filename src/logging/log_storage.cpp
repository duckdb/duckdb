#include "duckdb/logging/log_storage.hpp"
//
//#include "duckdb/common/types/column/column_data_collection.hpp"
//#include "duckdb/common/types/data_chunk.hpp"
//#include "duckdb/main/client_context.hpp"
//#include "duckdb/main/connection.hpp"
//#include "duckdb/main/database.hpp"
//#include "duckdb/main/table_description.hpp"
//
//#include <duckdb/common/file_opener.hpp>
//#include <duckdb/parallel/thread_context.hpp>

namespace duckdb {

InMemoryLogStorage::InMemoryLogStorage(shared_ptr<DatabaseInstance> &db_p) : entry_buffer(make_uniq<DataChunk>()), log_context_buffer(make_uniq<DataChunk>()) {
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

	max_buffer_size = 1;
	entry_buffer->Initialize(Allocator::DefaultAllocator(), log_entry_schema, max_buffer_size);
	log_context_buffer->Initialize(Allocator::DefaultAllocator(), log_context_schema, max_buffer_size);
	log_entries = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), log_entry_schema);
	log_contexts = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), log_context_schema);
}

InMemoryLogStorage::~InMemoryLogStorage() {
};

void InMemoryLogStorage::WriteLogEntry(timestamp_t timestamp, LogLevel level, const string& log_type, const string& log_message, const RegisteredLoggingContext& context) {
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

	entry_buffer->SetCardinality(size+1);

	if (size+1 >= max_buffer_size) {
		Flush();
	}
}

void InMemoryLogStorage::WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) {
	log_entries->Append(chunk);
}

void InMemoryLogStorage::Flush() {
	log_entries->Append(*entry_buffer);
	entry_buffer->Reset();

	log_contexts->Append(*log_context_buffer);
	log_context_buffer->Reset();
}

void InMemoryLogStorage::WriteLoggingContext(RegisteredLoggingContext &context) {
	auto size = log_context_buffer->size();

	auto context_id_data = FlatVector::GetData<idx_t>(log_context_buffer->data[0]);
	context_id_data[size] = context.context_id;

	auto context_scope_data = FlatVector::GetData<string_t>(log_context_buffer->data[1]);
	context_scope_data[size] = StringVector::AddString(log_context_buffer->data[1], EnumUtil::ToString(context.context.scope));

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
		Flush();
	}
}

bool InMemoryLogStorage::IsInternal() {
	return true;
}

ColumnDataCollection & InMemoryLogStorage::GetContexts() {
	return *log_contexts;
}

ColumnDataCollection &InMemoryLogStorage::GetEntries() {
	return *log_entries;
}

} // namespace duckdb
