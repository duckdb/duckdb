#include "duckdb/logging/log_storage.hpp"

#include "duckdb/common/csv_writer.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/main/database_file_opener.hpp"
#include "duckdb/logging/logging.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"

#include <complex>
#include <iostream>

namespace duckdb {

vector<LogicalType> LogStorage::GetSchema(LoggingTargetTable table) {
	switch (table) {
	case LoggingTargetTable::ALL_LOGS:
		return {
			LogicalType::UBIGINT,   // context_id
			LogicalType::VARCHAR,   // scope
			LogicalType::UBIGINT,   // connection_id
			LogicalType::UBIGINT,   // transaction_id
			LogicalType::UBIGINT,   // query_id
			LogicalType::UBIGINT,   // thread
			LogicalType::TIMESTAMP, // timestamp
			LogicalType::VARCHAR,   // log_type
			LogicalType::VARCHAR,   // level
			LogicalType::VARCHAR,   // message
			};
	case LoggingTargetTable::LOG_ENTRIES:
		return {
		LogicalType::UBIGINT,   // context_id
		LogicalType::TIMESTAMP, // timestamp
		LogicalType::VARCHAR,   // log_type
		LogicalType::VARCHAR,   // level
		LogicalType::VARCHAR,   // message
		};
	case LoggingTargetTable::LOG_CONTEXTS:
		return {
			LogicalType::UBIGINT,   // context_id
			LogicalType::VARCHAR,   // scope
			LogicalType::UBIGINT,   // connection_id
			LogicalType::UBIGINT,   // transaction_id
			LogicalType::UBIGINT,   // query_id
			LogicalType::UBIGINT,   // thread
		};
	default:
		throw NotImplementedException("Unknown logging target table");
	}
}

vector<string> LogStorage::GetColumnNames(LoggingTargetTable table) {
	switch (table) {
	case LoggingTargetTable::ALL_LOGS:
		return {
			"context_id",
			"scope",
			"connection_id",
			"transaction_id",
			"query_id",
			"thread_id",
			"timestamp",
			"type",
			"log_level",
			"message",
		};
	case LoggingTargetTable::LOG_ENTRIES:
		return {
			"context_id",
			"timestamp",
			"type",
			"log_level",
			"message"};
	case LoggingTargetTable::LOG_CONTEXTS:
		return {
			"context_id",
			"scope",
			"connection_id",
			"transaction_id",
			"query_id",
			"thread_id",
		};
	default:
		throw NotImplementedException("Unknown logging target table");
	}
}

unique_ptr<LogStorageScanState> LogStorage::CreateScanState(LoggingTargetTable table) const {
	throw NotImplementedException("Not implemented for this LogStorage: CreateScanEntriesState");
}
bool LogStorage::Scan(LogStorageScanState &state, DataChunk &result) const {
	throw NotImplementedException("Not implemented for this LogStorage: ScanEntries");
}
void LogStorage::InitializeScan(LogStorageScanState &state) const {
	throw NotImplementedException("Not implemented for this LogStorage: InitializeScanEntries");
}
void LogStorage::Truncate() {
	throw NotImplementedException("Not implemented for this LogStorage: TruncateLogStorage");
}

void LogStorage::UpdateConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config) {
	if (config.size() > 1) {
		throw InvalidInputException("LogStorage does not support passing configuration");
	}
}

unique_ptr<TableRef> LogStorage::BindReplace(ClientContext &context, TableFunctionBindInput &input, LoggingTargetTable table) {
	return nullptr;
}

CSVLogStorage::~CSVLogStorage() {
}

CSVLogStorage::CSVLogStorage(DatabaseInstance &db, bool normalize)
: BufferingLogStorage(db, STANDARD_VECTOR_SIZE, normalize) {
	ResetCastChunk();
}

void BufferingLogStorage::UpdateConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config) {
	lock_guard<mutex> lck(lock);
	return UpdateConfigInternal(db, config);
}

bool BufferingLogStorage::IsEnabled(LoggingTargetTable table) {
	lock_guard<mutex> lck(lock);
	return IsEnabledInternal(table);
}

bool BufferingLogStorage::IsEnabledInternal(LoggingTargetTable table) {
	if (normalize_contexts) {
		return table == LoggingTargetTable::LOG_CONTEXTS || table == LoggingTargetTable::LOG_ENTRIES;
	}

	return  table == LoggingTargetTable::ALL_LOGS;
}

void CSVLogStorage::ExecuteCast(LoggingTargetTable table, DataChunk &chunk) {
 	// Reset the cast buffer before use
	cast_buffers[table]->Reset();

	bool success = true;

	CastParameters cast_params;

	auto &cast_buffer = *cast_buffers[table];

	idx_t count = chunk.size();

	if (table == LoggingTargetTable::LOG_ENTRIES) {
		// context_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(chunk.data[0], cast_buffer.data[0], count, cast_params);
		// timestamp: LogicalType::TIMESTAMP
		success &= VectorCastHelpers::StringCast<timestamp_t, duckdb::StringCast>(chunk.data[1], cast_buffer.data[1], count, cast_params);
		// log_type: LogicalType::VARCHAR  (no cast)
		cast_buffer.data[2].Reference(chunk.data[2]);
		// level: LogicalType::VARCHAR  (no cast)
		cast_buffer.data[3].Reference(chunk.data[3]);
		// message: LogicalType::VARCHAR  (no cast)
		cast_buffer.data[4].Reference(chunk.data[4]);
	} else if (table == LoggingTargetTable::LOG_CONTEXTS) {
		// -- Cast Log Contexts
		// context_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(chunk.data[0], cast_buffer.data[0], count, cast_params);
		// scope: LogicalType::VARCHAR (no cast)
		cast_buffer.data[1].Reference(chunk.data[1]);
		// connection_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(chunk.data[2], cast_buffer.data[2], count, cast_params);
		// transaction_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(chunk.data[3], cast_buffer.data[3], count, cast_params);
		// query_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(chunk.data[4], cast_buffer.data[4], count, cast_params);
		// thread: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(chunk.data[5], cast_buffer.data[5], count, cast_params);
	} else if (table == LoggingTargetTable::ALL_LOGS) {
		// context_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(chunk.data[0], cast_buffer.data[0], count, cast_params);
		// scope: LogicalType::VARCHAR (no cast)
		cast_buffer.data[1].Reference(chunk.data[1]);
		// connection_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(chunk.data[2], cast_buffer.data[2], count, cast_params);
		// transaction_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(chunk.data[3], cast_buffer.data[3], count, cast_params);
		// query_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(chunk.data[4], cast_buffer.data[4], count, cast_params);
		// thread: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(chunk.data[5], cast_buffer.data[5], count, cast_params);
		// timestamp: LogicalType::TIMESTAMP
		success &= VectorCastHelpers::StringCast<timestamp_t, duckdb::StringCast>(chunk.data[6], cast_buffer.data[6], count, cast_params);
		// log_type: LogicalType::VARCHAR  (no cast)
		cast_buffer.data[7].Reference(chunk.data[7]);
		// level: LogicalType::VARCHAR  (no cast)
		cast_buffer.data[8].Reference(chunk.data[8]);
		// message: LogicalType::VARCHAR  (no cast)
		cast_buffer.data[9].Reference(chunk.data[9]);
	}

	cast_buffer.SetCardinality(count);

	if (!success) {
		throw InvalidInputException("Failed to cast log entries");
	}
}

void CSVLogStorage::ResetAllBuffers() {
	BufferingLogStorage::ResetAllBuffers();
	ResetCastChunk();
}


void CSVLogStorage::InitializeCastChunk(LoggingTargetTable table) {
	cast_buffers[table] = make_uniq<DataChunk>();

	vector<LogicalType> types;
	types.resize(GetSchema(table).size(), LogicalType::VARCHAR);

	cast_buffers[table]->Initialize(Allocator::DefaultAllocator(), types);
}
void CSVLogStorage::ResetCastChunk() {
	InitializeCastChunk(LoggingTargetTable::LOG_ENTRIES);
	InitializeCastChunk(LoggingTargetTable::LOG_CONTEXTS);
	InitializeCastChunk(LoggingTargetTable::ALL_LOGS);
}

void CSVLogStorage::SetWriterConfigs(CSVWriter &writer, vector<string> column_names) {
	writer.options.dialect_options.state_machine_options.escape = '\"';
	writer.options.dialect_options.state_machine_options.quote = '\"';
	writer.options.dialect_options.state_machine_options.delimiter = CSVOption<string>("\t");
	writer.options.name_list = column_names;

	writer.options.force_quote = vector<bool>(column_names.size(), false);
}

void CSVLogStorage::FlushChunk(LoggingTargetTable table, DataChunk &chunk) {
	BeforeFlush(table, chunk);

	// Execute the cast
	ExecuteCast(table, chunk);

	// Write the chunk to the CSVWriter
	writers[table]->WriteChunk(*cast_buffers[table]);
	writers[table]->Flush();

	// Call child class to implement any post flushing behaviour (e.g. calling sync or writing out buffer to stdout)
	OnFlush(table, chunk);

	// Reset the cast buffer
	cast_buffers[table]->Reset();
}

void BufferingLogStorage::UpdateConfigInternal(DatabaseInstance &db, case_insensitive_map_t<Value> &config) {
	for (const auto &it : config) {
		if (StringUtil::Lower(it.first) == "buffer_size") {
			buffer_limit = it.second.GetValue<uint64_t>();
			ResetAllBuffers();
		} else if (StringUtil::Lower(it.first) == "only_flush_on_full_buffer") {
			// This is a debug option used during testing. It disables the manual
			only_flush_on_full_buffer = it.second.GetValue<bool>();
		} else if (StringUtil::Lower(it.first) == "normalize") {
			throw InternalException("'normalize' setting should be handled in child class");
		} else {
			throw InvalidInputException("Unrecognized log storage config option: '%s'", it.first);
		}
	}
}

StdOutLogStorage::StdOutLogStorage(DatabaseInstance &db) : CSVLogStorage(db, false) {
	// StdOutLogStorage is denormalized only
	auto target_table = LoggingTargetTable::ALL_LOGS;

	// Initialize writer and streams
	stdout_stream = make_uniq<MemoryStream>();
	writers[target_table] = make_uniq<CSVWriter>(*stdout_stream, GetColumnNames(target_table), false);
	SetWriterConfigs(*writers[target_table], GetColumnNames(target_table));
	writers[target_table]->writer_options.newline_writing_mode = CSVNewLineMode::WRITE_AFTER;
}

StdOutLogStorage::~StdOutLogStorage() {
}

void StdOutLogStorage::OnFlush(LoggingTargetTable table, DataChunk &chunk) {
	D_ASSERT(table == LoggingTargetTable::ALL_LOGS);
	auto &stream = stdout_stream;
	std::cout.write(const_char_ptr_cast(stream->GetData()),
	                NumericCast<int64_t>(stream->GetPosition()));
	std::cout.flush();
	stream->Rewind();
}

FileLogStorage::FileLogStorage(DatabaseInstance &db_p)
    : CSVLogStorage(db_p, true), db(db_p) {
}

FileLogStorage::~FileLogStorage() {
}

void FileLogStorage::InitializeFile(DatabaseInstance &db, LoggingTargetTable table) {
	//! Create file writer
	file_writers[table] = InitializeFileWriter(db, file_paths[table]);
	auto file_writer = file_writers[table].get();

	//! Create CSV writer that writes to file
	auto column_names = GetColumnNames(table);
	writers[table] = make_uniq<CSVWriter>(*file_writer, column_names, false);
	auto csv_writer = writers[table].get();

	SetWriterConfigs(*csv_writer, column_names);

	bool should_write_header = file_writer->handle->GetFileSize() == 0;

	// We write the header only if the file was empty: when appending to the file we don't
	csv_writer->options.dialect_options.header = {should_write_header, true};

	// Initialize the writer, this writes out the header if required
	csv_writer->Initialize();

	// Needed to ensure we correctly start with a newline
	csv_writer->SetWrittenAnything(true);

	// Ensures that the file is fully initialized when this function returns
	file_writer->Sync();
}

unique_ptr<BufferedFileWriter> FileLogStorage::InitializeFileWriter(DatabaseInstance &db, const string &path) {
	auto &fs = db.GetFileSystem();

	// Create parent directories if non existent
	auto pos = path.find_last_of(fs.PathSeparator(path));
	if (pos != path.npos) {
		fs.CreateDirectoriesRecursive(path.substr(0, pos));
	}

	FileOpenFlags flags;
	if (!fs.FileExists(path)) {
		flags = FileFlags::FILE_FLAGS_DISABLE_LOGGING | FileFlags::FILE_FLAGS_WRITE |
		        FileFlags::FILE_FLAGS_FILE_CREATE_NEW | FileCompressionType::UNCOMPRESSED;
	} else {
		flags = FileFlags::FILE_FLAGS_DISABLE_LOGGING | FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_APPEND;
	}

	return make_uniq<BufferedFileWriter>(fs, path, flags);
}

void FileLogStorage::Truncate() {
	lock_guard<mutex> lck(lock);

	// Reset buffers
	ResetAllBuffers();

	for (const auto &writer : file_writers) {
		// Truncate the file writer
		writer.second->Truncate(0);
		// Re-initialize the corresponding CSVWriter
		writers[writer.first]->Initialize(true);
		// TODO: why is this done?
		writer.second->Sync();
	}
}

void FileLogStorage::BeforeFlush(LoggingTargetTable table, DataChunk&) {
	// Lazily initialize the files
	Initialize(table);
}

void FileLogStorage::OnFlush(LoggingTargetTable table, DataChunk&) {
	file_writers[table]->Sync();
}

void FileLogStorage::Initialize(LoggingTargetTable table) {
	if (!initialized_writers[table]) {
		if (file_paths.find(table) == file_paths.end()) {
			throw InvalidConfigurationException("Failed to initialize file log storage table, path wasn't set");
		}

		InitializeFile(db, table);
	}
}

void FileLogStorage::SetPaths(const string &base_path) {
	file_paths.clear();

	LocalFileSystem fs;
	if (normalize_contexts) {
		file_paths[LoggingTargetTable::LOG_CONTEXTS] = fs.JoinPath(base_path, "duckdb_log_contexts.csv");
		file_paths[LoggingTargetTable::LOG_ENTRIES] = fs.JoinPath(base_path, "duckdb_log_entries.csv");
	} else {
		if (StringUtil::EndsWith(base_path, ".csv")) {
			file_paths[LoggingTargetTable::ALL_LOGS] = base_path;
		} else {
			file_paths[LoggingTargetTable::LOG_ENTRIES] = fs.JoinPath(base_path, "duckdb_log_entries.csv");
		}
	}
}

void FileLogStorage::UpdateConfigInternal(DatabaseInstance &db, case_insensitive_map_t<Value> &config) {
	auto config_copy = config;

	string new_path;
	bool normalize_contexts_new_value = normalize_contexts;

	vector<string> to_remove;
	for (const auto &it : config_copy) {
		if (StringUtil::Lower(it.first) == "path") {
			new_path = it.second.ToString();
			to_remove.push_back(it.first);
		}
		if (StringUtil::Lower(it.first) == "normalize") {
			normalize_contexts_new_value = it.second.GetValue<bool>();
			to_remove.push_back(it.first);
		}
	}

	// If any writer is initialized, we flush first:
	// - when switching between normalized and denormalized, this is necessary since we are changing the buffer schema
	// - when simply changing the path, it avoids writing log entries written before this change to end up in the new file
	bool initialized = false;
	for (auto &it : initialized_writers) {
		initialized |= it.second;
	}
	if (initialized) {
		FlushAllInternal();
	}

	bool normalize_contexts_changed = normalize_contexts != normalize_contexts_new_value;
	normalize_contexts = normalize_contexts_new_value;

	// Reset the buffers to ensure they have the correct schema
	if (normalize_contexts_changed) {
		ResetAllBuffers();
	}

	// Apply any path change
	if (new_path != base_path) {
		base_path = new_path;
		SetPaths(new_path);
	}

	for (const auto &it : to_remove) {
		config_copy.erase(it);
	}

	CSVLogStorage::UpdateConfigInternal(db, config_copy);
}

unique_ptr<TableRef> FileLogStorage::BindReplaceInternal(ClientContext &context, TableFunctionBindInput &input,
                                                         const string &path, const string &select_clause,
                                                         const string &csv_columns) {
	string sub_query_string;

	string escaped_path = KeywordHelper::WriteOptionallyQuoted(path);
	sub_query_string =
	    StringUtil::Format("%s FROM read_csv_auto(%s, columns={%s})", select_clause, escaped_path, csv_columns);

	Parser parser(context.GetParserOptions());
	parser.ParseQuery(sub_query_string);
	auto select_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));

	return duckdb::make_uniq<SubqueryRef>(std::move(select_stmt));
}

unique_ptr<TableRef> FileLogStorage::BindReplace(ClientContext &context, TableFunctionBindInput &input, LoggingTargetTable table) {
	lock_guard<mutex> lck(lock);

	// We only allow scanning enabled tables
	if (!IsEnabledInternal(table)) {
		return nullptr;
	}

	// We start by flushing the table to ensure we scan the latest version
	FlushInternal(table);

	if (normalize_contexts && table == LoggingTargetTable::ALL_LOGS) {
		throw InvalidConfigurationException("Can not scan ALL_LOGS table when logs are normalized");
	}
	if (!normalize_contexts && table != LoggingTargetTable::ALL_LOGS) {
		throw InvalidConfigurationException("Can only scan ALL_LOGS table when logs are normalized");
	}

	string select = "SELECT *";
	string path = file_paths[table];

	string columns;
	if (table == LoggingTargetTable::LOG_ENTRIES) {
		columns = "'context_id': 'UBIGINT', 'timestamp': 'TIMESTAMP', 'type': 'VARCHAR', 'log_level': 'VARCHAR' , "
				  "'message': 'VARCHAR'";
	} else if (table == LoggingTargetTable::LOG_CONTEXTS) {
		columns = "'context_id': 'UBIGINT', 'scope': 'VARCHAR', 'connection_id': 'UBIGINT', 'transaction_id': "
						 "'UBIGINT', 'query_id': 'UBIGINT', 'thread_id': 'UBIGINT'";
	} else if (table == LoggingTargetTable::ALL_LOGS) {
		select = "SELECT context_id, scope, connection_id, transaction_id, query_id, thread_id, timestamp, type, log_level, message ";
		columns = "'context_id': 'UBIGINT', 'scope': 'VARCHAR', 'connection_id': 'UBIGINT', 'transaction_id': "
				  "'UBIGINT', 'query_id': 'UBIGINT', 'thread_id': 'UBIGINT', 'timestamp': 'TIMESTAMP', 'type': "
				  "'VARCHAR', 'log_level': 'VARCHAR' , 'message': 'VARCHAR'";
	} else {
		throw InternalException("Invalid logging target table");
	}

	return BindReplaceInternal(context, input, path, select, columns);
}

BufferingLogStorage::BufferingLogStorage(DatabaseInstance &db_p, idx_t buffer_size, bool normalize) : normalize_contexts(normalize), buffer_limit(buffer_size) {
	ResetLogBuffers();
}

void BufferingLogStorage::ResetLogBuffers() {
	if (normalize_contexts) {
		buffers[LoggingTargetTable::LOG_ENTRIES] = make_uniq<DataChunk>();
		buffers[LoggingTargetTable::LOG_CONTEXTS] = make_uniq<DataChunk>();
		buffers[LoggingTargetTable::LOG_ENTRIES]->Initialize(Allocator::DefaultAllocator(), GetSchema(LoggingTargetTable::LOG_ENTRIES),
								   buffer_limit);
		buffers[LoggingTargetTable::LOG_CONTEXTS]->Initialize(Allocator::DefaultAllocator(), GetSchema(LoggingTargetTable::LOG_CONTEXTS),
								   buffer_limit);

	} else {
		buffers[LoggingTargetTable::ALL_LOGS] = make_uniq<DataChunk>();
		buffers[LoggingTargetTable::ALL_LOGS]->Initialize(Allocator::DefaultAllocator(), GetSchema(LoggingTargetTable::ALL_LOGS),
								   buffer_limit);
	}
}

void BufferingLogStorage::ResetAllBuffers() {
	ResetLogBuffers();
}

InMemoryLogStorageScanState::InMemoryLogStorageScanState(LoggingTargetTable table) : LogStorageScanState(table){
}
InMemoryLogStorageScanState::~InMemoryLogStorageScanState() {
}

InMemoryLogStorage::InMemoryLogStorage(DatabaseInstance &db_p) : BufferingLogStorage(db_p, STANDARD_VECTOR_SIZE, true) {
	log_storage_buffers[LoggingTargetTable::LOG_ENTRIES] = make_uniq<ColumnDataCollection>(db_p.GetBufferManager(), GetSchema(LoggingTargetTable::LOG_ENTRIES));
	log_storage_buffers[LoggingTargetTable::LOG_CONTEXTS] = make_uniq<ColumnDataCollection>(db_p.GetBufferManager(), GetSchema(LoggingTargetTable::LOG_CONTEXTS));
}

void InMemoryLogStorage::ResetAllBuffers() {
	BufferingLogStorage::ResetAllBuffers();

	for (const auto &buffer: log_storage_buffers) {
		buffer.second->Reset();
	}
}

InMemoryLogStorage::~InMemoryLogStorage() {
}

BufferingLogStorage::~BufferingLogStorage() {
}

static void WriteLoggingContextsToChunk(DataChunk &chunk, const RegisteredLoggingContext &context, idx_t &col) {

	auto size = chunk.size();

	auto context_id_data = FlatVector::GetData<idx_t>(chunk.data[col++]);
	context_id_data[size] = context.context_id;

	auto context_scope_data = FlatVector::GetData<string_t>(chunk.data[col]);
	context_scope_data[size] = StringVector::AddString(chunk.data[col++], EnumUtil::ToString(context.context.scope));

	if (context.context.connection_id.IsValid()) {
		auto client_context_data = FlatVector::GetData<idx_t>(chunk.data[col++]);
		client_context_data[size] = context.context.connection_id.GetIndex();
	} else {
		FlatVector::Validity(chunk.data[col++]).SetInvalid(size);
	}
	if (context.context.transaction_id.IsValid()) {
		auto client_context_data = FlatVector::GetData<idx_t>(chunk.data[col++]);
		client_context_data[size] = context.context.transaction_id.GetIndex();
	} else {
		FlatVector::Validity(chunk.data[col++]).SetInvalid(size);
	}
	if (context.context.query_id.IsValid()) {
		auto client_context_data = FlatVector::GetData<idx_t>(chunk.data[col++]);
		client_context_data[size] = context.context.query_id.GetIndex();
	} else {
		FlatVector::Validity(chunk.data[col++]).SetInvalid(size);
	}

	if (context.context.thread_id.IsValid()) {
		auto thread_data = FlatVector::GetData<idx_t>(chunk.data[col++]);
		thread_data[size] = context.context.thread_id.GetIndex();
	} else {
		FlatVector::Validity(chunk.data[col++]).SetInvalid(size);
	}

	chunk.SetCardinality(size + 1);
}

void BufferingLogStorage::WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type,
                                        const string &log_message, const RegisteredLoggingContext &context) {
	unique_lock<mutex> lck(lock);

	auto &log_entries_buffer = normalize_contexts ? buffers[LoggingTargetTable::LOG_ENTRIES] : buffers[LoggingTargetTable::ALL_LOGS];

	auto size = log_entries_buffer->size();

	if (size + 1 > buffer_limit) {
		if (normalize_contexts) {
			if (flush_contexts_on_next_entry_flush) {
				FlushInternal(LoggingTargetTable::LOG_CONTEXTS);
				flush_contexts_on_next_entry_flush = false;
			}
			FlushInternal(LoggingTargetTable::LOG_ENTRIES);
		} else {
			FlushInternal(LoggingTargetTable::ALL_LOGS);
		}

		size = log_entries_buffer->size();
	}

	if (registered_contexts.find(context.context_id) == registered_contexts.end()) {
		WriteLoggingContext(context);
		// New context_id: we should flush both contexts and entries next time LOG_ENTRIES gets flushed
		flush_contexts_on_next_entry_flush = true;
	}

	idx_t col = 0;

	if (normalize_contexts) {
		auto context_id_data = FlatVector::GetData<idx_t>(log_entries_buffer->data[col++]);
		context_id_data[size] = context.context_id;
	} else {
		WriteLoggingContextsToChunk(*log_entries_buffer, context, col);
	}

	auto timestamp_data = FlatVector::GetData<timestamp_t>(log_entries_buffer->data[col++]);
	timestamp_data[size] = timestamp;

	auto type_data = FlatVector::GetData<string_t>(log_entries_buffer->data[col]);
	type_data[size] = StringVector::AddString(log_entries_buffer->data[col++], log_type);

	auto level_data = FlatVector::GetData<string_t>(log_entries_buffer->data[col]);
	level_data[size] = StringVector::AddString(log_entries_buffer->data[col++],
	                                           EnumUtil::ToString(level)); // TODO: do cast on write out

	auto message_data = FlatVector::GetData<string_t>(log_entries_buffer->data[col]);
	message_data[size] = StringVector::AddString(log_entries_buffer->data[col++], log_message);

	log_entries_buffer->SetCardinality(size + 1);
}

void BufferingLogStorage::WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) {
	throw NotImplementedException("BufferingLogStorage::WriteLogEntries(DataChunk &chunk) not implemented");
}

void BufferingLogStorage::FlushAll() {
	unique_lock<mutex> lck(lock);
	if (!only_flush_on_full_buffer) {
		FlushAllInternal();
	}
}

void BufferingLogStorage::Flush(LoggingTargetTable table) {
	unique_lock<mutex> lck(lock);
	if (!only_flush_on_full_buffer) {
		FlushInternal(table);
	}
}

void BufferingLogStorage::Truncate() {
	unique_lock<mutex> lck(lock);
	ResetAllBuffers();
}

void InMemoryLogStorage::FlushChunk(LoggingTargetTable table, DataChunk &chunk) {
	D_ASSERT(table == LoggingTargetTable::LOG_ENTRIES || table == LoggingTargetTable::LOG_CONTEXTS);
	log_storage_buffers[table]->Append(chunk);
}

void BufferingLogStorage::FlushAllInternal() {
	if (normalize_contexts) {
		FlushInternal(LoggingTargetTable::LOG_ENTRIES);
		FlushInternal(LoggingTargetTable::LOG_CONTEXTS);
	} else {
		FlushInternal(LoggingTargetTable::ALL_LOGS);
	}
}

void BufferingLogStorage::FlushInternal(LoggingTargetTable table) {
	if (!IsEnabledInternal(table)) {
		throw InvalidConfigurationException("Cannot flush disabled logging target");
	}
	FlushChunk(table, *buffers[table]);
	buffers[table]->Reset();
}

void BufferingLogStorage::WriteLoggingContext(const RegisteredLoggingContext &context) {
	registered_contexts.insert(context.context_id);

	// If we don't normalize the contexts they are written out on every log entry
	if (!normalize_contexts) {
		return;
	}

	idx_t col = 0;

	auto &log_contexts_buffer = buffers[LoggingTargetTable::LOG_CONTEXTS];

	if (log_contexts_buffer->size() + 1 > buffer_limit) {
		FlushInternal(LoggingTargetTable::LOG_CONTEXTS);
	}

	WriteLoggingContextsToChunk(*log_contexts_buffer, context, col);
}

bool InMemoryLogStorage::CanScan(LoggingTargetTable table) {
	unique_lock<mutex> lck(lock);
	return IsEnabledInternal(table);
}

unique_ptr<LogStorageScanState> InMemoryLogStorage::CreateScanState(LoggingTargetTable table) const {
	return make_uniq<InMemoryLogStorageScanState>(table);
}

bool InMemoryLogStorage::Scan(LogStorageScanState &state, DataChunk &result) const {
	unique_lock<mutex> lck(lock);
	auto &in_mem_scan_state = state.Cast<InMemoryLogStorageScanState>();
	return GetBuffer(in_mem_scan_state.table).Scan(in_mem_scan_state.scan_state, result);
}

void InMemoryLogStorage::InitializeScan(LogStorageScanState &state) const {
	unique_lock<mutex> lck(lock);
	auto &in_mem_scan_state = state.Cast<InMemoryLogStorageScanState>();
	 GetBuffer(in_mem_scan_state.table).InitializeScan(in_mem_scan_state.scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
}

} // namespace duckdb
