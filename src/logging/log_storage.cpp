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
void LogStorage::Truncate() {
	throw NotImplementedException("Not implemented for this LogStorage: TruncateLogStorage");
}

void LogStorage::UpdateConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config) {
	if (config.size() > 1) {
		throw InvalidInputException("LogStorage does not support passing configuration");
	}
}

unique_ptr<TableRef> LogStorage::BindReplaceEntries(ClientContext &context, TableFunctionBindInput &input) {
	return nullptr;
}

unique_ptr<TableRef> LogStorage::BindReplaceContexts(ClientContext &context, TableFunctionBindInput &input) {
	return nullptr;
}

CSVLogStorage::~CSVLogStorage() {
}

CSVLogStorage::CSVLogStorage(DatabaseInstance &db) : BufferingLogStorage(db) {
	ResetCastChunk();
}

void CSVLogStorage::UpdateConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config) {
	lock_guard<mutex> lck(lock);
	return UpdateConfigInternal(db, config);
}

void CSVLogStorage::ExecuteCast() {
	log_entries_cast_buffer->Reset();
	log_contexts_cast_buffer->Reset();

	bool success = true;

	CastParameters cast_params;

	if (normalize_contexts) {
		// -- Cast Log Entries
		// context_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(
		    log_entries_buffer->data[0], log_entries_cast_buffer->data[0], log_entries_buffer->size(), cast_params);
		// timestamp: LogicalType::TIMESTAMP
		success &= VectorCastHelpers::StringCast<timestamp_t, duckdb::StringCast>(
		    log_entries_buffer->data[1], log_entries_cast_buffer->data[1], log_entries_buffer->size(), cast_params);
		// log_type: LogicalType::VARCHAR  (no cast)
		log_entries_cast_buffer->data[2].Reference(log_entries_buffer->data[2]);
		// level: LogicalType::VARCHAR  (no cast)
		log_entries_cast_buffer->data[3].Reference(log_entries_buffer->data[3]);
		// message: LogicalType::VARCHAR  (no cast)
		log_entries_cast_buffer->data[4].Reference(log_entries_buffer->data[4]);

		log_entries_cast_buffer->SetCardinality(log_entries_buffer->size());

		// -- Cast Log Contexts
		// context_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(
		    log_contexts_buffer->data[0], log_contexts_cast_buffer->data[0], log_contexts_buffer->size(), cast_params);
		// scope: LogicalType::VARCHAR (no cast)
		log_contexts_cast_buffer->data[1].Reference(log_contexts_buffer->data[1]);
		// connection_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(
		    log_contexts_buffer->data[2], log_contexts_cast_buffer->data[2], log_contexts_buffer->size(), cast_params);
		// transaction_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(
		    log_contexts_buffer->data[3], log_contexts_cast_buffer->data[3], log_contexts_buffer->size(), cast_params);
		// query_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(
		    log_contexts_buffer->data[4], log_contexts_cast_buffer->data[4], log_contexts_buffer->size(), cast_params);
		// thread: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(
		    log_contexts_buffer->data[5], log_contexts_cast_buffer->data[5], log_contexts_buffer->size(), cast_params);
		// scope is already string so doesn't need casting

		log_contexts_cast_buffer->SetCardinality(log_contexts_buffer->size());
	} else {
		// -- Cast Log Entries

		// context_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(
		    log_entries_buffer->data[0], log_entries_cast_buffer->data[0], log_entries_buffer->size(), cast_params);
		// scope: LogicalType::VARCHAR (no cast)
		log_entries_cast_buffer->data[1].Reference(log_entries_buffer->data[1]);
		// connection_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(
		    log_entries_buffer->data[2], log_entries_cast_buffer->data[2], log_entries_buffer->size(), cast_params);
		// transaction_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(
		    log_entries_buffer->data[3], log_entries_cast_buffer->data[3], log_entries_buffer->size(), cast_params);
		// query_id: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(
		    log_entries_buffer->data[4], log_entries_cast_buffer->data[4], log_entries_buffer->size(), cast_params);
		// thread: LogicalType::UBIGINT
		success &= VectorCastHelpers::StringCast<idx_t, duckdb::StringCast>(
		    log_entries_buffer->data[5], log_entries_cast_buffer->data[5], log_entries_buffer->size(), cast_params);
		// timestamp: LogicalType::TIMESTAMP
		success &= VectorCastHelpers::StringCast<timestamp_t, duckdb::StringCast>(
		    log_entries_buffer->data[6], log_entries_cast_buffer->data[6], log_entries_buffer->size(), cast_params);
		// log_type: LogicalType::VARCHAR  (no cast)
		log_entries_cast_buffer->data[7].Reference(log_entries_buffer->data[7]);
		// level: LogicalType::VARCHAR  (no cast)
		log_entries_cast_buffer->data[8].Reference(log_entries_buffer->data[8]);
		// message: LogicalType::VARCHAR  (no cast)
		log_entries_cast_buffer->data[9].Reference(log_entries_buffer->data[9]);

		log_entries_cast_buffer->SetCardinality(log_entries_buffer->size());
	}

	if (!success) {
		throw InvalidInputException("Failed to cast log entries");
	}
}

void CSVLogStorage::ResetAllBuffers() {
	BufferingLogStorage::ResetAllBuffers();
	ResetCastChunk();
}

void CSVLogStorage::ResetCastChunk() {
	log_entries_cast_buffer = make_uniq<DataChunk>();
	log_contexts_cast_buffer = make_uniq<DataChunk>();

	// Initialize the Cast chunks for casting everything to strings
	vector<LogicalType> types;
	types.resize(log_entries_buffer->ColumnCount(), LogicalType::VARCHAR);
	log_entries_cast_buffer->Initialize(Allocator::DefaultAllocator(), types);

	types.resize(log_contexts_buffer->ColumnCount(), LogicalType::VARCHAR);
	log_contexts_cast_buffer->Initialize(Allocator::DefaultAllocator(), types);
}

void CSVLogStorage::SetWriterConfigs(CSVWriter &writer, vector<string> column_names) {
	writer.options.dialect_options.state_machine_options.escape = '\"';
	writer.options.dialect_options.state_machine_options.quote = '\"';
	writer.options.dialect_options.state_machine_options.delimiter = CSVOption<string>("\t");
	writer.options.name_list = column_names;

	writer.options.force_quote = vector<bool>(column_names.size(), false);
}

void CSVLogStorage::FlushInternal(LoggingTargetTable table) {
	// Execute the cast
	ExecuteCast();

	// Write the cast data to sCSV
	if (log_entries_cast_buffer->size() > 0) {
		log_entries_writer->WriteChunk(*log_entries_cast_buffer);
		log_entries_writer->Flush();
		log_entries_buffer->Reset();
	}

	if (log_contexts_cast_buffer->size() > 0) {
		log_contexts_writer->WriteChunk(*log_contexts_cast_buffer);
		log_contexts_writer->Flush();
		log_contexts_buffer->Reset();
	}
}

void CSVLogStorage::UpdateConfigInternal(DatabaseInstance &db, case_insensitive_map_t<Value> &config) {
	for (const auto &it : config) {
		if (StringUtil::Lower(it.first) == "buffer_size") {
			buffer_limit = it.second.GetValue<uint64_t>();
		} else {
			throw InvalidInputException("Unrecognized log storage config option: '%s'", it.first);
		}
	}
}

StdOutLogStorage::StdOutLogStorage(DatabaseInstance &db) : CSVLogStorage(db) {
	log_entries_stream = make_uniq<MemoryStream>();
	log_contexts_stream = make_uniq<MemoryStream>();
	log_entries_writer = make_uniq<CSVWriter>(*log_contexts_stream, GetColumnNames(LoggingTargetTable::LOG_ENTRIES), false);
	log_contexts_writer = make_uniq<CSVWriter>(*log_contexts_stream, GetColumnNames(LoggingTargetTable::LOG_CONTEXTS), false);

	SetWriterConfigs(*log_entries_writer, GetColumnNames(LoggingTargetTable::LOG_ENTRIES));
	SetWriterConfigs(*log_contexts_writer, GetColumnNames(LoggingTargetTable::LOG_CONTEXTS));
}

StdOutLogStorage::~StdOutLogStorage() {
}

void StdOutLogStorage::FlushInternal(LoggingTargetTable table) {
	// Flush CSV buffer into stream
	CSVLogStorage::FlushInternal(table);

	// Write stream to stdout
	std::cout.write(const_char_ptr_cast(log_entries_stream->GetData()),
	                NumericCast<int64_t>(log_entries_stream->GetPosition()));
	std::cout.flush();
	log_entries_stream->Rewind();
}

static string GetDefaultPath(DatabaseInstance &db, const string &filename) {
	auto &fs = db.GetFileSystem();
	DatabaseFileOpener opener(db);
	auto default_path = FileSystem::GetHomeDirectory(opener);
	default_path = fs.JoinPath(default_path, ".duckdb");
	default_path = fs.JoinPath(default_path, "logs");
	default_path = fs.JoinPath(default_path, filename);
	return default_path;
}

string FileLogStorage::GetDefaultLogEntriesFilePath(DatabaseInstance &db) {
	return GetDefaultPath(db, "log_entries.csv");
}

string FileLogStorage::GetDefaultLogContextsFilePath(DatabaseInstance &db) {
	return GetDefaultPath(db, "log_contexts.csv");
}

FileLogStorage::FileLogStorage(DatabaseInstance &db_p)
    : CSVLogStorage(db_p), db(db_p), log_contexts_path(GetDefaultLogContextsFilePath(db)),
      log_entries_path(GetDefaultLogEntriesFilePath(db)) {
}

FileLogStorage::~FileLogStorage() {
}

void FileLogStorage::InitializeFile(DatabaseInstance &db, const string &path,
                                    unique_ptr<BufferedFileWriter> &file_writer, unique_ptr<CSVWriter> &csv_writer,
                                    vector<string> column_names) {
	//! Create file writer
	file_writer = InitializeFileWriter(db, path);

	//! Create CSV writer that writes to file
	csv_writer = make_uniq<CSVWriter>(*file_writer, column_names, false);
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

void FileLogStorage::InitializeLogContextsFile(DatabaseInstance &db) {
	InitializeFile(db, log_contexts_path, log_contexts_file_writer, log_contexts_writer, GetColumnNames(LoggingTargetTable::LOG_CONTEXTS));
}

void FileLogStorage::InitializeLogEntriesFile(DatabaseInstance &db) {
	InitializeFile(db, log_entries_path, log_entries_file_writer, log_entries_writer,
	               normalize_contexts ? GetColumnNames(LoggingTargetTable::LOG_ENTRIES) : GetColumnNames(LoggingTargetTable::ALL_LOGS));
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

	// Truncate the writers
	if (log_entries_file_writer) {
		log_entries_file_writer->Truncate(0);
		log_entries_writer->Initialize(true);
		log_entries_file_writer->Sync();
	}
	if (log_contexts_file_writer) {
		log_contexts_file_writer->Truncate(0);
		log_contexts_writer->Initialize(true);
		log_contexts_file_writer->Sync();
	}
}

void FileLogStorage::FlushInternal(LoggingTargetTable table) {
	// Early out if buffers empty
	if (log_contexts_buffer->size() == 0 && log_entries_buffer->size() == 0) {
		return;
	}

	Initialize();

	// Call base class FlushInternal to perform cast and write buffers to CSVWriters
	CSVLogStorage::FlushInternal(table);

	// Sync the writers to disk
	if (log_contexts_file_writer) {
		log_contexts_file_writer->Sync();
	}
	if (log_entries_file_writer) {
		log_entries_file_writer->Sync();
	}
}

void FileLogStorage::Initialize() {
	if (!initialized) {
		InitializeLogEntriesFile(db);
		if (normalize_contexts) {
			InitializeLogContextsFile(db);
		}
	}
}

void FileLogStorage::UpdateConfigInternal(DatabaseInstance &db, case_insensitive_map_t<Value> &config) {
	auto config_copy = config;

	string contexts_path_new_value = log_contexts_path;
	string entries_path_new_value = log_entries_path;
	bool normalize_contexts_new_value = normalize_contexts;

	vector<string> to_remove;
	for (const auto &it : config_copy) {
		if (StringUtil::Lower(it.first) == "path") {
			entries_path_new_value = it.second.ToString();
			to_remove.push_back(it.first);
			normalize_contexts_new_value = false;
		} else if (StringUtil::Lower(it.first) == "contexts_path") {
			contexts_path_new_value = it.second.ToString();
			to_remove.push_back(it.first);
			normalize_contexts_new_value = true;
		} else if (StringUtil::Lower(it.first) == "entries_path") {
			entries_path_new_value = it.second.ToString();
			to_remove.push_back(it.first);
			normalize_contexts_new_value = true;
		}
	}

	// If we are initialized, we need to flush first
	if (initialized) {
		FlushAllInternal();
	}

	log_entries_path = entries_path_new_value;
	log_contexts_path = contexts_path_new_value;

	bool normalize_contexts_changed = normalize_contexts != normalize_contexts_new_value;
	normalize_contexts = normalize_contexts_new_value;

	// Reset the buffers to ensure they have the correct schema
	if (normalize_contexts_changed) {
		ResetAllBuffers();
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

unique_ptr<TableRef> FileLogStorage::BindReplaceEntries(ClientContext &context, TableFunctionBindInput &input) {
	lock_guard<mutex> lck(lock);

	string columns;
	string select;
	if (normalize_contexts) {
		FlushInternal(LoggingTargetTable::LOG_ENTRIES);
		select = "SELECT *";
		columns = "'context_id': 'UBIGINT', 'timestamp': 'TIMESTAMP', 'type': 'VARCHAR', 'log_level': 'VARCHAR' , "
		          "'message': 'VARCHAR'";
	} else {
		FlushInternal(LoggingTargetTable::ALL_LOGS);
		select = "SELECT context_id, timestamp, type, log_level, message";
		columns = "'context_id': 'UBIGINT', 'scope': 'VARCHAR', 'connection_id': 'UBIGINT', 'transaction_id': "
		          "'UBIGINT', 'query_id': 'UBIGINT', 'thread_id': 'UBIGINT', 'timestamp': 'TIMESTAMP', 'type': "
		          "'VARCHAR', 'log_level': 'VARCHAR' , 'message': 'VARCHAR'";
	}

	return BindReplaceInternal(context, input, log_entries_path, select, columns);
}

unique_ptr<TableRef> FileLogStorage::BindReplaceContexts(ClientContext &context, TableFunctionBindInput &input) {
	lock_guard<mutex> lck(lock);

	FlushInternal(LoggingTargetTable::LOG_CONTEXTS);
	if (normalize_contexts) {
		string columns = "'context_id': 'UBIGINT', 'scope': 'VARCHAR', 'connection_id': 'UBIGINT', 'transaction_id': "
		                 "'UBIGINT', 'query_id': 'UBIGINT', 'thread_id': 'UBIGINT'";
		return BindReplaceInternal(context, input, log_contexts_path, "SELECT *", columns);
	}

	// When log contexts are denormalized in the csv files, we will be reading them horribly inefficiently by doing a
	// select DISTINCT on the log_entries_file_handl TODO: fix? throw?
	string columns = "'context_id': 'UBIGINT', 'scope': 'VARCHAR', 'connection_id': 'UBIGINT', 'transaction_id': "
	                 "'UBIGINT', 'query_id': 'UBIGINT', 'thread_id': 'UBIGINT', 'timestamp': 'TIMESTAMP', 'type': "
	                 "'VARCHAR', 'log_level': 'VARCHAR' , 'message': 'VARCHAR'";
	string query =
	    "SELECT DISTINCT context_id as context_id, scope, connection_id, transaction_id, query_id, thread_id";
	return BindReplaceInternal(context, input, log_entries_path, query, columns);
}

BufferingLogStorage::BufferingLogStorage(DatabaseInstance &db_p) {
	ResetLogBuffers();
}

void BufferingLogStorage::ResetLogBuffers() {
	max_buffer_size = STANDARD_VECTOR_SIZE;
	log_entries_buffer = make_uniq<DataChunk>();
	log_contexts_buffer = make_uniq<DataChunk>();

	if (normalize_contexts) {
		log_entries_buffer->Initialize(Allocator::DefaultAllocator(), GetSchema(LoggingTargetTable::LOG_ENTRIES),
								   max_buffer_size);
	} else {
		log_entries_buffer->Initialize(Allocator::DefaultAllocator(), GetSchema(LoggingTargetTable::ALL_LOGS),
								   max_buffer_size);
	}
	log_contexts_buffer->Initialize(Allocator::DefaultAllocator(), GetSchema(LoggingTargetTable::LOG_CONTEXTS), max_buffer_size);
}

void BufferingLogStorage::ResetAllBuffers() {
	ResetLogBuffers();
}

InMemoryLogStorageScanState::InMemoryLogStorageScanState() {
}
InMemoryLogStorageScanState::~InMemoryLogStorageScanState() {
}

InMemoryLogStorage::InMemoryLogStorage(DatabaseInstance &db_p) : BufferingLogStorage(db_p) {
	max_buffer_size = STANDARD_VECTOR_SIZE;
	log_entries = make_uniq<ColumnDataCollection>(db_p.GetBufferManager(), GetSchema(LoggingTargetTable::LOG_ENTRIES));
	log_contexts = make_uniq<ColumnDataCollection>(db_p.GetBufferManager(), GetSchema(LoggingTargetTable::LOG_CONTEXTS));
}

void InMemoryLogStorage::ResetAllBuffers() {
	BufferingLogStorage::ResetAllBuffers();
	log_entries->Reset();
	log_contexts->Reset();
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

	if (registered_contexts.find(context.context_id) == registered_contexts.end()) {
		WriteLoggingContext(context);
	}

	auto size = log_entries_buffer->size();

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

	if (size + 1 >= max_buffer_size) {
		FlushInternal(LoggingTargetTable::LOG_ENTRIES);
	}
}

void BufferingLogStorage::WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) {
	throw NotImplementedException("BufferingLogStorage::WriteLogEntries(DataChunk &chunk) not implemented");
}

void BufferingLogStorage::Flush() {
	unique_lock<mutex> lck(lock);
	FlushAllInternal();
}

void BufferingLogStorage::Truncate() {
	unique_lock<mutex> lck(lock);
	ResetAllBuffers();
}

void InMemoryLogStorage::FlushInternal(LoggingTargetTable table) {
	if (log_entries_buffer->size() > 0) {
		log_entries->Append(*log_entries_buffer);
		log_entries_buffer->Reset();
	}

	if (log_contexts_buffer->size() > 0) {
		log_contexts->Append(*log_contexts_buffer);
		log_contexts_buffer->Reset();
	}
}

void BufferingLogStorage::FlushAllInternal() {
	if (normalize_contexts) {
		FlushInternal(LoggingTargetTable::LOG_ENTRIES);
		FlushInternal(LoggingTargetTable::LOG_CONTEXTS);
	} else {
		FlushInternal(LoggingTargetTable::ALL_LOGS);
	}
}

void BufferingLogStorage::WriteLoggingContext(const RegisteredLoggingContext &context) {
	registered_contexts.insert(context.context_id);

	// If we don't normalize the contexts they are written out on every log entry
	if (!normalize_contexts) {
		return;
	}

	idx_t col = 0;
	WriteLoggingContextsToChunk(*log_contexts_buffer, context, col);

	if (log_contexts_buffer->size() >= max_buffer_size) {
		FlushInternal(LoggingTargetTable::LOG_CONTEXTS);
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
