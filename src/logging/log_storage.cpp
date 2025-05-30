#include "duckdb/logging/log_storage.hpp"

#include "duckdb/common/csv_utils.hpp"
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

static void WriteDelim(LogStorageCsvConfig &config, WriteStream &writer) {
	writer.WriteData(const_data_ptr_cast(config.delim.c_str()), config.delim.size());
}

static void WriteOptionalId(LogStorageCsvConfig &config, WriteStream &writer, const optional_idx &id,
                            bool write_delim = true) {
	string result_string;
	if (id.IsValid()) {
		result_string = to_string(id.GetIndex());
	}
	writer.WriteData(const_data_ptr_cast(result_string.c_str()), result_string.size());
	if (write_delim) {
		WriteDelim(config, writer);
	}
}

static void WriteString(LogStorageCsvConfig &config, WriteStream &writer, const string &value,
                        bool write_delim = true) {
	writer.WriteData(const_data_ptr_cast(value.c_str()), value.size());
	if (write_delim) {
		WriteDelim(config, writer);
	}
}

template <class T>
static void WriteEnum(LogStorageCsvConfig &config, WriteStream &writer, const T &enum_value, bool write_delim = true) {
	auto scope_string = EnumUtil::ToString(enum_value);
	writer.WriteData(const_data_ptr_cast(scope_string.c_str()), scope_string.size());
	if (write_delim) {
		WriteDelim(config, writer);
	}
}

static void WriteLogEntryToCSVString(LogStorageCsvConfig &config, WriteStream &writer, timestamp_t timestamp,
                                     LogLevel level, const string &log_type, const string &log_message,
                                     const RegisteredLoggingContext &context, bool normalize_context = false) {

	auto context_id_string = to_string(context.context_id);
	writer.WriteData(const_data_ptr_cast(context_id_string.c_str()), context_id_string.size());
	WriteDelim(config, writer);
	if (!normalize_context) {
		WriteEnum(config, writer, context.context.scope);
		WriteOptionalId(config, writer, context.context.connection_id);
		WriteOptionalId(config, writer, context.context.transaction_id);
		WriteOptionalId(config, writer, context.context.query_id);
		WriteOptionalId(config, writer, context.context.thread_id);
	}

	// Write timestamp
	auto timestamp_string = Value::TIMESTAMP(timestamp).ToString();
	writer.WriteData(const_data_ptr_cast(timestamp_string.c_str()), timestamp_string.size());
	WriteDelim(config, writer);

	// Write level
	WriteEnum(config, writer, level);

	// Write log type
	writer.WriteData(const_data_ptr_cast(log_type.c_str()), log_type.size());
	WriteDelim(config, writer);

	// Write message
	CSVUtils::WriteQuotedString(writer, log_message.c_str(), log_message.size(), false, config.null_strings,
	                            config.requires_quotes, config.quote, config.escape);

	writer.WriteData(const_data_ptr_cast(config.newline.c_str()), config.newline.size());
}

static void WriteLogContextToCSVString(LogStorageCsvConfig &config, WriteStream &writer,
                                       const RegisteredLoggingContext &context) {
	auto context_id_string = to_string(context.context_id);
	writer.WriteData(const_data_ptr_cast(context_id_string.c_str()), context_id_string.size());
	WriteDelim(config, writer);
	WriteEnum(config, writer, context.context.scope);
	WriteOptionalId(config, writer, context.context.connection_id);
	WriteOptionalId(config, writer, context.context.transaction_id);
	WriteOptionalId(config, writer, context.context.query_id);
	WriteOptionalId(config, writer, context.context.thread_id, false);
}

CSVLogStorage::CSVLogStorage(DatabaseInstance &db) {
	log_entries_stream = make_uniq<MemoryStream>();
	log_contexts_stream = make_uniq<MemoryStream>();
}

CSVLogStorage::~CSVLogStorage() {
}

void CSVLogStorage::WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type,
                                  const string &log_message, const RegisteredLoggingContext &context) {
	lock_guard<mutex> lck(lock);

	if (normalize_contexts && registered_contexts.find(context.context_id) == registered_contexts.end()) {
		WriteLogContextToCSVString(csv_config, *log_contexts_stream, context);
		registered_contexts.insert(context.context_id);
	}

	WriteLogEntryToCSVString(csv_config, *log_entries_stream, timestamp, level, log_type, log_message, context,
	                         normalize_contexts);

	if (log_entries_stream->GetPosition() > buffer_limit) {
		FlushInternal();
	}
}

void CSVLogStorage::WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) {
	throw NotImplementedException("CSVLogStorage::WriteLogEntries");
}

void CSVLogStorage::UpdateConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config) {
	lock_guard<mutex> lck(lock);
	return UpdateConfigInternal(db, config);
}

void CSVLogStorage::Flush() {
	lock_guard<mutex> lck(lock);
	FlushInternal();
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
}

StdOutLogStorage::~StdOutLogStorage() {
}

void StdOutLogStorage::Truncate() {
	// NOP
}

void StdOutLogStorage::FlushInternal() {
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

FileLogStorage::FileLogStorage(DatabaseInstance &db_p) : CSVLogStorage(db_p), db(db_p) {
}

FileLogStorage::~FileLogStorage() {
}

void FileLogStorage::WriteLogEntriesHeader() {
	MemoryStream buffer;
	WriteString(csv_config, buffer, "context_id");
	if (!normalize_contexts) {
		WriteString(csv_config, buffer, "scope");
		WriteString(csv_config, buffer, "connection_id");
		WriteString(csv_config, buffer, "transaction_id");
		WriteString(csv_config, buffer, "query_id");
		WriteString(csv_config, buffer, "thread_id");
	}
	WriteString(csv_config, buffer, "timestamp");
	WriteString(csv_config, buffer, "log_level");
	WriteString(csv_config, buffer, "type");
	WriteString(csv_config, buffer, "message", false);
	buffer.WriteData(const_data_ptr_cast(csv_config.newline.c_str()), csv_config.newline.size());
	log_entries_file_handle->Write(buffer.GetData(), buffer.GetPosition());
	log_entries_should_write_header = false;
}

void FileLogStorage::WriteLogContextsHeader() {
	MemoryStream buffer;
	WriteString(csv_config, buffer, "context_id");
	WriteString(csv_config, buffer, "scope");
	WriteString(csv_config, buffer, "connection_id");
	WriteString(csv_config, buffer, "transaction_id");
	WriteString(csv_config, buffer, "query_id");
	WriteString(csv_config, buffer, "thread_id", false);
	buffer.WriteData(const_data_ptr_cast(csv_config.newline.c_str()), csv_config.newline.size());
	log_contexts_file_handle->Write(buffer.GetData(), buffer.GetPosition());
	log_contexts_should_write_header = false;
}

void FileLogStorage::InitializeLogContextsFile(DatabaseInstance &db, const string &path) {
	if (path.empty()) {
		return InitializeFile(db, GetDefaultLogContextsFilePath(db), log_contexts_file_handle,
		                      log_contexts_should_write_header);
	}
	return InitializeFile(db, path, log_contexts_file_handle, log_contexts_should_write_header);
}

void FileLogStorage::InitializeLogEntriesFile(DatabaseInstance &db, const string &path) {
	if (path.empty()) {
		return InitializeFile(db, GetDefaultLogEntriesFilePath(db), log_entries_file_handle,
		                      log_entries_should_write_header);
	}
	return InitializeFile(db, path, log_entries_file_handle, log_entries_should_write_header);
}

void FileLogStorage::InitializeFile(DatabaseInstance &db, const string &path, unique_ptr<FileHandle> &handle,
                                    bool &should_write_header) {
	auto &fs = db.GetFileSystem();

	// Create parent directories if non existent
	auto pos = path.find_last_of(fs.PathSeparator(path));
	if (pos != path.npos) {
		fs.CreateDirectoriesRecursive(path.substr(0, pos));
	}

	if (!fs.FileExists(path)) {
		handle = fs.OpenFile(path,
		                     FileFlags::FILE_FLAGS_DISABLE_LOGGING | FileFlags::FILE_FLAGS_WRITE |
		                         FileFlags::FILE_FLAGS_FILE_CREATE_NEW,
		                     nullptr);
	} else {
		handle = fs.OpenFile(
		    path, FileFlags::FILE_FLAGS_DISABLE_LOGGING | FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_APPEND,
		    nullptr);
	}

	if (handle->GetFileSize() == 0) {
		should_write_header = true;
	}
}

void FileLogStorage::Truncate() {
	lock_guard<mutex> lck(lock);

	log_entries_file_handle->Truncate(0);
	log_entries_stream->Rewind();

	if (log_contexts_file_handle) {
		log_contexts_file_handle->Truncate(0);
		log_contexts_stream->Rewind();
	}
}

void FileLogStorage::FlushInternal() {
	if (!initialized) {
		InitializeLogEntriesFile(db);
		if (normalize_contexts) {
			InitializeLogContextsFile(db);
		}
		initialized = true;
	}

	// Write entries
	if (log_entries_stream->GetPosition() > 0) {
		if (log_entries_should_write_header) {
			WriteLogEntriesHeader();
		}
		log_entries_file_handle->Write((void *)log_entries_stream->GetData(), log_entries_stream->GetPosition());
		log_entries_stream->Rewind();
	}

	// Write contexts
	if (log_contexts_stream->GetPosition() > 0) {
		if (log_contexts_should_write_header) {
			WriteLogContextsHeader();
		}
		log_contexts_file_handle->Write((void *)log_contexts_stream->GetData(), log_contexts_stream->GetPosition());
		log_contexts_stream->Rewind();
	}
}

void FileLogStorage::UpdateConfigInternal(DatabaseInstance &db, case_insensitive_map_t<Value> &config) {
	auto config_copy = config;

	string contexts_path;
	string entries_path;

	vector<string> to_remove;
	for (const auto &it : config_copy) {
		if (StringUtil::Lower(it.first) == "path") {
			entries_path = it.second.ToString();
			to_remove.push_back(it.first);
			normalize_contexts = false;
		} else if (StringUtil::Lower(it.first) == "contexts_path") {
			contexts_path = it.second.ToString();
			to_remove.push_back(it.first);
			normalize_contexts = true;
		} else if (StringUtil::Lower(it.first) == "entries_path") {
			entries_path = it.second.ToString();
			to_remove.push_back(it.first);
			normalize_contexts = true;
		}
	}

	if (!contexts_path.empty() || !entries_path.empty()) {
		FlushInternal();
	}
	if (!entries_path.empty()) {
		InitializeLogEntriesFile(db, entries_path);
	}
	if (!contexts_path.empty()) {
		InitializeLogContextsFile(db, contexts_path);
	}

	for (const auto &it : to_remove) {
		config_copy.erase(it);
	}

	CSVLogStorage::UpdateConfigInternal(db, config_copy);
}

unique_ptr<TableRef> FileLogStorage::BindReplaceInternal(ClientContext &context, TableFunctionBindInput &input,
                                                         const string &path, const string &select_clause) {
	string sub_query_string;

	string escaped_path = KeywordHelper::WriteOptionallyQuoted(path);
	sub_query_string = StringUtil::Format("%s FROM %s", select_clause, escaped_path);

	Parser parser(context.GetParserOptions());
	parser.ParseQuery(sub_query_string);
	auto select_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));

	return duckdb::make_uniq<SubqueryRef>(std::move(select_stmt));
}

// TODO: we can remove this once the CSV reader handles growing CSV files properly
static void ThrowIfLogging(ClientContext &context) {
	if (LogManager::Get(context).GetConfig().enabled) {
		throw InvalidInputException("Please disable logging before scanning the logging csv file to avoid duckdb "
		                            "getting stuck in infinite logging limbo");
	}
}

unique_ptr<TableRef> FileLogStorage::BindReplaceEntries(ClientContext &context, TableFunctionBindInput &input) {
	ThrowIfLogging(context);

	lock_guard<mutex> lck(lock);
	FlushInternal();

	if (log_entries_file_handle) {
		return BindReplaceInternal(
		    context, input, log_entries_file_handle->path,
		    "SELECT context_id::UBIGINT as context_id, timestamp::TIMESTAMP as timestamp, type::VARCHAR as type, "
		    "log_level::VARCHAR as log_level, message::VARCHAR as message");
	}
	return nullptr;
}

unique_ptr<TableRef> FileLogStorage::BindReplaceContexts(ClientContext &context, TableFunctionBindInput &input) {
	ThrowIfLogging(context);

	lock_guard<mutex> lck(lock);
	FlushInternal();
	if (normalize_contexts) {
		D_ASSERT(log_contexts_file_handle);
		return BindReplaceInternal(context, input, log_contexts_file_handle->path,
		                           "SELECT context_id::UBIGINT as context_id, scope::VARCHAR as scope, "
		                           "connection_id::UBIGINT as connection_id, transaction_id::UBIGINT as "
		                           "transaction_id, query_id::UBIGINT as query_id, thread_id::UBIGINT as thread_id");
	}

	// When log contexts are denormalized in the csv files, we will be reading them horribly inefficiently by doing a
	// select DISTINCT on the log_entries_file_handle
	// TODO: fix? throw?
	return BindReplaceInternal(context, input, log_entries_file_handle->path,
	                           "SELECT DISTINCT context_id::UBIGINT as context_id, scope::VARCHAR as scope, "
	                           "connection_id::UBIGINT as connection_id, transaction_id::UBIGINT as transaction_id, "
	                           "query_id::UBIGINT as query_id, thread_id::UBIGINT as thread_id");
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
	    LogicalType::UBIGINT, // connection_id
	    LogicalType::UBIGINT, // transaction_id
	    LogicalType::UBIGINT, // query_id
	    LogicalType::UBIGINT, // thread
	};

	max_buffer_size = STANDARD_VECTOR_SIZE;
	entry_buffer->Initialize(Allocator::DefaultAllocator(), log_entry_schema, max_buffer_size);
	log_context_buffer->Initialize(Allocator::DefaultAllocator(), log_context_schema, max_buffer_size);
	log_entries = make_uniq<ColumnDataCollection>(db_p.GetBufferManager(), log_entry_schema);
	log_contexts = make_uniq<ColumnDataCollection>(db_p.GetBufferManager(), log_context_schema);
}

void InMemoryLogStorage::ResetBuffers() {
	entry_buffer->Reset();
	log_context_buffer->Reset();

	log_entries->Reset();
	log_contexts->Reset();

	registered_contexts.clear();
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

void InMemoryLogStorage::Truncate() {
	unique_lock<mutex> lck(lock);
	ResetBuffers();
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

	if (context.context.connection_id.IsValid()) {
		auto client_context_data = FlatVector::GetData<idx_t>(log_context_buffer->data[2]);
		client_context_data[size] = context.context.connection_id.GetIndex();
	} else {
		FlatVector::Validity(log_context_buffer->data[2]).SetInvalid(size);
	}
	if (context.context.transaction_id.IsValid()) {
		auto client_context_data = FlatVector::GetData<idx_t>(log_context_buffer->data[3]);
		client_context_data[size] = context.context.transaction_id.GetIndex();
	} else {
		FlatVector::Validity(log_context_buffer->data[3]).SetInvalid(size);
	}
	if (context.context.query_id.IsValid()) {
		auto client_context_data = FlatVector::GetData<idx_t>(log_context_buffer->data[4]);
		client_context_data[size] = context.context.query_id.GetIndex();
	} else {
		FlatVector::Validity(log_context_buffer->data[4]).SetInvalid(size);
	}

	if (context.context.thread_id.IsValid()) {
		auto thread_data = FlatVector::GetData<idx_t>(log_context_buffer->data[5]);
		thread_data[size] = context.context.thread_id.GetIndex();
	} else {
		FlatVector::Validity(log_context_buffer->data[5]).SetInvalid(size);
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
