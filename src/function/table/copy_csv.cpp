#include "duckdb/common/bind_helpers.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/write_stream.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <limits>

namespace duckdb {

void AreOptionsEqual(char str_1, char str_2, const string &name_str_1, const string &name_str_2) {
	if (str_1 == '\0' || str_2 == '\0') {
		return;
	}
	if (str_1 == str_2) {
		throw BinderException("%s must not appear in the %s specification and vice versa", name_str_1, name_str_2);
	}
}

void SubstringDetection(char str_1, string &str_2, const string &name_str_1, const string &name_str_2) {
	if (str_1 == '\0' || str_2.empty()) {
		return;
	}
	if (str_2.find(str_1) != string::npos) {
		throw BinderException("%s must not appear in the %s specification and vice versa", name_str_1, name_str_2);
	}
}

//===--------------------------------------------------------------------===//
// Bind
//===--------------------------------------------------------------------===//
void WriteQuoteOrEscape(WriteStream &writer, char quote_or_escape) {
	if (quote_or_escape != '\0') {
		writer.Write(quote_or_escape);
	}
}

void BaseCSVData::Finalize() {
	// verify that the options are correct in the final pass
	if (options.dialect_options.state_machine_options.escape == '\0') {
		options.dialect_options.state_machine_options.escape = options.dialect_options.state_machine_options.quote;
	}
	// escape and delimiter must not be substrings of each other
	AreOptionsEqual(options.dialect_options.state_machine_options.delimiter.GetValue(),
	                options.dialect_options.state_machine_options.escape.GetValue(), "DELIMITER", "ESCAPE");

	// delimiter and quote must not be substrings of each other
	AreOptionsEqual(options.dialect_options.state_machine_options.quote.GetValue(),
	                options.dialect_options.state_machine_options.delimiter.GetValue(), "DELIMITER", "QUOTE");

	// escape and quote must not be substrings of each other (but can be the same)
	if (options.dialect_options.state_machine_options.quote != options.dialect_options.state_machine_options.escape) {
		AreOptionsEqual(options.dialect_options.state_machine_options.quote.GetValue(),
		                options.dialect_options.state_machine_options.escape.GetValue(), "QUOTE", "ESCAPE");
	}

	// delimiter and quote must not be substrings of each other
	AreOptionsEqual(options.dialect_options.state_machine_options.comment.GetValue(),
	                options.dialect_options.state_machine_options.quote.GetValue(), "COMMENT", "QUOTE");

	// delimiter and quote must not be substrings of each other
	AreOptionsEqual(options.dialect_options.state_machine_options.comment.GetValue(),
	                options.dialect_options.state_machine_options.delimiter.GetValue(), "COMMENT", "DELIMITER");

	// null string and delimiter must not be substrings of each other
	for (auto &null_str : options.null_str) {
		if (!null_str.empty()) {
			SubstringDetection(options.dialect_options.state_machine_options.delimiter.GetValue(), null_str,
			                   "DELIMITER", "NULL");

			// quote and nullstr must not be substrings of each other
			SubstringDetection(options.dialect_options.state_machine_options.quote.GetValue(), null_str, "QUOTE",
			                   "NULL");

			// Validate the nullstr against the escape character
			const char escape = options.dialect_options.state_machine_options.escape.GetValue();
			// Allow nullstr to be escape character + some non-special character, e.g., "\N" (MySQL default).
			// In this case, only unquoted occurrences of the nullstr will be recognized as null values.
			if (options.dialect_options.state_machine_options.rfc_4180 == false && null_str.size() == 2 &&
			    null_str[0] == escape && null_str[1] != '\0') {
				continue;
			}
			SubstringDetection(escape, null_str, "ESCAPE", "NULL");
		}
	}

	if (!options.prefix.empty() || !options.suffix.empty()) {
		if (options.prefix.empty() || options.suffix.empty()) {
			throw BinderException("COPY ... (FORMAT CSV) must have both PREFIX and SUFFIX, or none at all");
		}
		if (options.dialect_options.header.GetValue()) {
			throw BinderException("COPY ... (FORMAT CSV)'s HEADER cannot be combined with PREFIX/SUFFIX");
		}
	}
}

string TransformNewLine(string new_line) {
	new_line = StringUtil::Replace(new_line, "\\r", "\r");
	return StringUtil::Replace(new_line, "\\n", "\n");
	;
}

static vector<unique_ptr<Expression>> CreateCastExpressions(WriteCSVData &bind_data, ClientContext &context,
                                                            const vector<string> &names,
                                                            const vector<LogicalType> &sql_types) {
	auto &options = bind_data.options;
	auto &formats = options.write_date_format;

	bool has_dateformat = !formats[LogicalTypeId::DATE].IsNull();
	bool has_timestampformat = !formats[LogicalTypeId::TIMESTAMP].IsNull();

	// Create a binder
	auto binder = Binder::CreateBinder(context);

	auto &bind_context = binder->bind_context;
	auto table_index = binder->GenerateTableIndex();
	bind_context.AddGenericBinding(table_index, "copy_csv", names, sql_types);

	// Create the ParsedExpressions (cast, strftime, etc..)
	vector<unique_ptr<ParsedExpression>> unbound_expressions;
	for (idx_t i = 0; i < sql_types.size(); i++) {
		auto &type = sql_types[i];
		auto &name = names[i];

		bool is_timestamp = type.id() == LogicalTypeId::TIMESTAMP || type.id() == LogicalTypeId::TIMESTAMP_TZ;
		if (has_dateformat && type.id() == LogicalTypeId::DATE) {
			// strftime(<name>, 'format')
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(make_uniq<BoundExpression>(make_uniq<BoundReferenceExpression>(name, type, i)));
			children.push_back(make_uniq<ConstantExpression>(formats[LogicalTypeId::DATE]));
			auto func = make_uniq_base<ParsedExpression, FunctionExpression>("strftime", std::move(children));
			unbound_expressions.push_back(std::move(func));
		} else if (has_timestampformat && is_timestamp) {
			// strftime(<name>, 'format')
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(make_uniq<BoundExpression>(make_uniq<BoundReferenceExpression>(name, type, i)));
			children.push_back(make_uniq<ConstantExpression>(formats[LogicalTypeId::TIMESTAMP]));
			auto func = make_uniq_base<ParsedExpression, FunctionExpression>("strftime", std::move(children));
			unbound_expressions.push_back(std::move(func));
		} else {
			// CAST <name> AS VARCHAR
			auto column = make_uniq<BoundExpression>(make_uniq<BoundReferenceExpression>(name, type, i));
			auto expr = make_uniq_base<ParsedExpression, CastExpression>(LogicalType::VARCHAR, std::move(column));
			unbound_expressions.push_back(std::move(expr));
		}
	}

	// Create an ExpressionBinder, bind the Expressions
	vector<unique_ptr<Expression>> expressions;
	ExpressionBinder expression_binder(*binder, context);
	expression_binder.target_type = LogicalType::VARCHAR;
	for (auto &expr : unbound_expressions) {
		expressions.push_back(expression_binder.Bind(expr));
	}

	return expressions;
}

static unique_ptr<FunctionData> WriteCSVBind(ClientContext &context, CopyFunctionBindInput &input,
                                             const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto bind_data = make_uniq<WriteCSVData>(input.info.file_path, sql_types, names);

	// check all the options in the copy info
	for (auto &option : input.info.options) {
		auto loption = StringUtil::Lower(option.first);
		auto &set = option.second;
		bind_data->options.SetWriteOption(loption, ConvertVectorToValue(set));
	}
	// verify the parsed options
	if (bind_data->options.force_quote.empty()) {
		// no FORCE_QUOTE specified: initialize to false
		bind_data->options.force_quote.resize(names.size(), false);
	}
	bind_data->Finalize();

	switch (bind_data->options.compression) {
	case FileCompressionType::GZIP:
		if (!IsFileCompressed(input.file_extension, FileCompressionType::GZIP)) {
			input.file_extension += CompressionExtensionFromType(FileCompressionType::GZIP);
		}
		break;
	case FileCompressionType::ZSTD:
		if (!IsFileCompressed(input.file_extension, FileCompressionType::ZSTD)) {
			input.file_extension += CompressionExtensionFromType(FileCompressionType::ZSTD);
		}
		break;
	default:
		break;
	}

	auto expressions = CreateCastExpressions(*bind_data, context, names, sql_types);
	bind_data->cast_expressions = std::move(expressions);

	bind_data->requires_quotes = make_unsafe_uniq_array<bool>(256);
	memset(bind_data->requires_quotes.get(), 0, sizeof(bool) * 256);
	bind_data->requires_quotes['\n'] = true;
	bind_data->requires_quotes['\r'] = true;
	bind_data->requires_quotes[NumericCast<idx_t>(
	    bind_data->options.dialect_options.state_machine_options.delimiter.GetValue())] = true;
	bind_data->requires_quotes[NumericCast<idx_t>(
	    bind_data->options.dialect_options.state_machine_options.quote.GetValue())] = true;

	if (!bind_data->options.write_newline.empty()) {
		bind_data->newline = TransformNewLine(bind_data->options.write_newline);
	}
	return std::move(bind_data);
}

static unique_ptr<FunctionData> ReadCSVBind(ClientContext &context, CopyInfo &info, vector<string> &expected_names,
                                            vector<LogicalType> &expected_types) {
	auto bind_data = make_uniq<ReadCSVData>();
	bind_data->csv_types = expected_types;
	bind_data->csv_names = expected_names;
	bind_data->return_types = expected_types;
	bind_data->return_names = expected_names;

	auto multi_file_reader = MultiFileReader::CreateDefault("CSVCopy");
	bind_data->files = multi_file_reader->CreateFileList(context, Value(info.file_path))->GetAllFiles();

	auto &options = bind_data->options;

	// check all the options in the copy info
	for (auto &option : info.options) {
		auto loption = StringUtil::Lower(option.first);
		auto &set = option.second;
		options.SetReadOption(loption, ConvertVectorToValue(set), expected_names);
	}
	// verify the parsed options
	if (options.force_not_null.empty()) {
		// no FORCE_QUOTE specified: initialize to false
		options.force_not_null.resize(expected_types.size(), false);
	}

	// Look for rejects table options last
	named_parameter_map_t options_map;
	for (auto &option : info.options) {
		options_map[option.first] = ConvertVectorToValue(std::move(option.second));
	}
	options.file_path = bind_data->files[0];
	options.name_list = expected_names;
	options.sql_type_list = expected_types;
	options.columns_set = true;
	for (idx_t i = 0; i < expected_types.size(); i++) {
		options.sql_types_per_column[expected_names[i]] = i;
	}

	if (options.auto_detect) {
		auto buffer_manager = make_shared_ptr<CSVBufferManager>(context, options, bind_data->files[0], 0);
		CSVSniffer sniffer(options, buffer_manager, CSVStateMachineCache::Get(context));
		sniffer.SniffCSV();
	}
	bind_data->FinalizeRead(context);

	return std::move(bind_data);
}

//===--------------------------------------------------------------------===//
// Helper writing functions
//===--------------------------------------------------------------------===//
static string AddEscapes(char to_be_escaped, const char escape, const string &val) {
	idx_t i = 0;
	string new_val = "";
	idx_t found = val.find(to_be_escaped);

	while (found != string::npos) {
		while (i < found) {
			new_val += val[i];
			i++;
		}
		if (escape != '\0') {
			new_val += escape;
			found = val.find(to_be_escaped, found + 1);
		}
	}
	while (i < val.length()) {
		new_val += val[i];
		i++;
	}
	return new_val;
}

static bool RequiresQuotes(WriteCSVData &csv_data, const char *str, idx_t len) {
	auto &options = csv_data.options;
	// check if the string is equal to the null string
	if (len == options.null_str[0].size() && memcmp(str, options.null_str[0].c_str(), len) == 0) {
		return true;
	}
	auto str_data = reinterpret_cast<const_data_ptr_t>(str);
	for (idx_t i = 0; i < len; i++) {
		if (csv_data.requires_quotes[str_data[i]]) {
			// this byte requires quotes - write a quoted string
			return true;
		}
	}
	// no newline, quote or delimiter in the string
	// no quoting or escaping necessary
	return false;
}

static void WriteQuotedString(WriteStream &writer, WriteCSVData &csv_data, const char *str, idx_t len,
                              bool force_quote) {
	auto &options = csv_data.options;
	if (!force_quote) {
		// force quote is disabled: check if we need to add quotes anyway
		force_quote = RequiresQuotes(csv_data, str, len);
	}
	// If a quote is set to none (i.e., null-terminator) we skip the quotation
	if (force_quote && options.dialect_options.state_machine_options.quote.GetValue() != '\0') {
		// quoting is enabled: we might need to escape things in the string
		bool requires_escape = false;
		// simple CSV
		// do a single loop to check for a quote or escape value
		for (idx_t i = 0; i < len; i++) {
			if (str[i] == options.dialect_options.state_machine_options.quote.GetValue() ||
			    str[i] == options.dialect_options.state_machine_options.escape.GetValue()) {
				requires_escape = true;
				break;
			}
		}

		if (!requires_escape) {
			// fast path: no need to escape anything
			WriteQuoteOrEscape(writer, options.dialect_options.state_machine_options.quote.GetValue());
			writer.WriteData(const_data_ptr_cast(str), len);
			WriteQuoteOrEscape(writer, options.dialect_options.state_machine_options.quote.GetValue());
			return;
		}

		// slow path: need to add escapes
		string new_val(str, len);
		new_val = AddEscapes(options.dialect_options.state_machine_options.escape.GetValue(),
		                     options.dialect_options.state_machine_options.escape.GetValue(), new_val);
		if (options.dialect_options.state_machine_options.escape !=
		    options.dialect_options.state_machine_options.quote) {
			// need to escape quotes separately
			new_val = AddEscapes(options.dialect_options.state_machine_options.quote.GetValue(),
			                     options.dialect_options.state_machine_options.escape.GetValue(), new_val);
		}
		WriteQuoteOrEscape(writer, options.dialect_options.state_machine_options.quote.GetValue());
		writer.WriteData(const_data_ptr_cast(new_val.c_str()), new_val.size());
		WriteQuoteOrEscape(writer, options.dialect_options.state_machine_options.quote.GetValue());
	} else {
		writer.WriteData(const_data_ptr_cast(str), len);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct LocalWriteCSVData : public LocalFunctionData {
public:
	LocalWriteCSVData(ClientContext &context, vector<unique_ptr<Expression>> &expressions)
	    : executor(context, expressions) {
	}

public:
	//! Used to execute the expressions that transform input -> string
	ExpressionExecutor executor;
	//! The thread-local buffer to write data into
	MemoryStream stream;
	//! A chunk with VARCHAR columns to cast intermediates into
	DataChunk cast_chunk;
	//! If we've written any rows yet, allows us to prevent a trailing comma when writing JSON ARRAY
	bool written_anything = false;
};

struct GlobalWriteCSVData : public GlobalFunctionData {
	GlobalWriteCSVData(FileSystem &fs, const string &file_path, FileCompressionType compression)
	    : fs(fs), written_anything(false) {
		handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW |
		                                    FileLockType::WRITE_LOCK | compression);
	}

	//! Write generic data, e.g., CSV header
	void WriteData(const_data_ptr_t data, idx_t size) {
		lock_guard<mutex> flock(lock);
		handle->Write((void *)data, size);
	}

	void WriteData(const char *data, idx_t size) {
		WriteData(const_data_ptr_cast(data), size);
	}

	//! Write rows
	void WriteRows(const_data_ptr_t data, idx_t size, const string &newline) {
		lock_guard<mutex> flock(lock);
		if (written_anything) {
			handle->Write((void *)newline.c_str(), newline.length());
		} else {
			written_anything = true;
		}
		handle->Write((void *)data, size);
	}

	idx_t FileSize() {
		lock_guard<mutex> flock(lock);
		return handle->GetFileSize();
	}

	FileSystem &fs;
	//! The mutex for writing to the physical file
	mutex lock;
	//! The file handle to write to
	unique_ptr<FileHandle> handle;
	//! If we've written any rows yet, allows us to prevent a trailing comma when writing JSON ARRAY
	bool written_anything;
};

static unique_ptr<LocalFunctionData> WriteCSVInitializeLocal(ExecutionContext &context, FunctionData &bind_data) {
	auto &csv_data = bind_data.Cast<WriteCSVData>();
	auto local_data = make_uniq<LocalWriteCSVData>(context.client, csv_data.cast_expressions);

	// create the chunk with VARCHAR types
	vector<LogicalType> types;
	types.resize(csv_data.options.name_list.size(), LogicalType::VARCHAR);

	local_data->cast_chunk.Initialize(Allocator::Get(context.client), types);
	return std::move(local_data);
}

static unique_ptr<GlobalFunctionData> WriteCSVInitializeGlobal(ClientContext &context, FunctionData &bind_data,
                                                               const string &file_path) {
	auto &csv_data = bind_data.Cast<WriteCSVData>();
	auto &options = csv_data.options;
	auto global_data =
	    make_uniq<GlobalWriteCSVData>(FileSystem::GetFileSystem(context), file_path, options.compression);

	if (!options.prefix.empty()) {
		global_data->WriteData(options.prefix.c_str(), options.prefix.size());
	}

	if (!(options.dialect_options.header.IsSetByUser() && !options.dialect_options.header.GetValue())) {
		MemoryStream stream;
		// write the header line to the file
		for (idx_t i = 0; i < csv_data.options.name_list.size(); i++) {
			if (i != 0) {
				WriteQuoteOrEscape(stream, options.dialect_options.state_machine_options.delimiter.GetValue());
			}
			WriteQuotedString(stream, csv_data, csv_data.options.name_list[i].c_str(),
			                  csv_data.options.name_list[i].size(), false);
		}
		stream.WriteData(const_data_ptr_cast(csv_data.newline.c_str()), csv_data.newline.size());

		global_data->WriteData(stream.GetData(), stream.GetPosition());
	}

	return std::move(global_data);
}

static void WriteCSVChunkInternal(ClientContext &context, FunctionData &bind_data, DataChunk &cast_chunk,
                                  MemoryStream &writer, DataChunk &input, bool &written_anything,
                                  ExpressionExecutor &executor) {
	auto &csv_data = bind_data.Cast<WriteCSVData>();
	auto &options = csv_data.options;

	// first cast the columns of the chunk to varchar
	cast_chunk.Reset();
	cast_chunk.SetCardinality(input);

	executor.Execute(input, cast_chunk);

	cast_chunk.Flatten();
	// now loop over the vectors and output the values
	for (idx_t row_idx = 0; row_idx < cast_chunk.size(); row_idx++) {
		if (row_idx == 0 && !written_anything) {
			written_anything = true;
		} else {
			writer.WriteData(const_data_ptr_cast(csv_data.newline.c_str()), csv_data.newline.size());
		}
		// write values
		D_ASSERT(options.null_str.size() == 1);
		for (idx_t col_idx = 0; col_idx < cast_chunk.ColumnCount(); col_idx++) {
			if (col_idx != 0) {
				WriteQuoteOrEscape(writer, options.dialect_options.state_machine_options.delimiter.GetValue());
			}
			if (FlatVector::IsNull(cast_chunk.data[col_idx], row_idx)) {
				// write null value
				writer.WriteData(const_data_ptr_cast(options.null_str[0].c_str()), options.null_str[0].size());
				continue;
			}

			// non-null value, fetch the string value from the cast chunk
			auto str_data = FlatVector::GetData<string_t>(cast_chunk.data[col_idx]);
			// FIXME: we could gain some performance here by checking for certain types if they ever require quotes
			// (e.g. integers only require quotes if the delimiter is a number, decimals only require quotes if the
			// delimiter is a number or "." character)
			WriteQuotedString(writer, csv_data, str_data[row_idx].GetData(), str_data[row_idx].GetSize(),
			                  csv_data.options.force_quote[col_idx]);
		}
	}
}

static void WriteCSVSink(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                         LocalFunctionData &lstate, DataChunk &input) {
	auto &csv_data = bind_data.Cast<WriteCSVData>();
	auto &local_data = lstate.Cast<LocalWriteCSVData>();
	auto &global_state = gstate.Cast<GlobalWriteCSVData>();

	// write data into the local buffer
	WriteCSVChunkInternal(context.client, bind_data, local_data.cast_chunk, local_data.stream, input,
	                      local_data.written_anything, local_data.executor);

	// check if we should flush what we have currently written
	auto &writer = local_data.stream;
	if (writer.GetPosition() >= csv_data.flush_size) {
		global_state.WriteRows(writer.GetData(), writer.GetPosition(), csv_data.newline);
		writer.Rewind();
		local_data.written_anything = false;
	}
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
static void WriteCSVCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                            LocalFunctionData &lstate) {
	auto &local_data = lstate.Cast<LocalWriteCSVData>();
	auto &global_state = gstate.Cast<GlobalWriteCSVData>();
	auto &csv_data = bind_data.Cast<WriteCSVData>();
	auto &writer = local_data.stream;
	// flush the local writer
	if (local_data.written_anything) {
		global_state.WriteRows(writer.GetData(), writer.GetPosition(), csv_data.newline);
		writer.Rewind();
	}
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
void WriteCSVFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &global_state = gstate.Cast<GlobalWriteCSVData>();
	auto &csv_data = bind_data.Cast<WriteCSVData>();
	auto &options = csv_data.options;

	MemoryStream stream;
	if (!options.suffix.empty()) {
		stream.WriteData(const_data_ptr_cast(options.suffix.c_str()), options.suffix.size());
	} else if (global_state.written_anything) {
		stream.WriteData(const_data_ptr_cast(csv_data.newline.c_str()), csv_data.newline.size());
	}
	global_state.WriteData(stream.GetData(), stream.GetPosition());

	global_state.handle->Close();
	global_state.handle.reset();
}

//===--------------------------------------------------------------------===//
// Execution Mode
//===--------------------------------------------------------------------===//
CopyFunctionExecutionMode WriteCSVExecutionMode(bool preserve_insertion_order, bool supports_batch_index) {
	if (!preserve_insertion_order) {
		return CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
	}
	if (supports_batch_index) {
		return CopyFunctionExecutionMode::BATCH_COPY_TO_FILE;
	}
	return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}
//===--------------------------------------------------------------------===//
// Prepare Batch
//===--------------------------------------------------------------------===//
struct WriteCSVBatchData : public PreparedBatchData {
	//! The thread-local buffer to write data into
	MemoryStream stream;
};

unique_ptr<PreparedBatchData> WriteCSVPrepareBatch(ClientContext &context, FunctionData &bind_data,
                                                   GlobalFunctionData &gstate,
                                                   unique_ptr<ColumnDataCollection> collection) {
	auto &csv_data = bind_data.Cast<WriteCSVData>();

	// create the cast chunk with VARCHAR types
	vector<LogicalType> types;
	types.resize(csv_data.options.name_list.size(), LogicalType::VARCHAR);
	DataChunk cast_chunk;
	cast_chunk.Initialize(Allocator::Get(context), types);

	auto &original_types = collection->Types();
	auto expressions = CreateCastExpressions(csv_data, context, csv_data.options.name_list, original_types);
	ExpressionExecutor executor(context, expressions);

	// write CSV chunks to the batch data
	bool written_anything = false;
	auto batch = make_uniq<WriteCSVBatchData>();
	for (auto &chunk : collection->Chunks()) {
		WriteCSVChunkInternal(context, bind_data, cast_chunk, batch->stream, chunk, written_anything, executor);
	}
	return std::move(batch);
}

//===--------------------------------------------------------------------===//
// Flush Batch
//===--------------------------------------------------------------------===//
void WriteCSVFlushBatch(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                        PreparedBatchData &batch) {
	auto &csv_batch = batch.Cast<WriteCSVBatchData>();
	auto &global_state = gstate.Cast<GlobalWriteCSVData>();
	auto &csv_data = bind_data.Cast<WriteCSVData>();
	auto &writer = csv_batch.stream;
	global_state.WriteRows(writer.GetData(), writer.GetPosition(), csv_data.newline);
	writer.Rewind();
}

//===--------------------------------------------------------------------===//
// File rotation
//===--------------------------------------------------------------------===//
bool WriteCSVRotateFiles(FunctionData &, const optional_idx &file_size_bytes) {
	return file_size_bytes.IsValid();
}

bool WriteCSVRotateNextFile(GlobalFunctionData &gstate, FunctionData &, const optional_idx &file_size_bytes) {
	auto &global_state = gstate.Cast<GlobalWriteCSVData>();
	return global_state.FileSize() > file_size_bytes.GetIndex();
}

void CSVCopyFunction::RegisterFunction(BuiltinFunctions &set) {
	CopyFunction info("csv");
	info.copy_to_bind = WriteCSVBind;
	info.copy_to_initialize_local = WriteCSVInitializeLocal;
	info.copy_to_initialize_global = WriteCSVInitializeGlobal;
	info.copy_to_sink = WriteCSVSink;
	info.copy_to_combine = WriteCSVCombine;
	info.copy_to_finalize = WriteCSVFinalize;
	info.execution_mode = WriteCSVExecutionMode;
	info.prepare_batch = WriteCSVPrepareBatch;
	info.flush_batch = WriteCSVFlushBatch;
	info.rotate_files = WriteCSVRotateFiles;
	info.rotate_next_file = WriteCSVRotateNextFile;

	info.copy_from_bind = ReadCSVBind;
	info.copy_from_function = ReadCSVTableFunction::GetFunction();

	info.extension = "csv";

	set.AddFunction(info);
}

} // namespace duckdb
