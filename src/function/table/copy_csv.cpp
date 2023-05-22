#include "duckdb/common/bind_helpers.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

#include <limits>

namespace duckdb {

void SubstringDetection(string &str_1, string &str_2, const string &name_str_1, const string &name_str_2) {
	if (str_1.empty() || str_2.empty()) {
		return;
	}
	if ((str_1.find(str_2) != string::npos || str_2.find(str_1) != std::string::npos)) {
		throw BinderException("%s must not appear in the %s specification and vice versa", name_str_1, name_str_2);
	}
}

//===--------------------------------------------------------------------===//
// Bind
//===--------------------------------------------------------------------===//

void BaseCSVData::Finalize() {
	// verify that the options are correct in the final pass
	if (options.escape.empty()) {
		options.escape = options.quote;
	}
	// escape and delimiter must not be substrings of each other
	if (options.has_delimiter && options.has_escape) {
		SubstringDetection(options.delimiter, options.escape, "DELIMITER", "ESCAPE");
	}
	// delimiter and quote must not be substrings of each other
	if (options.has_quote && options.has_delimiter) {
		SubstringDetection(options.quote, options.delimiter, "DELIMITER", "QUOTE");
	}
	// escape and quote must not be substrings of each other (but can be the same)
	if (options.quote != options.escape && options.has_quote && options.has_escape) {
		SubstringDetection(options.quote, options.escape, "QUOTE", "ESCAPE");
	}
	if (!options.null_str.empty()) {
		// null string and delimiter must not be substrings of each other
		if (options.has_delimiter) {
			SubstringDetection(options.delimiter, options.null_str, "DELIMITER", "NULL");
		}
		// quote/escape and nullstr must not be substrings of each other
		if (options.has_quote) {
			SubstringDetection(options.quote, options.null_str, "QUOTE", "NULL");
		}
		if (options.has_escape) {
			SubstringDetection(options.escape, options.null_str, "ESCAPE", "NULL");
		}
	}

	if (!options.prefix.empty() || !options.suffix.empty()) {
		if (options.prefix.empty() || options.suffix.empty()) {
			throw BinderException("COPY ... (FORMAT CSV) must have both PREFIX and SUFFIX, or none at all");
		}
		if (options.header) {
			throw BinderException("COPY ... (FORMAT CSV)'s HEADER cannot be combined with PREFIX/SUFFIX");
		}
	}
}

static unique_ptr<FunctionData> WriteCSVBind(ClientContext &context, CopyInfo &info, vector<string> &names,
                                             vector<LogicalType> &sql_types) {
	auto bind_data = make_uniq<WriteCSVData>(info.file_path, sql_types, names);

	// check all the options in the copy info
	for (auto &option : info.options) {
		auto loption = StringUtil::Lower(option.first);
		auto &set = option.second;
		bind_data->options.SetWriteOption(loption, ConvertVectorToValue(std::move(set)));
	}
	// verify the parsed options
	if (bind_data->options.force_quote.empty()) {
		// no FORCE_QUOTE specified: initialize to false
		bind_data->options.force_quote.resize(names.size(), false);
	}
	bind_data->Finalize();
	bind_data->is_simple = bind_data->options.delimiter.size() == 1 && bind_data->options.escape.size() == 1 &&
	                       bind_data->options.quote.size() == 1;
	if (bind_data->is_simple) {
		bind_data->requires_quotes = make_unsafe_uniq_array<bool>(256);
		memset(bind_data->requires_quotes.get(), 0, sizeof(bool) * 256);
		bind_data->requires_quotes['\n'] = true;
		bind_data->requires_quotes['\r'] = true;
		bind_data->requires_quotes[bind_data->options.delimiter[0]] = true;
		bind_data->requires_quotes[bind_data->options.quote[0]] = true;
	}
	if (!bind_data->options.write_newline.empty()) {
		bind_data->newline = bind_data->options.write_newline;
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
	bind_data->files = MultiFileReader::GetFileList(context, Value(info.file_path), "CSV");

	auto &options = bind_data->options;

	// check all the options in the copy info
	for (auto &option : info.options) {
		auto loption = StringUtil::Lower(option.first);
		auto &set = option.second;
		options.SetReadOption(loption, ConvertVectorToValue(std::move(set)), expected_names);
	}
	// verify the parsed options
	if (options.force_not_null.empty()) {
		// no FORCE_QUOTE specified: initialize to false
		options.force_not_null.resize(expected_types.size(), false);
	}
	bind_data->FinalizeRead(context);
	if (!bind_data->single_threaded && options.auto_detect) {
		options.file_path = bind_data->files[0];
		options.name_list = expected_names;
		auto initial_reader = make_uniq<BufferedCSVReader>(context, options, expected_types);
		options = initial_reader->options;
	}
	return std::move(bind_data);
}

//===--------------------------------------------------------------------===//
// Helper writing functions
//===--------------------------------------------------------------------===//
static string AddEscapes(string &to_be_escaped, const string &escape, const string &val) {
	idx_t i = 0;
	string new_val = "";
	idx_t found = val.find(to_be_escaped);

	while (found != string::npos) {
		while (i < found) {
			new_val += val[i];
			i++;
		}
		new_val += escape;
		found = val.find(to_be_escaped, found + escape.length());
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
	if (len == options.null_str.size() && memcmp(str, options.null_str.c_str(), len) == 0) {
		return true;
	}
	if (csv_data.is_simple) {
		// simple CSV: check for newlines, quotes and delimiter all at once
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
	} else {
		// CSV with complex quotes/delimiter (multiple bytes)

		// first check for \n, \r, \n\r in string
		for (idx_t i = 0; i < len; i++) {
			if (str[i] == '\n' || str[i] == '\r') {
				// newline, write a quoted string
				return true;
			}
		}

		// check for delimiter
		if (options.delimiter.length() != 0 &&
		    ContainsFun::Find((const unsigned char *)str, len, (const unsigned char *)options.delimiter.c_str(),
		                      options.delimiter.size()) != DConstants::INVALID_INDEX) {
			return true;
		}
		// check for quote
		if (options.quote.length() != 0 &&
		    ContainsFun::Find((const unsigned char *)str, len, (const unsigned char *)options.quote.c_str(),
		                      options.quote.size()) != DConstants::INVALID_INDEX) {
			return true;
		}
		return false;
	}
}

static void WriteQuotedString(Serializer &serializer, WriteCSVData &csv_data, const char *str, idx_t len,
                              bool force_quote) {
	auto &options = csv_data.options;
	if (!force_quote) {
		// force quote is disabled: check if we need to add quotes anyway
		force_quote = RequiresQuotes(csv_data, str, len);
	}
	if (force_quote) {
		// quoting is enabled: we might need to escape things in the string
		bool requires_escape = false;
		if (csv_data.is_simple) {
			// simple CSV
			// do a single loop to check for a quote or escape value
			for (idx_t i = 0; i < len; i++) {
				if (str[i] == options.quote[0] || str[i] == options.escape[0]) {
					requires_escape = true;
					break;
				}
			}
		} else {
			// complex CSV
			// check for quote or escape separately
			if (options.quote.length() != 0 &&
			    ContainsFun::Find((const unsigned char *)str, len, (const unsigned char *)options.quote.c_str(),
			                      options.quote.size()) != DConstants::INVALID_INDEX) {
				requires_escape = true;
			} else if (options.escape.length() != 0 &&
			           ContainsFun::Find((const unsigned char *)str, len, (const unsigned char *)options.escape.c_str(),
			                             options.escape.size()) != DConstants::INVALID_INDEX) {
				requires_escape = true;
			}
		}
		if (!requires_escape) {
			// fast path: no need to escape anything
			serializer.WriteBufferData(options.quote);
			serializer.WriteData((const_data_ptr_t)str, len);
			serializer.WriteBufferData(options.quote);
			return;
		}

		// slow path: need to add escapes
		string new_val(str, len);
		new_val = AddEscapes(options.escape, options.escape, new_val);
		if (options.escape != options.quote) {
			// need to escape quotes separately
			new_val = AddEscapes(options.quote, options.escape, new_val);
		}
		serializer.WriteBufferData(options.quote);
		serializer.WriteBufferData(new_val);
		serializer.WriteBufferData(options.quote);
	} else {
		serializer.WriteData((const_data_ptr_t)str, len);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct LocalWriteCSVData : public LocalFunctionData {
	//! The thread-local buffer to write data into
	BufferedSerializer serializer;
	//! A chunk with VARCHAR columns to cast intermediates into
	DataChunk cast_chunk;
	//! If we've written any rows yet, allows us to prevent a trailing comma when writing JSON ARRAY
	bool written_anything = false;
};

struct GlobalWriteCSVData : public GlobalFunctionData {
	GlobalWriteCSVData(FileSystem &fs, const string &file_path, FileCompressionType compression)
	    : fs(fs), written_anything(false) {
		handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW,
		                     FileLockType::WRITE_LOCK, compression);
	}

	//! Write generic data, e.g., CSV header
	void WriteData(const_data_ptr_t data, idx_t size) {
		lock_guard<mutex> flock(lock);
		handle->Write((void *)data, size);
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
	auto local_data = make_uniq<LocalWriteCSVData>();

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
		global_data->WriteData((const_data_ptr_t)options.prefix.c_str(), options.prefix.size());
	}

	if (options.header) {
		BufferedSerializer serializer;
		// write the header line to the file
		for (idx_t i = 0; i < csv_data.options.name_list.size(); i++) {
			if (i != 0) {
				serializer.WriteBufferData(options.delimiter);
			}
			WriteQuotedString(serializer, csv_data, csv_data.options.name_list[i].c_str(),
			                  csv_data.options.name_list[i].size(), false);
		}
		serializer.WriteBufferData(csv_data.newline);

		global_data->WriteData(serializer.blob.data.get(), serializer.blob.size);
	}

	return std::move(global_data);
}

static void WriteCSVChunkInternal(ClientContext &context, FunctionData &bind_data, DataChunk &cast_chunk,
                                  BufferedSerializer &writer, DataChunk &input, bool &written_anything) {
	auto &csv_data = bind_data.Cast<WriteCSVData>();
	auto &options = csv_data.options;

	// first cast the columns of the chunk to varchar
	cast_chunk.Reset();
	cast_chunk.SetCardinality(input);
	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		if (csv_data.sql_types[col_idx].id() == LogicalTypeId::VARCHAR) {
			// VARCHAR, just reinterpret (cannot reference, because LogicalTypeId::VARCHAR is used by the JSON type too)
			cast_chunk.data[col_idx].Reinterpret(input.data[col_idx]);
		} else if (options.has_format[LogicalTypeId::DATE] && csv_data.sql_types[col_idx].id() == LogicalTypeId::DATE) {
			// use the date format to cast the chunk
			csv_data.options.write_date_format[LogicalTypeId::DATE].ConvertDateVector(
			    input.data[col_idx], cast_chunk.data[col_idx], input.size());
		} else if (options.has_format[LogicalTypeId::TIMESTAMP] &&
		           (csv_data.sql_types[col_idx].id() == LogicalTypeId::TIMESTAMP ||
		            csv_data.sql_types[col_idx].id() == LogicalTypeId::TIMESTAMP_TZ)) {
			// use the timestamp format to cast the chunk
			csv_data.options.write_date_format[LogicalTypeId::TIMESTAMP].ConvertTimestampVector(
			    input.data[col_idx], cast_chunk.data[col_idx], input.size());
		} else {
			// non varchar column, perform the cast
			VectorOperations::Cast(context, input.data[col_idx], cast_chunk.data[col_idx], input.size());
		}
	}

	cast_chunk.Flatten();
	// now loop over the vectors and output the values
	for (idx_t row_idx = 0; row_idx < cast_chunk.size(); row_idx++) {
		if (row_idx == 0 && !written_anything) {
			written_anything = true;
		} else {
			writer.WriteBufferData(csv_data.newline);
		}
		// write values
		for (idx_t col_idx = 0; col_idx < cast_chunk.ColumnCount(); col_idx++) {
			if (col_idx != 0) {
				writer.WriteBufferData(options.delimiter);
			}
			if (FlatVector::IsNull(cast_chunk.data[col_idx], row_idx)) {
				// write null value
				writer.WriteBufferData(options.null_str);
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
	WriteCSVChunkInternal(context.client, bind_data, local_data.cast_chunk, local_data.serializer, input,
	                      local_data.written_anything);

	// check if we should flush what we have currently written
	auto &writer = local_data.serializer;
	if (writer.blob.size >= csv_data.flush_size) {
		global_state.WriteRows(writer.blob.data.get(), writer.blob.size, csv_data.newline);
		writer.Reset();
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
	auto &writer = local_data.serializer;
	// flush the local writer
	if (local_data.written_anything) {
		global_state.WriteRows(writer.blob.data.get(), writer.blob.size, csv_data.newline);
		writer.Reset();
	}
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
void WriteCSVFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &global_state = gstate.Cast<GlobalWriteCSVData>();
	auto &csv_data = bind_data.Cast<WriteCSVData>();
	auto &options = csv_data.options;

	BufferedSerializer serializer;
	if (!options.suffix.empty()) {
		serializer.WriteBufferData(options.suffix);
	} else if (global_state.written_anything) {
		serializer.WriteBufferData(csv_data.newline);
	}
	global_state.WriteData(serializer.blob.data.get(), serializer.blob.size);

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
	BufferedSerializer serializer;
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

	// write CSV chunks to the batch data
	bool written_anything = false;
	auto batch = make_uniq<WriteCSVBatchData>();
	for (auto &chunk : collection->Chunks()) {
		WriteCSVChunkInternal(context, bind_data, cast_chunk, batch->serializer, chunk, written_anything);
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
	auto &writer = csv_batch.serializer;
	global_state.WriteRows(writer.blob.data.get(), writer.blob.size, csv_data.newline);
	writer.Reset();
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

	info.copy_from_bind = ReadCSVBind;
	info.copy_from_function = ReadCSVTableFunction::GetFunction();

	info.extension = "csv";

	set.AddFunction(info);
}

} // namespace duckdb
