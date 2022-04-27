#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include <limits>

namespace duckdb {

void SubstringDetection(string &str_1, string &str_2, const string &name_str_1, const string &name_str_2) {
	if (str_1.empty() || str_2.empty()) {
		return;
	}
	if (str_1.find(str_2) != string::npos || str_2.find(str_1) != std::string::npos) {
		throw BinderException("%s must not appear in the %s specification and vice versa", name_str_1, name_str_2);
	}
}

static bool ParseBoolean(vector<Value> &set) {
	if (set.empty()) {
		// no option specified: default to true
		return true;
	}
	if (set.size() > 1) {
		throw BinderException("Expected a single argument as a boolean value (e.g. TRUE or 1)");
	}
	if (set[0].type() == LogicalType::FLOAT || set[0].type() == LogicalType::DOUBLE ||
	    set[0].type().id() == LogicalTypeId::DECIMAL) {
		throw BinderException("Expected a boolean value (e.g. TRUE or 1)");
	}
	return BooleanValue::Get(set[0].CastAs(LogicalType::BOOLEAN));
}

static string ParseString(vector<Value> &set) {
	if (set.size() != 1) {
		// no option specified or multiple options specified
		throw BinderException("Expected a single argument as a string value");
	}
	if (set[0].type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("Expected a string argument!");
	}
	return set[0].GetValue<string>();
}

static int64_t ParseInteger(vector<Value> &set) {
	if (set.size() != 1) {
		// no option specified or multiple options specified
		throw BinderException("Expected a single argument as a integer value");
	}
	return set[0].GetValue<int64_t>();
}

//===--------------------------------------------------------------------===//
// Bind
//===--------------------------------------------------------------------===//
static bool ParseBaseOption(BufferedCSVReaderOptions &options, string &loption, vector<Value> &set) {
	if (StringUtil::StartsWith(loption, "delim") || StringUtil::StartsWith(loption, "sep")) {
		options.SetDelimiter(ParseString(set));
	} else if (loption == "quote") {
		options.quote = ParseString(set);
		options.has_quote = true;
	} else if (loption == "escape") {
		options.escape = ParseString(set);
		options.has_escape = true;
	} else if (loption == "header") {
		options.header = ParseBoolean(set);
		options.has_header = true;
	} else if (loption == "null") {
		options.null_str = ParseString(set);
	} else if (loption == "encoding") {
		auto encoding = StringUtil::Lower(ParseString(set));
		if (encoding != "utf8" && encoding != "utf-8") {
			throw BinderException("Copy is only supported for UTF-8 encoded files, ENCODING 'UTF-8'");
		}
	} else if (loption == "compression") {
		options.compression = FileCompressionTypeFromString(ParseString(set));
	} else if (loption == "skip") {
		options.skip_rows = ParseInteger(set);
	} else if (loption == "max_line_size" || loption == "maximum_line_size") {
		options.maximum_line_size = ParseInteger(set);
	} else if (loption == "ignore_errors") {
		options.ignore_errors = ParseBoolean(set);
	} else {
		// unrecognized option in base CSV
		return false;
	}
	return true;
}

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
}

static vector<bool> ParseColumnList(vector<Value> &set, vector<string> &names) {
	vector<bool> result;
	if (set.empty()) {
		throw BinderException("Expected a column list or * as parameter");
	}
	if (set.size() == 1 && set[0].type().id() == LogicalTypeId::VARCHAR && set[0].GetValue<string>() == "*") {
		// *, force_not_null on all columns
		result.resize(names.size(), true);
	} else {
		// list of options: parse the list
		unordered_map<string, bool> option_map;
		for (idx_t i = 0; i < set.size(); i++) {
			option_map[set[i].ToString()] = false;
		}
		result.resize(names.size(), false);
		for (idx_t i = 0; i < names.size(); i++) {
			auto entry = option_map.find(names[i]);
			if (entry != option_map.end()) {
				result[i] = true;
				entry->second = true;
			}
		}
		for (auto &entry : option_map) {
			if (!entry.second) {
				throw BinderException("Column %s not found in table", entry.first.c_str());
			}
		}
	}
	return result;
}

static unique_ptr<FunctionData> WriteCSVBind(ClientContext &context, CopyInfo &info, vector<string> &names,
                                             vector<LogicalType> &sql_types) {
	auto bind_data = make_unique<WriteCSVData>(info.file_path, sql_types, names);

	// check all the options in the copy info
	for (auto &option : info.options) {
		auto loption = StringUtil::Lower(option.first);
		auto &set = option.second;
		if (ParseBaseOption(bind_data->options, loption, set)) {
			// parsed option in base CSV options: continue
			continue;
		} else if (loption == "force_quote") {
			bind_data->force_quote = ParseColumnList(set, names);
		} else {
			throw NotImplementedException("Unrecognized option for CSV: %s", option.first.c_str());
		}
	}
	// verify the parsed options
	if (bind_data->force_quote.empty()) {
		// no FORCE_QUOTE specified: initialize to false
		bind_data->force_quote.resize(names.size(), false);
	}
	bind_data->Finalize();
	bind_data->is_simple = bind_data->options.delimiter.size() == 1 && bind_data->options.escape.size() == 1 &&
	                       bind_data->options.quote.size() == 1;
	return move(bind_data);
}

static unique_ptr<FunctionData> ReadCSVBind(ClientContext &context, CopyInfo &info, vector<string> &expected_names,
                                            vector<LogicalType> &expected_types) {
	auto bind_data = make_unique<ReadCSVData>();
	bind_data->sql_types = expected_types;

	string file_pattern = info.file_path;

	auto &fs = FileSystem::GetFileSystem(context);
	bind_data->files = fs.Glob(file_pattern, context);
	if (bind_data->files.empty()) {
		throw IOException("No files found that match the pattern \"%s\"", file_pattern);
	}

	auto &options = bind_data->options;

	// check all the options in the copy info
	for (auto &option : info.options) {
		auto loption = StringUtil::Lower(option.first);
		auto &set = option.second;
		if (loption == "auto_detect") {
			options.auto_detect = ParseBoolean(set);
		} else if (ParseBaseOption(options, loption, set)) {
			// parsed option in base CSV options: continue
			continue;
		} else if (loption == "sample_size") {
			int64_t sample_size = ParseInteger(set);
			if (sample_size < 1 && sample_size != -1) {
				throw BinderException("Unsupported parameter for SAMPLE_SIZE: cannot be smaller than 1");
			}
			if (sample_size == -1) {
				options.sample_chunks = std::numeric_limits<uint64_t>::max();
				options.sample_chunk_size = STANDARD_VECTOR_SIZE;
			} else if (sample_size <= STANDARD_VECTOR_SIZE) {
				options.sample_chunk_size = sample_size;
				options.sample_chunks = 1;
			} else {
				options.sample_chunk_size = STANDARD_VECTOR_SIZE;
				options.sample_chunks = sample_size / STANDARD_VECTOR_SIZE;
			}
		} else if (loption == "sample_chunk_size") {
			options.sample_chunk_size = ParseInteger(set);
			if (options.sample_chunk_size > STANDARD_VECTOR_SIZE) {
				throw BinderException(
				    "Unsupported parameter for SAMPLE_CHUNK_SIZE: cannot be bigger than STANDARD_VECTOR_SIZE %d",
				    STANDARD_VECTOR_SIZE);
			} else if (options.sample_chunk_size < 1) {
				throw BinderException("Unsupported parameter for SAMPLE_CHUNK_SIZE: cannot be smaller than 1");
			}
		} else if (loption == "sample_chunks") {
			options.sample_chunks = ParseInteger(set);
			if (options.sample_chunks < 1) {
				throw BinderException("Unsupported parameter for SAMPLE_CHUNKS: cannot be smaller than 1");
			}
		} else if (loption == "force_not_null") {
			options.force_not_null = ParseColumnList(set, expected_names);
		} else if (loption == "date_format" || loption == "dateformat") {
			string format = ParseString(set);
			auto &date_format = options.date_format[LogicalTypeId::DATE];
			string error = StrTimeFormat::ParseFormatSpecifier(format, date_format);
			date_format.format_specifier = format;
			if (!error.empty()) {
				throw InvalidInputException("Could not parse DATEFORMAT: %s", error.c_str());
			}
			options.has_format[LogicalTypeId::DATE] = true;
		} else if (loption == "timestamp_format" || loption == "timestampformat") {
			string format = ParseString(set);
			auto &timestamp_format = options.date_format[LogicalTypeId::TIMESTAMP];
			string error = StrTimeFormat::ParseFormatSpecifier(format, timestamp_format);
			timestamp_format.format_specifier = format;
			if (!error.empty()) {
				throw InvalidInputException("Could not parse TIMESTAMPFORMAT: %s", error.c_str());
			}
			options.has_format[LogicalTypeId::TIMESTAMP] = true;
		} else {
			throw NotImplementedException("Unrecognized option for CSV: %s", option.first.c_str());
		}
	}
	// verify the parsed options
	if (options.force_not_null.empty()) {
		// no FORCE_QUOTE specified: initialize to false
		options.force_not_null.resize(expected_types.size(), false);
	}
	bind_data->Finalize();
	return move(bind_data);
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
		for (idx_t i = 0; i < len; i++) {
			if (str[i] == '\n' || str[i] == '\r' || str[i] == options.quote[0] || str[i] == options.delimiter[0]) {
				// newline, write a quoted string
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
		if (ContainsFun::Find((const unsigned char *)str, len, (const unsigned char *)options.delimiter.c_str(),
		                      options.delimiter.size()) != DConstants::INVALID_INDEX) {
			return true;
		}
		// check for quote
		if (ContainsFun::Find((const unsigned char *)str, len, (const unsigned char *)options.quote.c_str(),
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
			if (ContainsFun::Find((const unsigned char *)str, len, (const unsigned char *)options.quote.c_str(),
			                      options.quote.size()) != DConstants::INVALID_INDEX) {
				requires_escape = true;
			} else if (ContainsFun::Find((const unsigned char *)str, len, (const unsigned char *)options.escape.c_str(),
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
struct LocalReadCSVData : public LocalFunctionData {
	//! The thread-local buffer to write data into
	BufferedSerializer serializer;
	//! A chunk with VARCHAR columns to cast intermediates into
	DataChunk cast_chunk;
};

struct GlobalWriteCSVData : public GlobalFunctionData {
	GlobalWriteCSVData(FileSystem &fs, const string &file_path, FileOpener *opener, FileCompressionType compression)
	    : fs(fs) {
		handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW,
		                     FileLockType::WRITE_LOCK, compression, opener);
	}

	void WriteData(const_data_ptr_t data, idx_t size) {
		lock_guard<mutex> flock(lock);
		handle->Write((void *)data, size);
	}

	FileSystem &fs;
	//! The mutex for writing to the physical file
	mutex lock;
	//! The file handle to write to
	unique_ptr<FileHandle> handle;
};

static unique_ptr<LocalFunctionData> WriteCSVInitializeLocal(ClientContext &context, FunctionData &bind_data) {
	auto &csv_data = (WriteCSVData &)bind_data;
	auto local_data = make_unique<LocalReadCSVData>();

	// create the chunk with VARCHAR types
	vector<LogicalType> types;
	types.resize(csv_data.names.size(), LogicalType::VARCHAR);

	local_data->cast_chunk.Initialize(types);
	return move(local_data);
}

static unique_ptr<GlobalFunctionData> WriteCSVInitializeGlobal(ClientContext &context, FunctionData &bind_data,
                                                               const string &file_path) {
	auto &csv_data = (WriteCSVData &)bind_data;
	auto &options = csv_data.options;
	auto global_data = make_unique<GlobalWriteCSVData>(FileSystem::GetFileSystem(context), file_path,
	                                                   FileSystem::GetFileOpener(context), options.compression);

	if (options.header) {
		BufferedSerializer serializer;
		// write the header line to the file
		for (idx_t i = 0; i < csv_data.names.size(); i++) {
			if (i != 0) {
				serializer.WriteBufferData(options.delimiter);
			}
			WriteQuotedString(serializer, csv_data, csv_data.names[i].c_str(), csv_data.names[i].size(), false);
		}
		serializer.WriteBufferData(csv_data.newline);

		global_data->WriteData(serializer.blob.data.get(), serializer.blob.size);
	}
	return move(global_data);
}

static void WriteCSVSink(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                         LocalFunctionData &lstate, DataChunk &input) {
	auto &csv_data = (WriteCSVData &)bind_data;
	auto &options = csv_data.options;
	auto &local_data = (LocalReadCSVData &)lstate;
	auto &global_state = (GlobalWriteCSVData &)gstate;

	// write data into the local buffer

	// first cast the columns of the chunk to varchar
	auto &cast_chunk = local_data.cast_chunk;
	cast_chunk.SetCardinality(input);
	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		if (csv_data.sql_types[col_idx].id() == LogicalTypeId::VARCHAR) {
			// VARCHAR, just create a reference
			cast_chunk.data[col_idx].Reference(input.data[col_idx]);
		} else {
			// non varchar column, perform the cast
			VectorOperations::Cast(input.data[col_idx], cast_chunk.data[col_idx], input.size());
		}
	}

	cast_chunk.Normalify();
	auto &writer = local_data.serializer;
	// now loop over the vectors and output the values
	for (idx_t row_idx = 0; row_idx < cast_chunk.size(); row_idx++) {
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
			auto str_value = str_data[row_idx];
			// FIXME: we could gain some performance here by checking for certain types if they ever require quotes
			// (e.g. integers only require quotes if the delimiter is a number, decimals only require quotes if the
			// delimiter is a number or "." character)
			WriteQuotedString(writer, csv_data, str_value.GetDataUnsafe(), str_value.GetSize(),
			                  csv_data.force_quote[col_idx]);
		}
		writer.WriteBufferData(csv_data.newline);
	}
	// check if we should flush what we have currently written
	if (writer.blob.size >= csv_data.flush_size) {
		global_state.WriteData(writer.blob.data.get(), writer.blob.size);
		writer.Reset();
	}
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
static void WriteCSVCombine(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                            LocalFunctionData &lstate) {
	auto &local_data = (LocalReadCSVData &)lstate;
	auto &global_state = (GlobalWriteCSVData &)gstate;
	auto &writer = local_data.serializer;
	// flush the local writer
	if (writer.blob.size > 0) {
		global_state.WriteData(writer.blob.data.get(), writer.blob.size);
		writer.Reset();
	}
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
void WriteCSVFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &global_state = (GlobalWriteCSVData &)gstate;

	global_state.handle->Close();
	global_state.handle.reset();
}

void CSVCopyFunction::RegisterFunction(BuiltinFunctions &set) {
	CopyFunction info("csv");
	info.copy_to_bind = WriteCSVBind;
	info.copy_to_initialize_local = WriteCSVInitializeLocal;
	info.copy_to_initialize_global = WriteCSVInitializeGlobal;
	info.copy_to_sink = WriteCSVSink;
	info.copy_to_combine = WriteCSVCombine;
	info.copy_to_finalize = WriteCSVFinalize;

	info.copy_from_bind = ReadCSVBind;
	info.copy_from_function = ReadCSVTableFunction::GetFunction();

	info.extension = "csv";

	set.AddFunction(info);
}

} // namespace duckdb
