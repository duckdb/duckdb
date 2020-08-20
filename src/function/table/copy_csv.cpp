#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

struct BaseCSVData : public FunctionData {
	BaseCSVData(string file_path) : file_path(move(file_path)) {
	}

	//! The file path of the CSV file to read or write
	string file_path;
	//! Whether or not a header information was given by the user
	bool has_header = false;
	//! Whether or not to write a header in the file
	bool header = false;
	//! Whether or not a delimiter was defined by the user
	bool has_delimiter = false;
	//! Delimiter to separate columns within each line
	string delimiter = ",";
	//! Whether or not a quote sign was defined by the user
	bool has_quote = false;
	//! Quote used for columns that contain reserved characters, e.g., delimiter
	string quote = "\"";
	//! Quote used for columns that contain reserved characters, e.g., delimiter
	bool has_escape = false;
	//! Escape character to escape quote charaƒcter
	string escape;
	//! Specifies the string that represents a null value
	string null_str;
	//! Whether or not the options are specified; if not we default to auto detect
	bool is_auto_detect = false;

	void Finalize();
};

struct WriteCSVData : public BaseCSVData {
	WriteCSVData(string file_path, vector<LogicalType> sql_types, vector<string> names)
	    : BaseCSVData(move(file_path)), sql_types(move(sql_types)), names(move(names)) {
	}

	//! The SQL types to write
	vector<LogicalType> sql_types;
	//! The column names of the columns to write
	vector<string> names;
	//! True, if column with that index must be quoted
	vector<bool> force_quote;
	//! The newline string to write
	string newline = "\n";
	//! Whether or not we are writing a simple CSV (delimiter, quote and escape are all 1 byte in length)
	bool is_simple;
	//! The size of the CSV file (in bytes) that we buffer before we flush it to disk
	idx_t flush_size = 4096 * 8;
};

struct ReadCSVData : public BaseCSVData {
	ReadCSVData(string file_path, vector<LogicalType> sql_types)
	    : BaseCSVData(move(file_path)), sql_types(move(sql_types)) {
	}

	//! The expected SQL types to read
	vector<LogicalType> sql_types;
	//! True, if column with that index must be quoted
	vector<bool> force_not_null;
	//! The size of a sample for format/data type detection (csv)
	int sample_size = DEFAULT_SAMPLE_CHUNK_SIZE;
	//! The number of samples for data type detection (csv)
	int num_samples = 10;
	//! The DATE_FORMAT to use to read or write dates
	StrpTimeFormat date_format;
	//! Whether or not there is a date format specified
	bool has_date_format = false;
	//! The DATE_FORMAT to use to read or write dates
	StrpTimeFormat timestamp_format;
	//! Whether or not there is a date format specified
	bool has_timestamp_format = false;
};

void SubstringDetection(string &str_1, string &str_2, string name_str_1, string name_str_2) {
	if (str_1.find(str_2) != string::npos || str_2.find(str_1) != std::string::npos) {
		throw BinderException("COPY " + name_str_1 + " must not appear in the " + name_str_2 +
		                      " specification and vice versa");
	}
}

static bool ParseBoolean(vector<Value> &set) {
	if (set.size() == 0) {
		// no option specified: default to true
		return true;
	}
	if (set.size() > 1) {
		throw BinderException("Expected a single argument as a boolean value (e.g. TRUE or 1)");
	}
	if (set[0].type() == LogicalType::FLOAT || set[0].type() == LogicalType::DOUBLE || set[0].type().id() == LogicalTypeId::DECIMAL) {
		throw BinderException("Expected a boolean value (e.g. TRUE or 1)");
	}
	return set[0].CastAs(LogicalType::BOOLEAN).value_.boolean;
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

static idx_t ParseInteger(vector<Value> &set) {
	if (set.size() != 1) {
		// no option specified or multiple options specified
		throw BinderException("Expected a single argument as a integer value");
	}
	if (set[0].type().id() == LogicalTypeId::FLOAT || set[0].type().id() == LogicalTypeId::DOUBLE || set[0].type().id() == LogicalTypeId::DECIMAL) {
		throw BinderException("Expected a integer argument!");
	}
	return set[0].GetValue<int64_t>();
}

//===--------------------------------------------------------------------===//
// Bind
//===--------------------------------------------------------------------===//
static bool ParseBaseOption(BaseCSVData &bind_data, string &loption, vector<Value> &set) {
	if (StringUtil::StartsWith(loption, "delim") || StringUtil::StartsWith(loption, "sep")) {
		bind_data.delimiter = ParseString(set);
		bind_data.has_delimiter = true;
		if (bind_data.delimiter.length() == 0) {
			throw BinderException("DELIM or SEP must not be empty");
		}
	} else if (loption == "quote") {
		bind_data.quote = ParseString(set);
		bind_data.has_quote = true;
		if (bind_data.quote.length() == 0) {
			throw BinderException("QUOTE must not be empty");
		}
	} else if (loption == "escape") {
		bind_data.escape = ParseString(set);
		bind_data.has_escape = true;
		if (bind_data.escape.length() == 0) {
			throw BinderException("ESCAPE must not be empty");
		}
	} else if (loption == "header") {
		bind_data.header = ParseBoolean(set);
		bind_data.has_header = true;
	} else if (loption == "null") {
		bind_data.null_str = ParseString(set);
	} else if (loption == "encoding") {
		auto encoding = StringUtil::Lower(ParseString(set));
		if (encoding != "utf8" && encoding != "utf-8") {
			throw BinderException("Copy is only supported for UTF-8 encoded files, ENCODING 'UTF-8'");
		}
	} else {
		// unrecognized option in base CSV
		return false;
	}
	return true;
}

void BaseCSVData::Finalize() {
	// verify that the options are correct in the final pass
	if (escape.empty()) {
		escape = quote;
	}
	// escape and delimiter must not be substrings of each other
	SubstringDetection(delimiter, escape, "DELIMITER", "ESCAPE");
	// delimiter and quote must not be substrings of each other
	SubstringDetection(quote, delimiter, "DELIMITER", "QUOTE");
	// escape and quote must not be substrings of each other (but can be the same)
	if (quote != escape) {
		SubstringDetection(quote, escape, "QUOTE", "ESCAPE");
	}
	if (null_str != "") {
		// null string and delimiter must not be substrings of each other
		SubstringDetection(delimiter, null_str, "DELIMITER", "NULL");
		// quote/escape and nullstr must not be substrings of each other
		SubstringDetection(quote, null_str, "QUOTE", "NULL");
		SubstringDetection(escape, null_str, "ESCAPE", "NULL");
	}
}

static vector<bool> ParseColumnList(vector<Value> &set, vector<string> &names) {
	vector<bool> result;
	if (set.size() == 0) {
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
		for (auto entry : option_map) {
			if (!entry.second) {
				throw BinderException("Column %s not found in table", entry.first.c_str());
			}
		}
	}
	return result;
}

static unique_ptr<FunctionData> write_csv_bind(ClientContext &context, CopyInfo &info, vector<string> &names,
                                               vector<LogicalType> &sql_types) {
	auto bind_data = make_unique<WriteCSVData>(info.file_path, sql_types, names);

	// check all the options in the copy info
	for (auto &option : info.options) {
		auto loption = StringUtil::Lower(option.first);
		auto &set = option.second;
		if (ParseBaseOption(*bind_data, loption, set)) {
			// parsed option in base CSV options: continue
			continue;
		} else if (loption == "force_quote") {
			bind_data->force_quote = ParseColumnList(set, names);
		} else {
			throw NotImplementedException("Unrecognized option for CSV: %s", option.first.c_str());
		}
	}
	// verify the parsed options
	if (bind_data->force_quote.size() == 0) {
		// no FORCE_QUOTE specified: initialize to false
		bind_data->force_quote.resize(names.size(), false);
	}
	bind_data->Finalize();
	bind_data->is_simple =
	    bind_data->delimiter.size() == 1 && bind_data->escape.size() == 1 && bind_data->quote.size() == 1;
	return move(bind_data);
}

static unique_ptr<FunctionData> read_csv_bind(ClientContext &context, CopyInfo &info, vector<string> &expected_names,
                                              vector<LogicalType> &expected_types) {
	auto bind_data = make_unique<ReadCSVData>(info.file_path, expected_types);

	// check all the options in the copy info
	for (auto &option : info.options) {
		auto loption = StringUtil::Lower(option.first);
		auto &set = option.second;
		if (loption == "auto_detect") {
			bind_data->is_auto_detect = ParseBoolean(set);
		} else if (ParseBaseOption(*bind_data, loption, set)) {
			// parsed option in base CSV options: continue
			continue;
		} else if (loption == "sample_size") {
			bind_data->sample_size = ParseInteger(set);
			if (bind_data->sample_size > STANDARD_VECTOR_SIZE) {
				throw BinderException(
				    "Unsupported parameter for SAMPLE_SIZE: cannot be bigger than STANDARD_VECTOR_SIZE %d",
				    STANDARD_VECTOR_SIZE);
			} else if (bind_data->sample_size < 1) {
				throw BinderException("Unsupported parameter for SAMPLE_SIZE: cannot be smaller than 1");
			}
		} else if (loption == "num_samples") {
			bind_data->num_samples = ParseInteger(set);
			if (bind_data->num_samples < 1) {
				throw BinderException("Unsupported parameter for NUM_SAMPLES: cannot be smaller than 1");
			}
		} else if (loption == "force_not_null") {
			bind_data->force_not_null = ParseColumnList(set, expected_names);
		} else if (loption == "date_format" || loption == "dateformat") {
			string format = ParseString(set);
			string error = StrTimeFormat::ParseFormatSpecifier(format, bind_data->date_format);
			bind_data->date_format.format_specifier = format;
			if (!error.empty()) {
				throw InvalidInputException("Could not parse DATEFORMAT: %s", error.c_str());
			}
			bind_data->has_date_format = true;
		} else if (loption == "timestamp_format" || loption == "timestampformat") {
			string format = ParseString(set);
			string error = StrTimeFormat::ParseFormatSpecifier(format, bind_data->timestamp_format);
			bind_data->timestamp_format.format_specifier = format;
			if (!error.empty()) {
				throw InvalidInputException("Could not parse TIMESTAMPFORMAT: %s", error.c_str());
			}
			bind_data->has_timestamp_format = true;
		} else {
			throw NotImplementedException("Unrecognized option for CSV: %s", option.first.c_str());
		}
	}
	// verify the parsed options
	if (bind_data->force_not_null.size() == 0) {
		// no FORCE_QUOTE specified: initialize to false
		bind_data->force_not_null.resize(expected_types.size(), false);
	}
	bind_data->Finalize();
	return move(bind_data);
}

//===--------------------------------------------------------------------===//
// Helper writing functions
//===--------------------------------------------------------------------===//
static string AddEscapes(string &to_be_escaped, string escape, string val) {
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

static bool RequiresQuotes(WriteCSVData &options, const char *str, idx_t len) {
	// check if the string is equal to the null string
	if (len == options.null_str.size() && memcmp(str, options.null_str.c_str(), len) == 0) {
		return true;
	}
	if (options.is_simple) {
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
		if (strstr(str, options.delimiter.c_str())) {
			return true;
		}
		// check for quote
		if (strstr(str, options.quote.c_str())) {
			return true;
		}
		return false;
	}
}

static void WriteQuotedString(Serializer &serializer, WriteCSVData &options, const char *str, idx_t len,
                              bool force_quote) {
	if (!force_quote) {
		// force quote is disabled: check if we need to add quotes anyway
		force_quote = RequiresQuotes(options, str, len);
	}
	if (force_quote) {
		// quoting is enabled: we might need to escape things in the string
		bool requires_escape = false;
		if (options.is_simple) {
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
			if (strstr(str, options.quote.c_str())) {
				requires_escape = true;
			} else if (strstr(str, options.escape.c_str())) {
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
	GlobalWriteCSVData(FileSystem &fs, string file_path) : fs(fs) {
		handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW,
		                     FileLockType::WRITE_LOCK);
	}

	void WriteData(const_data_ptr_t data, idx_t size) {
		lock_guard<mutex> flock(lock);
		fs.Write(*handle, (void *)data, size);
	}

	FileSystem &fs;
	//! The mutex for writing to the physical file
	mutex lock;
	//! The file handle to write to
	unique_ptr<FileHandle> handle;
};

static unique_ptr<LocalFunctionData> write_csv_initialize_local(ClientContext &context, FunctionData &bind_data) {
	auto &csv_data = (WriteCSVData &)bind_data;
	auto local_data = make_unique<LocalReadCSVData>();

	// create the chunk with VARCHAR types
	vector<LogicalType> types;
	types.resize(csv_data.names.size(), LogicalType::VARCHAR);

	local_data->cast_chunk.Initialize(types);
	return move(local_data);
}

static unique_ptr<GlobalFunctionData> write_csv_initialize_global(ClientContext &context, FunctionData &bind_data) {
	auto &csv_data = (WriteCSVData &)bind_data;
	auto global_data = make_unique<GlobalWriteCSVData>(FileSystem::GetFileSystem(context), csv_data.file_path);

	if (csv_data.header) {
		BufferedSerializer serializer;
		// write the header line to the file
		for (idx_t i = 0; i < csv_data.names.size(); i++) {
			if (i != 0) {
				serializer.WriteBufferData(csv_data.delimiter);
			}
			WriteQuotedString(serializer, csv_data, csv_data.names[i].c_str(), csv_data.names[i].size(), false);
		}
		serializer.WriteBufferData(csv_data.newline);

		global_data->WriteData(serializer.blob.data.get(), serializer.blob.size);
	}
	return move(global_data);
}

static void write_csv_sink(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                           LocalFunctionData &lstate, DataChunk &input) {
	auto &csv_data = (WriteCSVData &)bind_data;
	auto &local_data = (LocalReadCSVData &)lstate;
	auto &global_state = (GlobalWriteCSVData &)gstate;

	// write data into the local buffer

	// first cast the columns of the chunk to varchar
	auto &cast_chunk = local_data.cast_chunk;
	cast_chunk.SetCardinality(input);
	for (idx_t col_idx = 0; col_idx < input.column_count(); col_idx++) {
		if (csv_data.sql_types[col_idx].id() == LogicalTypeId::VARCHAR ||
		    csv_data.sql_types[col_idx].id() == LogicalTypeId::BLOB) {
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
		for (idx_t col_idx = 0; col_idx < cast_chunk.column_count(); col_idx++) {
			if (col_idx != 0) {
				writer.WriteBufferData(csv_data.delimiter);
			}
			if (FlatVector::IsNull(cast_chunk.data[col_idx], row_idx)) {
				// write null value
				writer.WriteBufferData(csv_data.null_str);
				continue;
			}

			// non-null value, fetch the string value from the cast chunk
			auto str_data = FlatVector::GetData<string_t>(cast_chunk.data[col_idx]);
			auto str_value = str_data[row_idx];
			// FIXME: we could gain some performance here by checking for certain types if they ever require quotes
			// (e.g. integers only require quotes if the delimiter is a number, decimals only require quotes if the
			// delimiter is a number or "." character)
			WriteQuotedString(writer, csv_data, str_value.GetData(), str_value.GetSize(),
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
static void write_csv_combine(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
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
// Read CSV
//===--------------------------------------------------------------------===//
struct GlobalReadCSVData : public GlobalFunctionData {
	unique_ptr<BufferedCSVReader> csv_reader;
};

unique_ptr<GlobalFunctionData> read_csv_initialize(ClientContext &context, FunctionData &fdata) {
	auto global_data = make_unique<GlobalReadCSVData>();
	auto &bind_data = (ReadCSVData &)fdata;

	// set up the CSV reader with the parsed options
	BufferedCSVReaderOptions options;
	options.file_path = bind_data.file_path;
	options.auto_detect = bind_data.is_auto_detect;
	options.has_delimiter = bind_data.has_delimiter;
	options.delimiter = bind_data.delimiter;
	options.has_quote = bind_data.has_quote;
	options.quote = bind_data.quote;
	options.has_escape = bind_data.has_escape;
	options.escape = bind_data.escape;
	options.has_header = bind_data.has_header;
	options.header = bind_data.header;
	options.null_str = bind_data.null_str;
	options.skip_rows = 0;
	options.num_cols = bind_data.sql_types.size();
	options.force_not_null = bind_data.force_not_null;
	options.sample_size = bind_data.sample_size;
	options.num_samples = bind_data.num_samples;
	options.has_date_format = bind_data.has_date_format;
	options.date_format = move(bind_data.date_format);
	options.has_timestamp_format = bind_data.has_timestamp_format;
	options.timestamp_format = move(bind_data.timestamp_format);

	global_data->csv_reader = make_unique<BufferedCSVReader>(context, move(options), bind_data.sql_types);
	return move(global_data);
}

void read_csv_get_chunk(ExecutionContext &context, GlobalFunctionData &gstate, FunctionData &bind_data,
                        DataChunk &chunk) {
	// read a chunk from the CSV reader
	auto &gdata = (GlobalReadCSVData &)gstate;
	gdata.csv_reader->ParseCSV(chunk);
}

void CSVCopyFunction::RegisterFunction(BuiltinFunctions &set) {
	CopyFunction info("csv");
	info.copy_to_bind = write_csv_bind;
	info.copy_to_initialize_local = write_csv_initialize_local;
	info.copy_to_initialize_global = write_csv_initialize_global;
	info.copy_to_sink = write_csv_sink;
	info.copy_to_combine = write_csv_combine;

	info.copy_from_bind = read_csv_bind;
	info.copy_from_initialize = read_csv_initialize;
	info.copy_from_get_chunk = read_csv_get_chunk;

	info.extension = "csv";

	set.AddFunction(info);
}

} // namespace duckdb
