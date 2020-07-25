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

struct WriteCSVData : public FunctionData {
	WriteCSVData(string file_path, vector<SQLType> sql_types, vector<string> names) :
		file_path(move(file_path)), sql_types(move(sql_types)), names(move(names)) {}

	string file_path;
	vector<SQLType> sql_types;
	vector<string> names;

	//! Whether or not to write a header in the file
	bool header = false;
	//! Delimiter to separate columns within each line
	string delimiter = ",";
	//! Quote used for columns that contain reserved characters, e.g., delimiter
	string quote = "\"";
	//! Escape character to escape quote character
	string escape;
	//! Specifies the string that represents a null value
	string null_str;
	//! True, if column with that index must be quoted
	vector<bool> force_quote;
	//! The newline string to write
	string newline = "\n";
	//! Whether or not we are writing a simple CSV (delimiter, quote and escape are all 1 byte in length)
	bool is_simple;
	//! The size of the CSV file (in bytes) that we buffer before we flush it to disk
	idx_t flush_size = 4096 * 8;
};

void SubstringDetection(string &str_1, string &str_2, string name_str_1, string name_str_2) {
	if (str_1.find(str_2) != string::npos || str_2.find(str_1) != std::string::npos) {
		throw Exception("COPY " + name_str_1 + " must not appear in the " + name_str_2 +
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
	return set[0].CastAs(TypeId::BOOL).value_.boolean;
}

static string ParseString(vector<Value> &set) {
	if (set.size() != 1) {
		// no option specified or multiple options specified
		throw BinderException("Expected a single argument as a string value");
	}
	return set[0].ToString();
}

//===--------------------------------------------------------------------===//
// Bind
//===--------------------------------------------------------------------===//
static unique_ptr<FunctionData> write_csv_bind(ClientContext &context, CopyInfo &info, vector<string> &names,
                                           vector<SQLType> &sql_types) {
	auto bind_data = make_unique<WriteCSVData>(info.file_path, sql_types, names);

	// check all the options in the copy info
	for(auto &option : info.options) {
		auto loption = StringUtil::Lower(option.first);
		auto &set = option.second;
		if (StringUtil::StartsWith(loption, "delim")) {
			bind_data->delimiter = ParseString(set);
			if (bind_data->delimiter.length() == 0) {
				throw Exception("QUOTE must not be empty");
			}
		} else if (loption == "quote") {
			bind_data->quote = ParseString(set);
			if (bind_data->quote.length() == 0) {
				throw Exception("QUOTE must not be empty");
			}
		} else if (loption == "escape") {
			bind_data->escape = ParseString(set);
			if (bind_data->escape.length() == 0) {
				throw Exception("ESCAPE must not be empty");
			}
		} else if (loption == "header") {
			bind_data->header = ParseBoolean(set);
		} else if (loption == "null") {
			bind_data->null_str = ParseString(set);
		} else if (loption == "force_quote") {
			if (set.size() == 1 && set[0].type == TypeId::VARCHAR && set[0].str_value == "*") {
				// *, quote all
				bind_data->force_quote.resize(names.size(), true);
			} else {
				// list of options: parse the list
				unordered_set<string> force_quoted_values;
				for(idx_t i = 0; i < set.size(); i++) {
					force_quoted_values.insert(set[i].ToString());
				}
				bind_data->force_quote.resize(names.size(), false);
				for(idx_t i = 0; i < names.size(); i++) {
					if (force_quoted_values.find(names[i]) != force_quoted_values.end()) {
						bind_data->force_quote[i] = true;
					}
				}
			}
		} else if (loption == "encoding") {
			auto encoding = StringUtil::Lower(ParseString(set));
			if (encoding != "utf8" && encoding != "utf-8") {
				throw Exception("Copy is only supported for UTF-8 encoded files, ENCODING 'UTF-8'");
			}
		} else {
			throw NotImplementedException("Unrecognized option for CSV: %s", option.first.c_str());
		}
	}
	// verify the parsed options
	if (bind_data->escape == "") {
		bind_data->escape = bind_data->quote;
	}
	if (bind_data->force_quote.size() == 0) {
		// no FORCE_QUOTE specified: initialize to false
		bind_data->force_quote.resize(names.size(), false);
	}
	// escape and delimiter must not be substrings of each other
	SubstringDetection(bind_data->delimiter, bind_data->escape, "DELIMITER", "ESCAPE");
	// delimiter and quote must not be substrings of each other
	SubstringDetection(bind_data->quote, bind_data->delimiter, "DELIMITER", "QUOTE");
	// escape and quote must not be substrings of each other (but can be the same)
	if (bind_data->quote != bind_data->escape) {
		SubstringDetection(bind_data->quote, bind_data->escape, "QUOTE", "ESCAPE");
	}
	// null string and delimiter must not be substrings of each other
	if (bind_data->null_str != "") {
		SubstringDetection(bind_data->delimiter, bind_data->null_str, "DELIMITER", "NULL");
	}
	bind_data->is_simple = bind_data->delimiter.size() == 1 && bind_data->escape.size() == 1 && bind_data->quote.size() == 1;
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
	if (len != options.null_str.size() && memcmp(str, options.null_str.c_str(), len) == 0) {
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

static void WriteQuotedString(Serializer &serializer, WriteCSVData &options, const char *str, idx_t len, bool force_quote) {
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
			// check for delimiter or escape separately
			if (strstr(str, options.delimiter.c_str())) {
				requires_escape = true;
			} else if (strstr(str, options.escape.c_str())) {
				requires_escape = true;
			}
		}
		if (!requires_escape) {
			// fast path: no need to escape anything
			serializer.WriteData(options.quote);
			serializer.WriteData((const_data_ptr_t) str, len);
			serializer.WriteData(options.quote);
			return;
		}

		// slow path: need to add escapes
		string new_val(str, len);
		new_val = AddEscapes(options.escape, options.escape, new_val);
		if (options.escape != options.quote) {
			// need to escape quotes separately
			new_val = AddEscapes(options.quote, options.escape, new_val);
		}
		serializer.WriteData(options.quote);
		serializer.WriteData(new_val);
		serializer.WriteData(options.quote);
	} else {
		serializer.WriteData((const_data_ptr_t) str, len);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct LocalCSVData : public LocalFunctionData {
	//! The thread-local buffer to write data into
	BufferedSerializer serializer;
	//! A chunk with VARCHAR columns to cast intermediates into
	DataChunk cast_chunk;
};

struct GlobalCSVData : public GlobalFunctionData {
	GlobalCSVData(FileSystem &fs, string file_path) : fs(fs) {
		handle = fs.OpenFile(file_path, FileFlags::WRITE | FileFlags::CREATE, FileLockType::WRITE_LOCK);
	}

	void WriteData(const_data_ptr_t data, idx_t size) {
		lock_guard<mutex> flock(lock);
		fs.Write(*handle, (void*) data, size);
	}

	FileSystem &fs;
	//! The mutex for writing to the physical file
	mutex lock;
	//! The file handle to write to
	unique_ptr<FileHandle> handle;
};

static unique_ptr<LocalFunctionData> write_csv_initialize_local(ClientContext &context, FunctionData &bind_data) {
	auto &csv_data = (WriteCSVData &) bind_data;
	auto local_data = make_unique<LocalCSVData>();

	// create the chunk with VARCHAR types
	vector<TypeId> types;
	types.resize(csv_data.names.size(), TypeId::VARCHAR);

	local_data->cast_chunk.Initialize(types);
	return move(local_data);
}

static unique_ptr<GlobalFunctionData> write_csv_initialize_global(ClientContext &context, FunctionData &bind_data) {
	auto &csv_data = (WriteCSVData &) bind_data;
	auto global_data =  make_unique<GlobalCSVData>(FileSystem::GetFileSystem(context), csv_data.file_path);

	if (csv_data.header) {
		BufferedSerializer serializer;
		// write the header line to the file
		for (idx_t i = 0; i < csv_data.names.size(); i++) {
			if (i != 0) {
				serializer.WriteData((const_data_ptr_t) csv_data.delimiter.c_str(), csv_data.delimiter.size());
			}
			WriteQuotedString(serializer, csv_data, csv_data.names[i].c_str(), csv_data.names[i].size(), false);
		}
		serializer.Write(csv_data.newline);

		global_data->WriteData(serializer.blob.data.get(), serializer.blob.size);
	}
	return move(global_data);
}


static void write_csv_sink(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate, LocalFunctionData &lstate, DataChunk &input) {
	auto &csv_data = (WriteCSVData &) bind_data;
	auto &local_data = (LocalCSVData &) lstate;
	auto &global_state = (GlobalCSVData &) gstate;

	// write data into the local file data

	// first cast the columns of the chunk to varchar
	auto &cast_chunk = local_data.cast_chunk;
	cast_chunk.SetCardinality(input);
	for (idx_t col_idx = 0; col_idx < input.column_count(); col_idx++) {
		if (csv_data.sql_types[col_idx].id == SQLTypeId::VARCHAR || csv_data.sql_types[col_idx].id == SQLTypeId::BLOB) {
			// VARCHAR, just create a reference
			cast_chunk.data[col_idx].Reference(input.data[col_idx]);
		} else {
			// non varchar column, perform the cast
			VectorOperations::Cast(input.data[col_idx], cast_chunk.data[col_idx], csv_data.sql_types[col_idx],
									SQLType::VARCHAR, input.size());
		}
	}

	cast_chunk.Normalify();
	auto &writer = local_data.serializer;
	// now loop over the vectors and output the values
	for (idx_t row_idx = 0; row_idx < cast_chunk.size(); row_idx++) {
		// write values
		for (idx_t col_idx = 0; col_idx < cast_chunk.column_count(); col_idx++) {
			if (col_idx != 0) {
				writer.Write(csv_data.delimiter);
			}
			if (FlatVector::IsNull(cast_chunk.data[col_idx], row_idx)) {
				// write null value
				writer.Write(csv_data.null_str);
				continue;
			}

			// non-null value, fetch the string value from the cast chunk
			auto str_data = FlatVector::GetData<string_t>(cast_chunk.data[col_idx]);
			auto str_value = str_data[row_idx];
			if (csv_data.force_quote[col_idx]) {
				// this type requires us to either (1) write quotes, or (2) check if we need to write quotes
				// FIXME: we could gain some performance here by checking for certain types if they ever require quotes (e.g. integers only require quotes if the delimiter is a number, decimals only require quotes if the delimiter is a number or "." character)
				WriteQuotedString(writer, csv_data, str_value.GetData(), str_value.GetSize(), csv_data.force_quote[col_idx]);
			} else {
				// no quotes required: just write the string
				writer.WriteData((const_data_ptr_t) str_value.GetData(), str_value.GetSize());
			}
		}
		writer.Write(csv_data.newline);
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
	auto &local_data = (LocalCSVData &) lstate;
	auto &global_state = (GlobalCSVData &) gstate;
	auto &writer = local_data.serializer;
	// flush the local writer
	if (writer.blob.size > 0) {
		global_state.WriteData(writer.blob.data.get(), writer.blob.size);
		writer.Reset();
	}
}

void CSVCopyFunction::RegisterFunction(BuiltinFunctions &set) {
	CopyFunction info("csv");
	info.copy_to_bind = write_csv_bind;
	info.copy_to_initialize_local = write_csv_initialize_local;
	info.copy_to_initialize_global = write_csv_initialize_global;
	info.copy_to_sink = write_csv_sink;
	info.copy_to_combine = write_csv_combine;

	set.AddFunction(info);
}

} // namespace duckdb
