#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/table/range.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

struct ReadBlobOperation {
	static constexpr const char *NAME = "read_blob";
	static constexpr const char *FILE_TYPE = "blob";

	static inline LogicalType TYPE() {
		return LogicalType::BLOB;
	}

	static inline void VERIFY(const string &, const string_t &) {
	}
};

struct ReadTextOperation {
	static constexpr const char *NAME = "read_text";
	static constexpr const char *FILE_TYPE = "text";

	static inline LogicalType TYPE() {
		return LogicalType::VARCHAR;
	}

	static inline void VERIFY(const string &filename, const string_t &content) {
		if (Utf8Proc::Analyze(content.GetData(), content.GetSize()) == UnicodeType::INVALID) {
			throw InvalidInputException(
			    "read_text: could not read content of file '%s' as valid UTF-8 encoded text. You "
			    "may want to use read_blob instead.",
			    filename);
		}
	}
};

//------------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------------
struct ReadFileBindData : public TableFunctionData {
	vector<string> files;

	static constexpr const idx_t FILE_NAME_COLUMN = 0;
	static constexpr const idx_t FILE_CONTENT_COLUMN = 1;
	static constexpr const idx_t FILE_SIZE_COLUMN = 2;
	static constexpr const idx_t FILE_LAST_MODIFIED_COLUMN = 3;
};

template <class OP>
static unique_ptr<FunctionData> ReadFileBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ReadFileBindData>();

	auto multi_file_reader = MultiFileReader::Create(input.table_function);
	result->files =
	    multi_file_reader->CreateFileList(context, input.inputs[0], FileGlobOptions::ALLOW_EMPTY)->GetAllFiles();

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("filename");
	return_types.push_back(OP::TYPE());
	names.push_back("content");
	return_types.push_back(LogicalType::BIGINT);
	names.push_back("size");
	return_types.push_back(LogicalType::TIMESTAMP);
	names.push_back("last_modified");

	return std::move(result);
}

//------------------------------------------------------------------------------
// Global state
//------------------------------------------------------------------------------
struct ReadFileGlobalState : public GlobalTableFunctionState {
	ReadFileGlobalState() : current_file_idx(0) {
	}

	atomic<idx_t> current_file_idx;
	vector<string> files;
	vector<idx_t> column_ids;
	bool requires_file_open = false;
};

static unique_ptr<GlobalTableFunctionState> ReadFileInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ReadFileBindData>();
	auto result = make_uniq<ReadFileGlobalState>();

	result->files = bind_data.files;
	result->current_file_idx = 0;
	result->column_ids = input.column_ids;

	for (const auto &column_id : input.column_ids) {
		// For everything except the 'file' name column, we need to open the file
		if (column_id != ReadFileBindData::FILE_NAME_COLUMN && column_id != COLUMN_IDENTIFIER_ROW_ID) {
			result->requires_file_open = true;
			break;
		}
	}

	return std::move(result);
}

//------------------------------------------------------------------------------
// Execute
//------------------------------------------------------------------------------
static void AssertMaxFileSize(const string &file_name, idx_t file_size) {
	const auto max_file_size = NumericLimits<uint32_t>::Maximum();
	if (file_size > max_file_size) {
		auto max_byte_size_format = StringUtil::BytesToHumanReadableString(max_file_size);
		auto file_byte_size_format = StringUtil::BytesToHumanReadableString(file_size);
		auto error_msg = StringUtil::Format("File '%s' size (%s) exceeds maximum allowed file (%s)", file_name.c_str(),
		                                    file_byte_size_format, max_byte_size_format);
		throw InvalidInputException(error_msg);
	}
}

template <class OP>
static void ReadFileExecute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<ReadFileBindData>();
	auto &state = input.global_state->Cast<ReadFileGlobalState>();
	auto &fs = FileSystem::GetFileSystem(context);

	auto output_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.files.size() - state.current_file_idx);

	// We utilize projection pushdown here to only read the file content if the 'data' column is requested
	for (idx_t out_idx = 0; out_idx < output_count; out_idx++) {
		// Add the file name to the output
		auto &file_name = bind_data.files[state.current_file_idx + out_idx];

		unique_ptr<FileHandle> file_handle = nullptr;

		// Given the columns requested, do we even need to open the file?
		if (state.requires_file_open) {
			file_handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ);
		}

		for (idx_t col_idx = 0; col_idx < state.column_ids.size(); col_idx++) {
			// We utilize projection pushdown to avoid potentially expensive fs operations.
			auto proj_idx = state.column_ids[col_idx];
			if (proj_idx == COLUMN_IDENTIFIER_ROW_ID) {
				continue;
			}
			try {
				switch (proj_idx) {
				case ReadFileBindData::FILE_NAME_COLUMN: {
					auto &file_name_vector = output.data[col_idx];
					auto file_name_string = StringVector::AddString(file_name_vector, file_name);
					FlatVector::GetData<string_t>(file_name_vector)[out_idx] = file_name_string;
				} break;
				case ReadFileBindData::FILE_CONTENT_COLUMN: {
					auto file_size = file_handle->GetFileSize();
					AssertMaxFileSize(file_name, file_size);
					auto &file_content_vector = output.data[col_idx];
					auto content_string = StringVector::EmptyString(file_content_vector, file_size);
					file_handle->Read(content_string.GetDataWriteable(), file_size);
					content_string.Finalize();

					OP::VERIFY(file_name, content_string);

					FlatVector::GetData<string_t>(file_content_vector)[out_idx] = content_string;
				} break;
				case ReadFileBindData::FILE_SIZE_COLUMN: {
					auto &file_size_vector = output.data[col_idx];
					FlatVector::GetData<int64_t>(file_size_vector)[out_idx] =
					    NumericCast<int64_t>(file_handle->GetFileSize());
				} break;
				case ReadFileBindData::FILE_LAST_MODIFIED_COLUMN: {
					auto &last_modified_vector = output.data[col_idx];
					// This can sometimes fail (e.g. httpfs file system cant always parse the last modified time
					// correctly)
					try {
						auto timestamp_seconds = Timestamp::FromEpochSeconds(fs.GetLastModifiedTime(*file_handle));
						FlatVector::GetData<timestamp_t>(last_modified_vector)[out_idx] = timestamp_seconds;
					} catch (std::exception &ex) {
						ErrorData error(ex);
						if (error.Type() == ExceptionType::CONVERSION) {
							FlatVector::SetNull(last_modified_vector, out_idx, true);
						} else {
							throw;
						}
					}
				} break;
				default:
					throw InternalException("Unsupported column index for read_file");
				}
			}
			// Filesystems are not required to support all operations, so we just set the column to NULL if not
			// implemented
			catch (std::exception &ex) {
				ErrorData error(ex);
				if (error.Type() == ExceptionType::NOT_IMPLEMENTED) {
					FlatVector::SetNull(output.data[col_idx], out_idx, true);
				} else {
					throw;
				}
			}
		}
	}

	state.current_file_idx += output_count;
	output.SetCardinality(output_count);
}

//------------------------------------------------------------------------------
// Misc
//------------------------------------------------------------------------------

static double ReadFileProgress(ClientContext &context, const FunctionData *bind_data,
                               const GlobalTableFunctionState *gstate) {
	auto &state = gstate->Cast<ReadFileGlobalState>();
	return static_cast<double>(state.current_file_idx) / static_cast<double>(state.files.size());
}

static unique_ptr<NodeStatistics> ReadFileCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ReadFileBindData>();
	auto result = make_uniq<NodeStatistics>();
	result->has_max_cardinality = true;
	result->max_cardinality = bind_data.files.size();
	result->has_estimated_cardinality = true;
	result->estimated_cardinality = bind_data.files.size();
	return result;
}

//------------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------------
template <class OP>
static TableFunction GetFunction() {
	TableFunction func(OP::NAME, {LogicalType::VARCHAR}, ReadFileExecute<OP>, ReadFileBind<OP>, ReadFileInitGlobal);
	func.table_scan_progress = ReadFileProgress;
	func.cardinality = ReadFileCardinality;
	func.projection_pushdown = true;
	return func;
}

void ReadBlobFunction::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(MultiFileReader::CreateFunctionSet(GetFunction<ReadBlobOperation>()));
}

void ReadTextFunction::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(MultiFileReader::CreateFunctionSet(GetFunction<ReadTextOperation>()));
}

} // namespace duckdb
