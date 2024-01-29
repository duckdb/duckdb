#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/table/files.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------------
struct ReadFilesBindData : public TableFunctionData {
	vector<string> files;

	static constexpr const idx_t FILE_NAME_COLUMN = 0;
	static constexpr const idx_t FILE_DATA_COLUMN = 1;
	static constexpr const idx_t FILE_SIZE_COLUMN = 2;
	static constexpr const idx_t FILE_LAST_MODIFIED_COLUMN = 3;
	static constexpr const idx_t FILE_SYSTEM_COLUMN = 4;
};

static unique_ptr<FunctionData> ReadFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ReadFilesBindData>();
	result->files = MultiFileReader::GetFileList(context, input.inputs[0], "arbitrary", FileGlobOptions::ALLOW_EMPTY);

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("filename");
	return_types.push_back(LogicalType::BLOB);
	names.push_back("data");
	return_types.push_back(LogicalType::BIGINT);
	names.push_back("size");
	return_types.push_back(LogicalType::TIMESTAMP);
	names.push_back("last_modified");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("file_system");

	return std::move(result);
}

//------------------------------------------------------------------------------
// Global state
//------------------------------------------------------------------------------
struct ReadFilesGlobalState : public GlobalTableFunctionState {
	ReadFilesGlobalState() : current_file_idx(0) {
	}

	idx_t current_file_idx;
	vector<string> files;
	vector<idx_t> column_ids;
	bool requires_file_open = false;
};

static unique_ptr<GlobalTableFunctionState> ReadFilesInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ReadFilesBindData>();
	auto result = make_uniq<ReadFilesGlobalState>();

	result->files = bind_data.files;
	result->current_file_idx = 0;
	result->column_ids = input.column_ids;

	for (const auto &column_id : input.column_ids) {
		// For everything except the 'file' name column, we need to open the file
		if (column_id > ReadFilesBindData::FILE_NAME_COLUMN) {
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

static void ReadFilesExecute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<ReadFilesBindData>();
	auto &state = input.global_state->Cast<ReadFilesGlobalState>();
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
			try {
				switch (proj_idx) {
				case ReadFilesBindData::FILE_NAME_COLUMN: {
					auto &file_name_vector = output.data[col_idx];
					auto file_name_string = StringVector::AddString(file_name_vector, file_name);
					FlatVector::GetData<string_t>(file_name_vector)[out_idx] = file_name_string;
				} break;
				case ReadFilesBindData::FILE_DATA_COLUMN: {
					auto file_size = file_handle->GetFileSize();
					AssertMaxFileSize(file_name, file_size);
					auto &file_content_vector = output.data[col_idx];
					auto content_string = StringVector::EmptyString(file_content_vector, file_size);
					file_handle->Read(content_string.GetDataWriteable(), file_size);
					content_string.Finalize();
					FlatVector::GetData<string_t>(file_content_vector)[out_idx] = content_string;
				} break;
				case ReadFilesBindData::FILE_SIZE_COLUMN: {
					auto &file_size_vector = output.data[col_idx];
					FlatVector::GetData<int64_t>(file_size_vector)[out_idx] = file_handle->GetFileSize();
				} break;
				case ReadFilesBindData::FILE_LAST_MODIFIED_COLUMN: {
					// Last modified vector
					auto &last_modified_vector = output.data[col_idx];
					// This can sometimes fail (e.g. httpfs file system cant always parse the last modified time
					// correctly)
					auto timestamp_seconds = Timestamp::FromEpochSeconds(fs.GetLastModifiedTime(*file_handle));
					FlatVector::GetData<timestamp_t>(last_modified_vector)[out_idx] = timestamp_seconds;
				} break;
				case ReadFilesBindData::FILE_SYSTEM_COLUMN: {
					auto &file_system_vector = output.data[col_idx];
					auto file_system_name = file_handle->file_system.GetName();
					auto file_system_string = StringVector::AddString(file_system_vector, file_system_name);
					FlatVector::GetData<string_t>(file_system_vector)[out_idx] = file_system_string;
				} break;
				default:
					FlatVector::SetNull(output.data[col_idx], out_idx, true);
				}
			} catch (...) {
				// Filesystems are not required to support all operations, so we just set the column to NULL if it fails
				FlatVector::SetNull(output.data[col_idx], out_idx, true);
			}
		}
	}

	state.current_file_idx += output_count;
	output.SetCardinality(output_count);
}

//------------------------------------------------------------------------------
// Misc
//------------------------------------------------------------------------------

static double ReadFilesProgress(ClientContext &context, const FunctionData *bind_data,
                                const GlobalTableFunctionState *gstate) {
	auto &state = gstate->Cast<ReadFilesGlobalState>();
	return static_cast<double>(state.current_file_idx) / static_cast<double>(state.files.size());
}

static unique_ptr<NodeStatistics> ReadFilesCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ReadFilesBindData>();
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
TableFunction ReadFileFunction::GetFunction() {
	TableFunction read_files("read_file", {LogicalType::VARCHAR}, ReadFilesExecute, ReadFilesBind, ReadFilesInitGlobal);
	read_files.table_scan_progress = ReadFilesProgress;
	read_files.cardinality = ReadFilesCardinality;
	read_files.projection_pushdown = true;
	return read_files;
}

void ReadFileFunction::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(MultiFileReader::CreateFunctionSet(GetFunction()));
}

} // namespace duckdb
