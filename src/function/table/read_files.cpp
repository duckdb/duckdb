#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/table/range.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------------
struct ReadFilesBindData : public TableFunctionData {
	vector<string> files;
};

static unique_ptr<FunctionData> ReadFilesBind(ClientContext &context, TableFunctionBindInput &input,
											  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ReadFilesBindData>();
	result->files = MultiFileReader::GetFileList(context, input.inputs[0], "Files", FileGlobOptions::ALLOW_EMPTY);

	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::BLOB);
	names.push_back("file");
	names.push_back("data");
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
};

static unique_ptr<GlobalTableFunctionState> ReadFilesInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ReadFilesBindData>();
	auto result = make_uniq<ReadFilesGlobalState>();

	result->files = bind_data.files;
	result->current_file_idx = 0;

	return std::move(result);
}

//------------------------------------------------------------------------------
// Execute
//------------------------------------------------------------------------------
static void AssertMaxFileSize(const string &file_name, idx_t file_size) {
	const auto max_file_size = NumericLimits<uint32_t>::Maximum();
	if(file_size > max_file_size) {
		auto max_byte_size_format = StringUtil::BytesToHumanReadableString(max_file_size);
		auto file_byte_size_format = StringUtil::BytesToHumanReadableString(file_size);
		auto error_msg = StringUtil::Format("File '%s' size is too large. Maximum file size is %s, but file size is %s",
		                                    file_name.c_str(), max_byte_size_format, file_byte_size_format);
		throw InvalidInputException(error_msg);
	}
}

static void ReadFilesExecute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<ReadFilesBindData>();
	auto &state = input.global_state->Cast<ReadFilesGlobalState>();
	auto &fs = FileSystem::GetFileSystem(context);

	auto &file_name_vector = output.data[0];
	auto &file_content_vector = output.data[1];
	auto file_name_data = FlatVector::GetData<string_t>(file_name_vector);
	auto file_content_data = FlatVector::GetData<string_t>(file_content_vector);

	auto output_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.files.size() - state.current_file_idx);

	for (idx_t out_idx = 0; out_idx < output_count; out_idx++) {
		auto &file_name = bind_data.files[state.current_file_idx + out_idx];

		// Add the file name to the output
		file_name_data[out_idx] = StringVector::AddString(file_name_vector, file_name);

		// Add the file content to the output
		auto file_handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ);
		auto file_size = file_handle->GetFileSize();

		AssertMaxFileSize(file_name, file_size);

		auto content = StringVector::EmptyString(file_content_vector, file_size);
		file_handle->Read(content.GetDataWriteable(), file_size);
		content.Finalize();
		file_content_data[out_idx] = content;
	}

	state.current_file_idx += output_count;
	output.SetCardinality(output_count);
}

//------------------------------------------------------------------------------
// Misc
//------------------------------------------------------------------------------

static double ReadFilesProgress(ClientContext &context, const FunctionData *bind_data, const GlobalTableFunctionState *gstate) {
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
void ReadFilesFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction read_files("read_files", {LogicalType::VARCHAR}, ReadFilesExecute, ReadFilesBind, ReadFilesInitGlobal);
	read_files.table_scan_progress = ReadFilesProgress;
	read_files.cardinality = ReadFilesCardinality;
	set.AddFunction(MultiFileReader::CreateFunctionSet(read_files));
}

}