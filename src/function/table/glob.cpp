#include "duckdb/function/table/range.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/table_function_multi_file_list.hpp"
#include "duckdb/execution/progress_data.hpp"

#include <algorithm>
#include <cmath>

namespace duckdb {

struct GlobFunctionBindData : public TableFunctionData {
	explicit GlobFunctionBindData(FileGlobInput glob_input_p) : glob_input(std::move(glob_input_p)) {
	}

	//! The globs to expand
	vector<OpenFileInfo> globs;
	//! How the globs are expanded
	FileGlobInput glob_input;

	bool SupportStatementCache() const override {
		return false;
	}
};

static unique_ptr<FunctionData> GlobFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GlobFunctionBindData>(FileGlobOptions::ALLOW_EMPTY);
	auto multi_file_reader = MultiFileReader::Create(input.table_function);
	result->globs = multi_file_reader->ParseFileList(input.inputs[0]);
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("file");
	return std::move(result);
}

struct GlobFunctionState : public GlobalTableFunctionState {
	//! The glob that is being expanded
	idx_t current_glob = 0;
	//! The files of the glob that is being expanded, and the scan over them
	unique_ptr<MultiFileList> glob_files;
	MultiFileListScanData scan_state;

	//! The files that have been expanded but not yet returned
	vector<OpenFileInfo> pending_files;
	idx_t pending_offset = 0;

	//! Progress tracking - the progress is estimated while the globs are being expanded
	atomic<idx_t> files_expanded {0};
	atomic<idx_t> files_returned {0};
	atomic<bool> all_files_expanded {false};
	//! Keeps the progress monotonic
	MonotonicProgress progress;
};

static unique_ptr<GlobalTableFunctionState> GlobFunctionInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GlobFunctionState>();
}

//! Expand the next batch of files - the files of a glob that can be fetched with a single I/O operation, sorted.
//! Returns false when all globs have been expanded
static bool GlobExpandNext(ClientContext &context, const GlobFunctionBindData &bind_data, GlobFunctionState &state,
                           vector<OpenFileInfo> &result) {
	if (state.current_glob >= bind_data.globs.size()) {
		state.all_files_expanded = true;
		return false;
	}
	auto &current_entry = bind_data.globs[state.current_glob];
	if (current_entry.extended_info) {
		// the file was specified together with the options to open it with - use it as-is
		result.push_back(current_entry);
		state.current_glob++;
		state.files_expanded++;
		return true;
	}
	if (!state.glob_files) {
		// glob is not yet started for this entry - start it and initiate the scan over its files
		auto &fs = FileSystem::GetFileSystem(context);
		state.glob_files = fs.GlobFileList(current_entry.path, bind_data.glob_input);
		state.scan_state = MultiFileListScanData();
		state.glob_files->InitializeScan(state.scan_state);
	}
	// get the next batch of files we can fetch through the glob
	auto &glob_list = *state.glob_files;
	state.scan_state.scan_type = MultiFileListScanType::ALWAYS_FETCH;
	OpenFileInfo file;
	if (!glob_list.Scan(state.scan_state, file)) {
		// no more files available in this glob - move to the next glob
		state.glob_files.reset();
		state.current_glob++;
		return true;
	}
	auto batch_start = result.size();
	result.push_back(std::move(file));
	// now continue scanning files that are already available (i.e. that don't require extra I/O operations to fetch)
	state.scan_state.scan_type = MultiFileListScanType::FETCH_IF_AVAILABLE;
	while (glob_list.Scan(state.scan_state, file)) {
		result.push_back(std::move(file));
	}
	std::sort(result.begin() + NumericCast<int64_t>(batch_start), result.end());
	state.files_expanded += result.size() - batch_start;
	return true;
}

static void GlobFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<GlobFunctionBindData>();
	auto &state = data_p.global_state->Cast<GlobFunctionState>();

	// expand until there are files to return, or until all globs have been expanded
	while (state.pending_offset >= state.pending_files.size()) {
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
		state.pending_files.clear();
		state.pending_offset = 0;
		if (!GlobExpandNext(context, bind_data, state, state.pending_files)) {
			return;
		}
	}
	auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.pending_files.size() - state.pending_offset);
	auto writer = FlatVector::Writer<string_t>(output.data[0], count);
	for (idx_t i = 0; i < count; i++) {
		auto &file = state.pending_files[state.pending_offset + i];
		writer.WriteValue(string_t(file.path));
	}
	state.pending_offset += count;
	state.files_returned.fetch_add(count, std::memory_order_relaxed);
}

static bool GlobScanFiles(ClientContext &context, TableFunctionInput &data_p, vector<OpenFileInfo> &result) {
	auto &bind_data = data_p.bind_data->Cast<GlobFunctionBindData>();
	auto &state = data_p.global_state->Cast<GlobFunctionState>();
	return GlobExpandNext(context, bind_data, state, result);
}

static vector<OpenFileInfo> GlobDisplayFiles(const FunctionData &bind_data_p, optional_idx max_files) {
	// for globs we display the actual globs in the ToString() - instead of expanding to the files read
	auto &bind_data = bind_data_p.Cast<GlobFunctionBindData>();
	vector<OpenFileInfo> result;
	for (auto &glob : bind_data.globs) {
		if (max_files.IsValid() && result.size() >= max_files.GetIndex()) {
			break;
		}
		result.emplace_back(glob);
	}
	return result;
}

static double GlobFunctionProgress(ClientContext &context, const FunctionData *bind_data_p,
                                   const GlobalTableFunctionState *global_state) {
	auto &state = global_state->Cast<GlobFunctionState>();
	auto files_returned = static_cast<double>(state.files_returned.load(std::memory_order_relaxed));
	double fraction;
	if (state.all_files_expanded) {
		auto file_count = state.files_expanded.load();
		fraction = file_count == 0 ? 1.0 : files_returned / static_cast<double>(file_count);
	} else {
		// the number of files is not known yet - converge towards 100% as files are returned
		fraction = 1 - 1 / (1 + std::log2(1 + files_returned / static_cast<double>(STANDARD_VECTOR_SIZE)));
		fraction = MinValue<double>(fraction, 0.99);
	}
	auto result = state.progress.Update(ProgressData {MinValue<double>(fraction, 1.0), 1.0, false});
	return 100.0 * result.done;
}

TableFunction GlobTableFunction::GetFunction() {
	TableFunction glob_function("glob", FunctionSignature().AddPositionalOnly("pattern", LogicalType::VARCHAR),
	                            GlobFunction, GlobFunctionBind, GlobFunctionInit);
	glob_function.table_scan_progress = GlobFunctionProgress;
	// lists the filesystem at execution time
	glob_function.stability = FunctionStability::VOLATILE;
	return glob_function;
}

unique_ptr<MultiFileList> GlobTableFunction::CreateFileList(ClientContext &context, vector<OpenFileInfo> globs,
                                                            const FileGlobInput &glob_input) {
	auto bind_data = make_uniq<GlobFunctionBindData>(glob_input);
	bind_data->globs = std::move(globs);
	vector<LogicalType> return_types {LogicalType::VARCHAR};
	TableFunctionMultiFileListSettings settings;
	settings.scan_files = GlobScanFiles;
	settings.display_files = GlobDisplayFiles;
	return make_uniq<TableFunctionMultiFileList>(context, GetFunction(), std::move(bind_data), std::move(return_types),
	                                             settings);
}

void GlobTableFunction::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(MultiFileReader::CreateFunctionSet(GetFunction()));
}

} // namespace duckdb
