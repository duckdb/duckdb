#include "duckdb/function/table/range.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
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
	//! Whether to also return the options to open every file with
	bool full_file_info = false;

	bool SupportStatementCache() const override {
		return false;
	}
};

static unique_ptr<FunctionData> GlobFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	bool allow_empty = true;
	bool full_file_info = false;
	string fallback_extension;
	for (auto &entry : input.named_parameters) {
		if (entry.second.IsNull()) {
			continue;
		}
		if (entry.first == "allow_empty") {
			allow_empty = BooleanValue::Get(entry.second);
		} else if (entry.first == "fallback_extension") {
			fallback_extension = StringValue::Get(entry.second);
		} else if (entry.first == "full_file_info") {
			full_file_info = BooleanValue::Get(entry.second);
		}
	}
	FileGlobInput glob_input(allow_empty ? FileGlobOptions::ALLOW_EMPTY : FileGlobOptions::DISALLOW_EMPTY);
	if (!fallback_extension.empty()) {
		// a pattern without a glob that matches nothing is globbed as a directory of files with this extension
		glob_input = FileGlobInput(FileGlobOptions::FALLBACK_GLOB, fallback_extension);
		glob_input.allow_empty = allow_empty;
	}
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("file");
	if (full_file_info) {
		return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARIANT()));
		names.emplace_back("options");
	}
	auto result = make_uniq<GlobFunctionBindData>(std::move(glob_input));
	result->full_file_info = full_file_info;
	auto multi_file_reader = MultiFileReader::Create(input.table_function);
	result->globs = multi_file_reader->ParseFileList(input.inputs[0]);
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

//! Expand until there are files to return - returns the number of files to return next (0 when done)
static idx_t GlobNextFiles(ClientContext &context, TableFunctionInput &data_p) {
	auto &bind_data = data_p.bind_data->Cast<GlobFunctionBindData>();
	auto &state = data_p.global_state->Cast<GlobFunctionState>();
	while (state.pending_offset >= state.pending_files.size()) {
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
		state.pending_files.clear();
		state.pending_offset = 0;
		if (!GlobExpandNext(context, bind_data, state, state.pending_files)) {
			return 0;
		}
	}
	return MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.pending_files.size() - state.pending_offset);
}

static void WriteFiles(GlobFunctionState &state, Vector &result, idx_t count) {
	auto writer = FlatVector::Writer<string_t>(result, count);
	for (idx_t i = 0; i < count; i++) {
		writer.WriteValue(string_t(state.pending_files[state.pending_offset + i].path));
	}
}

//! Write the options to open the files with as a MAP(VARCHAR, VARIANT). The values are converted to VARIANT one
//! type at a time - a glob produces the same few option types for every file
static void WriteFileOptions(ClientContext &context, GlobFunctionState &state, Vector &result, idx_t count) {
	idx_t option_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto &file = state.pending_files[state.pending_offset + i];
		if (file.extended_info) {
			option_count += file.extended_info->options.size();
		}
	}
	ListVector::Reserve(result, option_count);
	auto &keys = MapVector::GetKeys(result);
	auto &values = MapVector::GetValues(result);

	//! the values of every type, and the position of each value among the options
	vector<LogicalType> value_types;
	vector<vector<reference<const Value>>> typed_values;
	vector<pair<idx_t, idx_t>> value_positions;
	value_positions.reserve(option_count);
	{
		auto list_writer = FlatVector::Writer<list_entry_t>(result, count);
		auto key_writer = FlatVector::Writer<string_t>(keys, option_count);
		idx_t offset = 0;
		for (idx_t i = 0; i < count; i++) {
			auto &file = state.pending_files[state.pending_offset + i];
			if (!file.extended_info) {
				list_writer.WriteNull();
				continue;
			}
			auto &options = file.extended_info->options;
			list_writer.WriteValue(list_entry_t(offset, options.size()));
			offset += options.size();
			for (auto &option : options) {
				key_writer.WriteValue(string_t(option.first));
				idx_t type_idx;
				for (type_idx = 0; type_idx < value_types.size(); type_idx++) {
					if (value_types[type_idx] == option.second.type()) {
						break;
					}
				}
				if (type_idx == value_types.size()) {
					value_types.push_back(option.second.type());
					typed_values.emplace_back();
				}
				value_positions.emplace_back(type_idx, typed_values[type_idx].size());
				typed_values[type_idx].push_back(option.second);
			}
		}
	}
	ListVector::SetListSize(result, option_count);
	if (option_count == 0) {
		return;
	}
	// convert the values of every type to VARIANT in one go, one after the other
	Vector variants(LogicalType::VARIANT(), option_count);
	vector<idx_t> type_offsets;
	idx_t variant_offset = 0;
	for (idx_t type_idx = 0; type_idx < value_types.size(); type_idx++) {
		auto &type_values = typed_values[type_idx];
		Vector typed(value_types[type_idx], type_values.size());
		for (idx_t i = 0; i < type_values.size(); i++) {
			typed.SetValue(i, type_values[i].get());
		}
		Vector converted(LogicalType::VARIANT(), type_values.size());
		VectorOperations::Cast(context, typed, converted, type_values.size());
		VectorOperations::Copy(converted, variants, type_values.size(), 0, variant_offset);
		type_offsets.push_back(variant_offset);
		variant_offset += type_values.size();
	}
	// and put them in the position of their option
	SelectionVector sel(option_count);
	for (idx_t i = 0; i < option_count; i++) {
		auto &position = value_positions[i];
		sel.set_index(i, type_offsets[position.first] + position.second);
	}
	VectorOperations::Copy(variants, values, sel, option_count, 0, 0);
}

static void GlobFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<GlobFunctionBindData>();
	auto &state = data_p.global_state->Cast<GlobFunctionState>();
	auto count = GlobNextFiles(context, data_p);
	if (count == 0) {
		return;
	}
	WriteFiles(state, output.data[0], count);
	if (bind_data.full_file_info) {
		WriteFileOptions(context, state, output.data[1], count);
	}
	state.pending_offset += count;
	state.files_returned.fetch_add(count, std::memory_order_relaxed);
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

void GlobTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction glob_function("glob",
	                            FunctionSignature()
	                                .AddPositionalOnly("pattern", LogicalType::VARCHAR)
	                                .AddKeywordOnly("allow_empty", LogicalType::BOOLEAN, Value::BOOLEAN(true))
	                                .AddKeywordOnly("fallback_extension", LogicalType::VARCHAR, Value())
	                                .AddKeywordOnly("full_file_info", LogicalType::BOOLEAN, Value::BOOLEAN(false)),
	                            GlobFunction, GlobFunctionBind, GlobFunctionInit);
	glob_function.table_scan_progress = GlobFunctionProgress;
	// lists the filesystem at execution time
	glob_function.stability = FunctionStability::VOLATILE;
	set.AddFunction(MultiFileReader::CreateFunctionSet(glob_function));
}

} // namespace duckdb
