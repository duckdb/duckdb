#include "json_scan.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "json_multi_file_info.hpp"

namespace duckdb {

JSONScanData::JSONScanData() {
}

void JSONScanData::InitializeReaders(ClientContext &context, MultiFileBindData &bind_data) {
	auto files = bind_data.file_list->GetAllFiles();
	bind_data.union_readers.resize(files.empty() ? 0 : files.size() - 1);
	for (idx_t file_idx = 0; file_idx < files.size(); file_idx++) {
		if (file_idx == 0) {
			bind_data.initial_reader = make_uniq<BufferedJSONReader>(context, options, files[0]);
		} else {
			auto union_data = make_uniq<BaseUnionData>(files[file_idx]);
			union_data->reader = make_uniq<BufferedJSONReader>(context, options, files[file_idx]);
			bind_data.union_readers[file_idx - 1] = std::move(union_data);
		}
	}
}

void JSONScanData::InitializeFormats() {
	InitializeFormats(options.auto_detect);
}

void JSONScanData::InitializeFormats(bool auto_detect_p) {
	type_id_map_t<vector<StrpTimeFormat>> candidate_formats;
	// Initialize date_format_map if anything was specified
	if (!options.date_format.empty()) {
		DateFormatMap::AddFormat(candidate_formats, LogicalTypeId::DATE, options.date_format);
	}
	if (!options.timestamp_format.empty()) {
		DateFormatMap::AddFormat(candidate_formats, LogicalTypeId::TIMESTAMP, options.timestamp_format);
	}

	if (auto_detect_p) {
		static const type_id_map_t<vector<const char *>> FORMAT_TEMPLATES = {
		    {LogicalTypeId::DATE, {"%m-%d-%Y", "%m-%d-%y", "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d", "%y-%m-%d"}},
		    {LogicalTypeId::TIMESTAMP,
		     {"%Y-%m-%d %H:%M:%S.%f", "%m-%d-%Y %I:%M:%S %p", "%m-%d-%y %I:%M:%S %p", "%d-%m-%Y %H:%M:%S",
		      "%d-%m-%y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"}},
		};

		// Populate possible date/timestamp formats, assume this is consistent across columns
		for (auto &kv : FORMAT_TEMPLATES) {
			const auto &logical_type = kv.first;
			if (DateFormatMap::HasFormats(candidate_formats, logical_type)) {
				continue; // Already populated
			}
			const auto &format_strings = kv.second;
			for (auto &format_string : format_strings) {
				DateFormatMap::AddFormat(candidate_formats, logical_type, format_string);
			}
		}
	}
	date_format_map = make_uniq<DateFormatMap>(std::move(candidate_formats));
}

JSONScanGlobalState::JSONScanGlobalState(ClientContext &context, const MultiFileBindData &bind_data_p)
    : bind_data(bind_data_p), json_data(bind_data.bind_data->Cast<JSONScanData>()),
      transform_options(json_data.transform_options), allocator(BufferAllocator::Get(context)),
      buffer_capacity(json_data.options.maximum_object_size * 2), file_index(0), batch_index(0),
      system_threads(TaskScheduler::GetScheduler(context).NumberOfThreads()),
      enable_parallel_scans(bind_data.file_list->GetTotalFileCount() < system_threads) {
}

JSONScanLocalState::JSONScanLocalState(ClientContext &context, JSONScanGlobalState &gstate)
    : total_read_size(0), total_tuple_count(0), scan_state(context, gstate.allocator, gstate.buffer_capacity) {
}

JSONGlobalTableFunctionState::JSONGlobalTableFunctionState(ClientContext &context, TableFunctionInitInput &input)
    : state(context, input.bind_data->Cast<MultiFileBindData>()) {
}

unique_ptr<GlobalTableFunctionState> JSONGlobalTableFunctionState::Init(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MultiFileBindData>();
	auto &json_data = bind_data.bind_data->Cast<JSONScanData>();
	auto result = make_uniq<JSONGlobalTableFunctionState>(context, input);
	auto &gstate = result->state;

	// Perform projection pushdown
	for (idx_t col_idx = 0; col_idx < input.column_ids.size(); col_idx++) {
		const auto &col_id = input.column_ids[col_idx];

		// Skip any multi-file reader / row id stuff
		if (col_id == bind_data.reader_bind.filename_idx || IsVirtualColumn(col_id)) {
			continue;
		}
		bool skip = false;
		for (const auto &hive_partitioning_index : bind_data.reader_bind.hive_partitioning_indexes) {
			if (col_id == hive_partitioning_index.index) {
				skip = true;
				break;
			}
		}
		if (skip) {
			continue;
		}

		gstate.names.push_back(json_data.key_names[col_id]);
		gstate.column_ids.push_back(col_idx);
		gstate.column_indices.push_back(input.column_indexes[col_idx]);
	}

	if (gstate.names.size() < json_data.key_names.size() || bind_data.file_options.union_by_name) {
		// If we are auto-detecting, but don't need all columns present in the file,
		// then we don't need to throw an error if we encounter an unseen column
		gstate.transform_options.error_unknown_key = false;
	}

	// Place readers where they belong
	if (bind_data.initial_reader) {
		auto &initial_reader = bind_data.initial_reader->Cast<BufferedJSONReader>();
		initial_reader.Reset();
		gstate.json_readers.emplace_back(&initial_reader);
	}
	for (const auto &reader : bind_data.union_readers) {
		auto &union_reader = reader->reader->Cast<BufferedJSONReader>();
		union_reader.Reset();
		gstate.json_readers.emplace_back(&union_reader);
	}

	vector<LogicalType> dummy_global_types(bind_data.names.size(), LogicalType::ANY);
	vector<LogicalType> dummy_local_types(gstate.names.size(), LogicalType::ANY);
	auto local_columns = MultiFileReaderColumnDefinition::ColumnsFromNamesAndTypes(gstate.names, dummy_local_types);
	auto global_columns =
	    MultiFileReaderColumnDefinition::ColumnsFromNamesAndTypes(bind_data.names, dummy_global_types);
	for (auto &reader : gstate.json_readers) {
		MultiFileReader().FinalizeBind(bind_data.file_options, gstate.bind_data.reader_bind, reader->GetFileName(),
		                               local_columns, global_columns, input.column_indexes, reader->reader_data,
		                               context, nullptr);
	}

	return std::move(result);
}

idx_t JSONGlobalTableFunctionState::MaxThreads() const {
	auto &bind_data = state.json_data;

	if (!state.json_readers.empty() && state.json_readers[0]->HasFileHandle()) {
		// We opened and auto-detected a file, so we can get a better estimate
		auto &reader = *state.json_readers[0];
		if (bind_data.options.format == JSONFormat::NEWLINE_DELIMITED ||
		    reader.GetFormat() == JSONFormat::NEWLINE_DELIMITED) {
			return MaxValue<idx_t>(
			    state.json_readers[0]->GetFileHandle().FileSize() / bind_data.options.maximum_object_size, 1);
		}
	}

	if (bind_data.options.format == JSONFormat::NEWLINE_DELIMITED) {
		// We haven't opened any files, so this is our best bet
		return state.system_threads;
	}

	// One reader per file
	return state.bind_data.file_list->GetTotalFileCount();
}

JSONLocalTableFunctionState::JSONLocalTableFunctionState(ClientContext &context, JSONScanGlobalState &gstate)
    : state(context, gstate) {
}

unique_ptr<LocalTableFunctionState> JSONLocalTableFunctionState::Init(ExecutionContext &context,
                                                                      TableFunctionInitInput &,
                                                                      GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<JSONGlobalTableFunctionState>();
	auto result = make_uniq<JSONLocalTableFunctionState>(context.client, gstate.state);

	// Copy the transform options / date format map because we need to do thread-local stuff
	result->state.transform_options = gstate.state.transform_options;

	return std::move(result);
}

idx_t JSONLocalTableFunctionState::GetBatchIndex() const {
	return state.GetScanState().batch_index.GetIndex();
}

idx_t JSONScanLocalState::ReadNext(JSONScanGlobalState &gstate) {
	scan_state.Reset();

	// We have to wrap this in a loop otherwise we stop scanning too early when there's an empty JSON file
	while (scan_state.scan_count == 0) {
		if (scan_state.buffer_offset == scan_state.buffer_size) {
			if (!ReadNextBuffer(gstate)) {
				break;
			}
		}

		ParseNextChunk(gstate);
	}

	return scan_state.scan_count;
}

void JSONScanLocalState::ParseJSON(char *const json_start, const idx_t json_size, const idx_t remaining) {
	current_reader->ParseJSON(scan_state, json_start, json_size, remaining);
}

void JSONScanLocalState::TryIncrementFileIndex(JSONScanGlobalState &gstate) const {
	if (gstate.file_index < gstate.json_readers.size() &&
	    RefersToSameObject(*current_reader, *gstate.json_readers[gstate.file_index])) {
		gstate.file_index++;
	}
}

bool JSONScanLocalState::IsParallel(JSONScanGlobalState &gstate) const {
	if (gstate.bind_data.file_list->GetTotalFileCount() >= gstate.system_threads) {
		return false; // More files than threads, just parallelize over the files
	}

	// NDJSON can be read in parallel
	return current_reader->GetFormat() == JSONFormat::NEWLINE_DELIMITED;
}

bool JSONScanLocalState::ReadNextBuffer(JSONScanGlobalState &gstate) {
	// First we make sure we have a buffer to read into
	AllocatedData buffer;

	optional_idx buffer_index;
	while (true) {
		// Continue with the current reader
		if (current_reader) {
			// Try to read (if we were not the last read in the previous iteration)
			bool file_done = false;
			bool read_success = current_reader->ReadNextBuffer(gstate, scan_state, buffer, buffer_index, file_done);
			if (file_done) {
				lock_guard<mutex> guard(gstate.lock);
				TryIncrementFileIndex(gstate);
				lock_guard<mutex> reader_guard(current_reader->lock);
				current_reader->GetFileHandle().Close();
			}

			if (read_success) {
				break;
			}

			// We were the last reader last time, or we didn't read anything this time
			current_reader = nullptr;
			scan_state.current_buffer_handle = nullptr;
			scan_state.is_last = false;
		}
		D_ASSERT(!scan_state.current_buffer_handle);

		// If we got here, we don't have a reader (anymore). Try to get one
		unique_lock<mutex> guard(gstate.lock);
		if (gstate.file_index == gstate.json_readers.size()) {
			return false; // No more files left
		}

		// Assign the next reader to this thread
		current_reader = gstate.json_readers[gstate.file_index].get();

		scan_state.batch_index = gstate.batch_index++;
		if (!gstate.enable_parallel_scans) {
			// Non-parallel scans, increment file index and unlock
			gstate.file_index++;
			guard.unlock();
		}

		bool file_done = false;
		current_reader->InitializeScan(gstate, scan_state, buffer, buffer_index, file_done);
		if (file_done) {
			TryIncrementFileIndex(gstate);
			lock_guard<mutex> reader_guard(current_reader->lock);
			current_reader->GetFileHandle().Close();
		}

		if (gstate.enable_parallel_scans) {
			if (!IsParallel(gstate)) {
				// We still have the lock here if parallel scans are enabled
				TryIncrementFileIndex(gstate);
			}
		}

		if (!buffer_index.IsValid() || scan_state.buffer_size == 0) {
			// If we didn't get a buffer index (because not auto-detecting), or the file was empty, just re-enter loop
			continue;
		}

		break;
	}
	current_reader->FinalizeBufferInternal(scan_state, buffer, buffer_index.GetIndex());
	return true;
}

void JSONScanLocalState::ParseNextChunk(JSONScanGlobalState &gstate) {
	auto buffer_offset_before = scan_state.buffer_offset;
	current_reader->ParseNextChunk(scan_state);

	total_read_size += scan_state.buffer_offset - buffer_offset_before;
	total_tuple_count += scan_state.scan_count;
}

const MultiFileReaderData &JSONScanLocalState::GetReaderData() const {
	return current_reader->reader_data;
}

void JSONScanLocalState::ThrowTransformError(idx_t object_index, const string &error_message) {
	current_reader->ThrowTransformError(scan_state, object_index, error_message);
}

double JSONScan::ScanProgress(ClientContext &, const FunctionData *, const GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<JSONGlobalTableFunctionState>().state;
	double progress = 0;
	for (auto &reader : gstate.json_readers) {
		progress += reader->GetProgress();
	}
	return progress / double(gstate.json_readers.size());
}

OperatorPartitionData JSONScan::GetPartitionData(ClientContext &, TableFunctionGetPartitionInput &input) {
	if (input.partition_info.RequiresPartitionColumns()) {
		throw InternalException("JSONScan::GetPartitionData: partition columns not supported");
	}
	auto &lstate = input.local_state->Cast<JSONLocalTableFunctionState>();
	return OperatorPartitionData(lstate.GetBatchIndex());
}

unique_ptr<NodeStatistics> JSONScan::Cardinality(ClientContext &, const FunctionData *bind_data) {
	auto &data = bind_data->Cast<MultiFileBindData>();
	auto &json_data = data.bind_data->Cast<JSONScanData>();
	idx_t per_file_cardinality;

	per_file_cardinality = 42; // The cardinality of an unknown JSON file is the almighty number 42
	if (data.initial_reader) {
		auto &initial_reader = data.initial_reader->Cast<BufferedJSONReader>();
		if (initial_reader.HasFileHandle()) {
			per_file_cardinality = initial_reader.GetFileHandle().FileSize() / json_data.avg_tuple_size;
		}
	} else {
	}
	return make_uniq<NodeStatistics>(per_file_cardinality * data.file_list->GetTotalFileCount());
}

void JSONScan::Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p, const TableFunction &) {
	throw NotImplementedException("JSONScan Serialize not implemented");
}

unique_ptr<FunctionData> JSONScan::Deserialize(Deserializer &deserializer, TableFunction &) {
	throw NotImplementedException("JSONScan Deserialize not implemented");
}

void JSONScan::TableFunctionDefaults(TableFunction &table_function) {
	MultiFileReader().AddParameters(table_function);

	table_function.named_parameters["maximum_object_size"] = LogicalType::UINTEGER;
	table_function.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	table_function.named_parameters["format"] = LogicalType::VARCHAR;
	table_function.named_parameters["compression"] = LogicalType::VARCHAR;

	table_function.table_scan_progress = ScanProgress;
	table_function.get_partition_data = GetPartitionData;
	table_function.cardinality = Cardinality;

	table_function.serialize = Serialize;
	table_function.deserialize = Deserialize;
	table_function.get_virtual_columns = MultiFileReaderFunction<JSONMultiFileInfo>::MultiFileGetVirtualColumns;

	table_function.projection_pushdown = true;
	table_function.filter_pushdown = false;
	table_function.filter_prune = false;
	table_function.pushdown_complex_filter = MultiFileReaderFunction<JSONMultiFileInfo>::MultiFileComplexFilterPushdown;
}

} // namespace duckdb
