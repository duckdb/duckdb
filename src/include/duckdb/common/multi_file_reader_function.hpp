//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file_reader_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include <numeric>

namespace duckdb {

enum class MultiFileFileState : uint8_t { UNOPENED, OPENING, OPEN, CLOSED };

struct MultiFileLocalState : public LocalTableFunctionState {
	shared_ptr<BaseFileReader> reader;
	bool is_parallel;
	idx_t batch_index;
	idx_t file_index;
	unique_ptr<LocalTableFunctionState> local_state;
	//! The DataChunk containing all read columns (even columns that are immediately removed)
	DataChunk all_columns;
};

struct MultiFileFileReaderData {
	// Create data for an unopened file
	explicit MultiFileFileReaderData(const string &file_to_be_opened)
	    : reader(nullptr), file_state(MultiFileFileState::UNOPENED), file_mutex(make_uniq<mutex>()),
	      file_to_be_opened(file_to_be_opened) {
	}
	// Create data for an existing reader
	explicit MultiFileFileReaderData(shared_ptr<BaseFileReader> reader_p)
	    : reader(std::move(reader_p)), file_state(MultiFileFileState::OPEN), file_mutex(make_uniq<mutex>()) {
	}
	// Create data for an existing reader
	explicit MultiFileFileReaderData(shared_ptr<BaseUnionData> union_data_p) : file_mutex(make_uniq<mutex>()) {
		if (union_data_p->reader) {
			reader = std::move(union_data_p->reader);
			file_state = MultiFileFileState::OPEN;
		} else {
			union_data = std::move(union_data_p);
			file_state = MultiFileFileState::UNOPENED;
		}
	}

	//! Currently opened reader for the file
	shared_ptr<BaseFileReader> reader;
	//! The file reader after we have started all scans to the file
	weak_ptr<BaseFileReader> closed_reader;
	//! Flag to indicate the file is being opened
	MultiFileFileState file_state;
	//! Mutexes to wait for the file when it is being opened
	unique_ptr<mutex> file_mutex;
	//! Options for opening the file
	shared_ptr<BaseUnionData> union_data;

	//! (only set when file_state is UNOPENED) the file to be opened
	string file_to_be_opened;
};

struct MultiFileGlobalState : public GlobalTableFunctionState {
	explicit MultiFileGlobalState(MultiFileList &file_list_p) : file_list(file_list_p) {
	}
	explicit MultiFileGlobalState(unique_ptr<MultiFileList> owned_file_list_p)
	    : file_list(*owned_file_list_p), owned_file_list(std::move(owned_file_list_p)) {
	}

	//! The file list to scan
	MultiFileList &file_list;
	//! The scan over the file_list
	MultiFileListScanData file_list_scan;
	//! Owned multi file list - if filters have been dynamically pushed into the reader
	unique_ptr<MultiFileList> owned_file_list;
	//! Reader state
	unique_ptr<MultiFileReaderGlobalState> multi_file_reader_state;
	//! Lock
	mutable mutex lock;
	//! Signal to other threads that a file failed to open, letting every thread abort.
	bool error_opening_file = false;

	//! Index of file currently up for scanning
	atomic<idx_t> file_index;
	//! Index of the lowest file we know we have completely read
	mutable idx_t completed_file_index = 0;
	//! The current set of readers
	vector<unique_ptr<MultiFileFileReaderData>> readers;

	idx_t batch_index = 0;

	idx_t max_threads = 1;
	vector<idx_t> projection_ids;
	vector<LogicalType> scanned_types;
	vector<ColumnIndex> column_indexes;
	optional_ptr<TableFilterSet> filters;

	unique_ptr<GlobalTableFunctionState> global_state;

	idx_t MaxThreads() const override {
		return max_threads;
	}

	bool CanRemoveColumns() const {
		return !projection_ids.empty();
	}
};

template <class OP>
class MultiFileReaderFunction : public TableFunction {
public:
	explicit MultiFileReaderFunction(string name_p)
	    : TableFunction(std::move(name_p), {LogicalType::VARCHAR}, MultiFileScan, MultiFileBind, MultiFileInitGlobal,
	                    MultiFileInitLocal) {
		cardinality = MultiFileCardinality;
		table_scan_progress = MultiFileProgress;
		get_partition_data = MultiFileGetPartitionData;
		get_bind_info = MultiFileGetBindInfo;
		projection_pushdown = true;
		pushdown_complex_filter = MultiFileComplexFilterPushdown;
		get_partition_info = MultiFileGetPartitionInfo;
		get_virtual_columns = MultiFileGetVirtualColumns;
		MultiFileReader::AddParameters(*this);
	}

	static unique_ptr<FunctionData> MultiFileBindInternal(ClientContext &context,
	                                                      unique_ptr<MultiFileReader> multi_file_reader_p,
	                                                      shared_ptr<MultiFileList> multi_file_list_p,
	                                                      vector<LogicalType> &return_types, vector<string> &names,
	                                                      MultiFileReaderOptions file_options_p,
	                                                      unique_ptr<BaseFileReaderOptions> options_p) {
		auto result = make_uniq<MultiFileBindData>();
		result->multi_file_reader = std::move(multi_file_reader_p);
		result->file_list = std::move(multi_file_list_p);
		// auto-detect hive partitioning
		result->file_options = std::move(file_options_p);
		result->bind_data = OP::InitializeBindData(*result, std::move(options_p));
		// now bind the readers
		// there are two ways of binding the readers
		// (1) MultiFileReader::Bind -> custom bind, used only for certain lakehouse extensions
		// (2) OP::BindReader
		bool bound_on_first_file = true;
		if (result->multi_file_reader->Bind(result->file_options, *result->file_list, result->types, result->names,
		                                    result->reader_bind)) {
			result->multi_file_reader->BindOptions(result->file_options, *result->file_list, result->types,
			                                       result->names, result->reader_bind);
			bound_on_first_file = false;
		} else {
			result->file_options.AutoDetectHivePartitioning(*result->file_list, context);
			OP::BindReader(context, result->types, result->names, *result);
		}
		OP::FinalizeBindData(*result);

		if (return_types.empty()) {
			// no expected types - just copy the types
			return_types = result->types;
			names = result->names;
		} else {
			// We're deserializing from a previously successful bind call
			// verify that the amount of columns still matches
			if (return_types.size() != result->types.size()) {
				auto file_string = !result->file_options.union_by_name && bound_on_first_file
				                       ? result->file_list->GetFirstFile()
				                       : StringUtil::Join(result->file_list->GetPaths(), ",");
				string extended_error;
				extended_error = "Table schema: ";
				for (idx_t col_idx = 0; col_idx < return_types.size(); col_idx++) {
					if (col_idx > 0) {
						extended_error += ", ";
					}
					extended_error += names[col_idx] + " " + return_types[col_idx].ToString();
				}
				extended_error += "\nParquet schema: ";
				for (idx_t col_idx = 0; col_idx < result->types.size(); col_idx++) {
					if (col_idx > 0) {
						extended_error += ", ";
					}
					extended_error += result->names[col_idx] + " " + result->types[col_idx].ToString();
				}
				extended_error += "\n\nPossible solutions:";
				extended_error += "\n* Manually specify which columns to insert using \"INSERT INTO tbl SELECT ... "
				                  "FROM read_parquet(...)\"";
				throw ConversionException(
				    "Failed to read file(s) \"%s\" - column count mismatch: expected %d columns but found %d\n%s",
				    file_string, return_types.size(), result->types.size(), extended_error);
			}
			// expected types - overwrite the types we want to read instead
			result->types = return_types;
			result->table_columns = names;
		}
		result->columns = MultiFileReaderColumnDefinition::ColumnsFromNamesAndTypes(result->names, result->types);
		return std::move(result);
	}

	static unique_ptr<FunctionData> MultiFileBind(ClientContext &context, TableFunctionBindInput &input,
	                                              vector<LogicalType> &return_types, vector<string> &names) {
		auto multi_file_reader = MultiFileReader::Create(input.table_function);
		auto file_list = multi_file_reader->CreateFileList(context, input.inputs[0]);
		MultiFileReaderOptions file_options;

		auto options = OP::InitializeOptions(context, input.info);
		for (auto &kv : input.named_parameters) {
			auto loption = StringUtil::Lower(kv.first);
			if (multi_file_reader->ParseOption(loption, kv.second, file_options, context)) {
				continue;
			}
			if (OP::ParseOption(context, kv.first, kv.second, file_options, *options)) {
				continue;
			}
			throw NotImplementedException("Unimplemented option %s", kv.first);
		}
		return MultiFileBindInternal(context, std::move(multi_file_reader), std::move(file_list), return_types, names,
		                             std::move(file_options), std::move(options));
	}

	static unique_ptr<FunctionData> MultiFileBindCopy(ClientContext &context, CopyInfo &info,
	                                                  vector<string> &expected_names,
	                                                  vector<LogicalType> &expected_types) {
		auto options = OP::InitializeOptions(context, nullptr);
		MultiFileReaderOptions file_options;

		for (auto &option : info.options) {
			auto loption = StringUtil::Lower(option.first);
			if (OP::ParseCopyOption(context, loption, option.second, *options, expected_names, expected_types)) {
				continue;
			}
			throw NotImplementedException("Unsupported option for COPY FROM: %s", option.first);
		}
		OP::FinalizeCopyBind(context, *options, expected_names, expected_types);

		// TODO: Allow overriding the MultiFileReader for COPY FROM?
		auto multi_file_reader = MultiFileReader::CreateDefault("COPY");
		vector<string> paths = {info.file_path};
		auto file_list = multi_file_reader->CreateFileList(context, paths);

		return MultiFileBindInternal(context, std::move(multi_file_reader), std::move(file_list), expected_types,
		                             expected_names, std::move(file_options), std::move(options));
	}

	static unique_ptr<MultiFileList> MultiFileFilterPushdown(ClientContext &context, const MultiFileBindData &data,
	                                                         const vector<column_t> &column_ids,
	                                                         optional_ptr<TableFilterSet> filters) {
		if (!filters) {
			return nullptr;
		}
		auto new_list = data.multi_file_reader->DynamicFilterPushdown(context, *data.file_list, data.file_options,
		                                                              data.names, data.types, column_ids, *filters);
		return new_list;
	}

	// Queries the metadataprovider for another file to scan, updating the files/reader lists in the process.
	// Returns true if resized
	static bool ResizeFiles(MultiFileGlobalState &global_state) {
		string scanned_file;
		if (!global_state.file_list.Scan(global_state.file_list_scan, scanned_file)) {
			return false;
		}

		// Push the file in the reader data, to be opened later
		global_state.readers.push_back(make_uniq<MultiFileFileReaderData>(scanned_file));

		return true;
	}

	static void InitializeReader(BaseFileReader &reader, const MultiFileBindData &bind_data,
	                             const vector<ColumnIndex> &global_column_ids,
	                             optional_ptr<TableFilterSet> table_filters, ClientContext &context,
	                             optional_idx file_idx, optional_ptr<MultiFileReaderGlobalState> reader_state) {
		auto &reader_data = reader.reader_data;

		reader.table_columns = bind_data.table_columns;
		// Mark the file in the file list we are scanning here
		reader_data.file_list_idx = file_idx;

		// 'reader_bind.schema' could be set explicitly by:
		// 1. The MultiFileReader::Bind call
		// 2. The 'schema' parquet option
		auto &global_columns = bind_data.reader_bind.schema.empty() ? bind_data.columns : bind_data.reader_bind.schema;
		bind_data.multi_file_reader->InitializeReader(reader, bind_data.file_options, bind_data.reader_bind,
		                                              global_columns, global_column_ids, table_filters,
		                                              bind_data.file_list->GetFirstFile(), context, reader_state);
	}

	//! Helper function that try to start opening a next file. Parallel lock should be locked when calling.
	static bool TryOpenNextFile(ClientContext &context, const MultiFileBindData &bind_data,
	                            MultiFileLocalState &scan_data, MultiFileGlobalState &global_state,
	                            unique_lock<mutex> &parallel_lock) {
		const auto file_index_limit =
		    global_state.file_index + NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());

		for (idx_t i = global_state.file_index; i < file_index_limit; i++) {
			// We check if we can resize files in this loop too otherwise we will only ever open 1 file ahead
			if (i >= global_state.readers.size() && !ResizeFiles(global_state)) {
				return false;
			}

			auto &current_reader_data = *global_state.readers[i];
			if (current_reader_data.file_state == MultiFileFileState::UNOPENED) {
				current_reader_data.file_state = MultiFileFileState::OPENING;
				// Get pointer to file mutex before unlocking
				auto &current_file_lock = *current_reader_data.file_mutex;

				// Now we switch which lock we are holding, instead of locking the global state, we grab the lock on
				// the file we are opening. This file lock allows threads to wait for a file to be opened.
				parallel_lock.unlock();
				unique_lock<mutex> file_lock(current_file_lock);

				shared_ptr<BaseFileReader> reader;
				try {
					if (current_reader_data.union_data) {
						auto &union_data = *current_reader_data.union_data;
						reader = OP::CreateReader(context, *global_state.global_state, union_data, bind_data);
					} else {
						reader = OP::CreateReader(context, *global_state.global_state,
						                          current_reader_data.file_to_be_opened, i, bind_data);
					}
					InitializeReader(*reader, bind_data, global_state.column_indexes, global_state.filters, context, i,
					                 global_state.multi_file_reader_state);
					OP::FinalizeReader(context, *reader, *global_state.global_state);
				} catch (...) {
					parallel_lock.lock();
					global_state.error_opening_file = true;
					throw;
				}

				// Now re-lock the state and add the reader
				parallel_lock.lock();
				current_reader_data.reader = std::move(reader);
				current_reader_data.file_state = MultiFileFileState::OPEN;

				return true;
			}
		}

		return false;
	}

	//! Wait for a file to become available. Parallel lock should be locked when calling.
	static void WaitForFile(idx_t file_index, MultiFileGlobalState &global_state, unique_lock<mutex> &parallel_lock) {
		while (true) {
			// Get pointer to file mutex before unlocking
			auto &file_mutex = *global_state.readers[file_index]->file_mutex;

			// To get the file lock, we first need to release the parallel_lock to prevent deadlocking. Note that this
			// requires getting the ref to the file mutex pointer with the lock stil held: readers get be resized
			parallel_lock.unlock();
			unique_lock<mutex> current_file_lock(file_mutex);
			parallel_lock.lock();

			// Here we have both locks which means we can stop waiting if:
			// - the thread opening the file is done and the file is available
			// - the thread opening the file has failed
			// - the file was somehow scanned till the end while we were waiting
			if (global_state.file_index >= global_state.readers.size() ||
			    global_state.readers[global_state.file_index]->file_state != MultiFileFileState::OPENING ||
			    global_state.error_opening_file) {
				return;
			}
		}
	}

	// This function looks for the next available row group. If not available, it will open files from bind_data.files
	// until there is a row group available for scanning or the files runs out
	static bool TryInitializeNextBatch(ClientContext &context, const MultiFileBindData &bind_data,
	                                   MultiFileLocalState &scan_data, MultiFileGlobalState &gstate) {
		unique_lock<mutex> parallel_lock(gstate.lock);

		while (true) {
			if (gstate.error_opening_file) {
				return false;
			}

			if (gstate.file_index >= gstate.readers.size() && !ResizeFiles(gstate)) {
				OP::FinishReading(context, *gstate.global_state, *scan_data.local_state);
				return false;
			}

			auto &current_reader_data = *gstate.readers[gstate.file_index];
			if (current_reader_data.file_state == MultiFileFileState::OPEN) {
				if (OP::TryInitializeScan(context, current_reader_data.reader, *gstate.global_state,
				                          *scan_data.local_state)) {
					if (!current_reader_data.reader) {
						throw InternalException("MultiFileReader was moved");
					}
					// The current reader has data left to be scanned
					scan_data.reader = current_reader_data.reader;
					scan_data.batch_index = gstate.batch_index++;
					scan_data.file_index = gstate.file_index;
					return true;
				} else {
					// Set state to the next file
					++gstate.file_index;

					// Close current file
					current_reader_data.file_state = MultiFileFileState::CLOSED;

					//! Finish processing the file
					OP::FinishFile(context, *gstate.global_state, *current_reader_data.reader);
					current_reader_data.closed_reader = current_reader_data.reader;
					current_reader_data.reader = nullptr;
					continue;
				}
			}

			if (TryOpenNextFile(context, bind_data, scan_data, gstate, parallel_lock)) {
				continue;
			}

			// Check if the current file is being opened, in that case we need to wait for it.
			if (current_reader_data.file_state == MultiFileFileState::OPENING) {
				WaitForFile(gstate.file_index, gstate, parallel_lock);
			}
		}
	}

	static unique_ptr<LocalTableFunctionState>
	MultiFileInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p) {
		auto &bind_data = input.bind_data->Cast<MultiFileBindData>();
		auto &gstate = gstate_p->Cast<MultiFileGlobalState>();

		auto result = make_uniq<MultiFileLocalState>();
		result->is_parallel = true;
		result->batch_index = 0;
		result->local_state = OP::InitializeLocalState(context, *gstate.global_state);

		if (gstate.CanRemoveColumns()) {
			result->all_columns.Initialize(context.client, gstate.scanned_types);
		}
		if (!TryInitializeNextBatch(context.client, bind_data, *result, gstate)) {
			return nullptr;
		}
		return std::move(result);
	}

	static unique_ptr<GlobalTableFunctionState> MultiFileInitGlobal(ClientContext &context,
	                                                                TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->CastNoConst<MultiFileBindData>();
		unique_ptr<MultiFileGlobalState> result;

		// before instantiating a scan trigger a dynamic filter pushdown if possible
		auto new_list = MultiFileFilterPushdown(context, bind_data, input.column_ids, input.filters);
		if (new_list) {
			result = make_uniq<MultiFileGlobalState>(std::move(new_list));
		} else {
			result = make_uniq<MultiFileGlobalState>(*bind_data.file_list);
		}
		auto &file_list = result->file_list;
		file_list.InitializeScan(result->file_list_scan);

		auto &global_columns = bind_data.reader_bind.schema.empty() ? bind_data.columns : bind_data.reader_bind.schema;
		result->multi_file_reader_state = bind_data.multi_file_reader->InitializeGlobalState(
		    context, bind_data.file_options, bind_data.reader_bind, file_list, global_columns, input.column_indexes);

		if (file_list.IsEmpty()) {
			result->readers = {};
		} else if (!bind_data.union_readers.empty()) {
			for (auto &reader : bind_data.union_readers) {
				result->readers.push_back(make_uniq<MultiFileFileReaderData>(reader));
			}
			if (result->readers.size() != file_list.GetTotalFileCount()) {
				result->readers = {};
			}
		} else if (bind_data.initial_reader) {
			// we can only use the initial reader if it was constructed from the first file
			if (bind_data.initial_reader->file_name == file_list.GetFirstFile()) {
				result->readers.push_back(make_uniq<MultiFileFileReaderData>(std::move(bind_data.initial_reader)));
			}
		}

		// Ensure all readers are initialized and FileListScan is sync with readers list
		for (auto &reader_data : result->readers) {
			string file_name;
			idx_t file_idx = result->file_list_scan.current_file_idx;
			file_list.Scan(result->file_list_scan, file_name);
			if (reader_data->union_data) {
				if (file_name != reader_data->union_data->GetFileName()) {
					throw InternalException("Mismatch in filename order and union reader order in multi file scan");
				}
			} else {
				D_ASSERT(reader_data->reader);
				if (file_name != reader_data->reader->file_name) {
					throw InternalException("Mismatch in filename order and reader order in multi file scan");
				}
				InitializeReader(*reader_data->reader, bind_data, input.column_indexes, input.filters, context,
				                 file_idx, result->multi_file_reader_state);
			}
		}

		result->column_indexes = input.column_indexes;
		result->filters = input.filters.get();
		result->file_index = 0;
		result->global_state = OP::InitializeGlobalState(context, bind_data, *result);
		result->max_threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
		auto expand_result = bind_data.file_list->GetExpandResult();
		auto max_threads = OP::MaxThreads(bind_data, *result, expand_result);
		if (max_threads.IsValid()) {
			result->max_threads = MinValue<idx_t>(result->max_threads, max_threads.GetIndex());
		}
		bool require_extra_columns =
		    result->multi_file_reader_state && result->multi_file_reader_state->RequiresExtraColumns();
		if (input.CanRemoveFilterColumns() || require_extra_columns) {
			if (!input.projection_ids.empty()) {
				result->projection_ids = input.projection_ids;
			} else {
				result->projection_ids.resize(input.column_indexes.size());
				for (idx_t i = 0; i < input.column_indexes.size(); i++) {
					result->projection_ids[i] = i;
				}
			}

			const auto table_types = bind_data.types;
			for (const auto &col_idx : input.column_indexes) {
				auto column_id = col_idx.GetPrimaryIndex();
				if (col_idx.IsVirtualColumn()) {
					auto entry = bind_data.virtual_columns.find(column_id);
					if (entry == bind_data.virtual_columns.end()) {
						throw InternalException("MultiFileReader - virtual column definition not found");
					}
					result->scanned_types.emplace_back(entry->second.type);
				} else {
					result->scanned_types.push_back(table_types[column_id]);
				}
			}
		}
		if (require_extra_columns) {
			for (const auto &column_type : result->multi_file_reader_state->extra_columns) {
				result->scanned_types.push_back(column_type);
			}
		}
		return std::move(result);
	}

	static OperatorPartitionData MultiFileGetPartitionData(ClientContext &context,
	                                                       TableFunctionGetPartitionInput &input) {
		auto &bind_data = input.bind_data->CastNoConst<MultiFileBindData>();
		auto &data = input.local_state->Cast<MultiFileLocalState>();
		auto &gstate = input.global_state->Cast<MultiFileGlobalState>();
		OperatorPartitionData partition_data(data.batch_index);
		bind_data.multi_file_reader->GetPartitionData(context, bind_data.reader_bind, data.reader->reader_data,
		                                              gstate.multi_file_reader_state, input.partition_info,
		                                              partition_data);
		return partition_data;
	}

	static void MultiFileScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		if (!data_p.local_state) {
			return;
		}
		auto &data = data_p.local_state->Cast<MultiFileLocalState>();
		auto &gstate = data_p.global_state->Cast<MultiFileGlobalState>();
		auto &bind_data = data_p.bind_data->CastNoConst<MultiFileBindData>();
		auto remove_columns = gstate.CanRemoveColumns();
		if (remove_columns) {
			data.all_columns.Reset();
		}

		do {
			auto &scan_chunk = remove_columns ? data.all_columns : output;
			OP::Scan(context, *data.reader, *gstate.global_state, *data.local_state, scan_chunk);
			if (scan_chunk.size() > 0) {
				bind_data.multi_file_reader->FinalizeChunk(context, bind_data.reader_bind, data.reader->reader_data,
				                                           scan_chunk, gstate.multi_file_reader_state);
				if (remove_columns) {
					output.ReferenceColumns(data.all_columns, gstate.projection_ids);
				}
				return;
			}
			scan_chunk.Reset();
			if (!TryInitializeNextBatch(context, bind_data, data, gstate)) {
				return;
			}
		} while (true);
	}

	static unique_ptr<BaseStatistics> MultiFileScanStats(ClientContext &context, const FunctionData *bind_data_p,
	                                                     column_t column_index) {
		auto &bind_data = bind_data_p->Cast<MultiFileBindData>();

		// NOTE: we do not want to parse the file metadata for the sole purpose of getting column statistics
		if (bind_data.file_list->GetExpandResult() == FileExpandResult::MULTIPLE_FILES) {
			// multiple files, no luck!
			return nullptr;
		}
		if (!bind_data.initial_reader) {
			// no reader
			return nullptr;
		}
		// scanning single file and we have the metadata read already
		if (IsVirtualColumn(column_index)) {
			return nullptr;
		}
		return OP::GetStatistics(context, *bind_data.initial_reader, bind_data.names[column_index]);
	}

	static double MultiFileProgress(ClientContext &context, const FunctionData *bind_data_p,
	                                const GlobalTableFunctionState *global_state) {
		auto &gstate = global_state->Cast<MultiFileGlobalState>();

		auto total_count = gstate.file_list.GetTotalFileCount();
		if (total_count == 0) {
			return 100.0;
		}
		unique_lock<mutex> parallel_lock(gstate.lock);
		double total_progress = 100.0 * static_cast<double>(gstate.completed_file_index);
		// iterate over the files we haven't completed yet
		for (idx_t i = gstate.completed_file_index; i <= gstate.file_index && i < gstate.readers.size(); i++) {
			auto &reader_data_ptr = gstate.readers[i];
			if (!reader_data_ptr) {
				continue;
			}
			auto &reader_data = *reader_data_ptr;
			double progress_in_file;
			if (reader_data.file_state == MultiFileFileState::OPEN) {
				// file is currently open - get the progress within the file
				progress_in_file = OP::GetProgressInFile(context, *reader_data.reader);
			} else if (reader_data.file_state == MultiFileFileState::CLOSED) {
				// file has been closed - check if the reader is still in use
				auto reader = reader_data.closed_reader.lock();
				if (!reader) {
					// reader has been destroyed - we are done with this file
					progress_in_file = 100.0;
				} else {
					// file is still being read
					progress_in_file = OP::GetProgressInFile(context, *reader);
				}
			} else {
				// file has not been opened yet - progress in this file is zero
				progress_in_file = 0;
			}
			progress_in_file = MaxValue<double>(0.0, MinValue<double>(100.0, progress_in_file));
			total_progress += progress_in_file;
			if (i == gstate.completed_file_index && progress_in_file >= 100) {
				// if the progress in this file is 100, we have completed the file
				// we don't need to check anymore in the next iteration
				gstate.completed_file_index++;
			}
		}
		return total_progress / static_cast<double>(total_count);
	}

	static unique_ptr<NodeStatistics> MultiFileCardinality(ClientContext &context, const FunctionData *bind_data) {
		auto &data = bind_data->Cast<MultiFileBindData>();
		auto file_list_cardinality_estimate = data.file_list->GetCardinality(context);
		if (file_list_cardinality_estimate) {
			return file_list_cardinality_estimate;
		}
		return OP::GetCardinality(data, data.file_list->GetTotalFileCount());
	}

	static void MultiFileComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
	                                           vector<unique_ptr<Expression>> &filters) {
		auto &data = bind_data_p->Cast<MultiFileBindData>();

		MultiFilePushdownInfo info(get);
		auto new_list =
		    data.multi_file_reader->ComplexFilterPushdown(context, *data.file_list, data.file_options, info, filters);

		if (new_list) {
			data.file_list = std::move(new_list);
			MultiFileReader::PruneReaders(data, *data.file_list);
		}
	}

	static BindInfo MultiFileGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
		BindInfo bind_info(ScanType::EXTERNAL);
		auto &bind_data = bind_data_p->Cast<MultiFileBindData>();

		vector<Value> file_path;
		for (const auto &file : bind_data.file_list->Files()) {
			file_path.emplace_back(file);
		}

		// LCOV_EXCL_START
		bind_info.InsertOption("file_path", Value::LIST(LogicalType::VARCHAR, file_path));
		OP::GetBindInfo(*bind_data.bind_data, bind_info);
		bind_data.file_options.AddBatchInfo(bind_info);
		// LCOV_EXCL_STOP
		return bind_info;
	}

	static TablePartitionInfo MultiFileGetPartitionInfo(ClientContext &context, TableFunctionPartitionInput &input) {
		auto &bind_data = input.bind_data->Cast<MultiFileBindData>();
		return bind_data.multi_file_reader->GetPartitionInfo(context, bind_data.reader_bind, input);
	}

	static virtual_column_map_t MultiFileGetVirtualColumns(ClientContext &context,
	                                                       optional_ptr<FunctionData> bind_data_p) {
		auto &bind_data = bind_data_p->Cast<MultiFileBindData>();
		virtual_column_map_t result;
		MultiFileReader::GetVirtualColumns(context, bind_data.reader_bind, result);

		OP::GetVirtualColumns(context, bind_data, result);

		bind_data.virtual_columns = result;
		return result;
	}

	static void PushdownType(ClientContext &context, optional_ptr<FunctionData> bind_data_p,
	                         const unordered_map<idx_t, LogicalType> &new_column_types) {
		auto &bind_data = bind_data_p->Cast<MultiFileBindData>();
		for (auto &type : new_column_types) {
			bind_data.types[type.first] = type.second;
			bind_data.columns[type.first].type = type.second;
		}
	}
};

} // namespace duckdb
