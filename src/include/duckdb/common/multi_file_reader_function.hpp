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

namespace duckdb {

enum class MultiFileFileState : uint8_t { UNOPENED, OPENING, OPEN, CLOSED };

template <class OP>
struct MultiFileBindData : public TableFunctionData {
	unique_ptr<TableFunctionData> bind_data;
	shared_ptr<MultiFileList> file_list;
	unique_ptr<MultiFileReader> multi_file_reader;
	vector<MultiFileReaderColumnDefinition> columns;
	MultiFileReaderBindData reader_bind;
	MultiFileReaderOptions file_options;
	vector<LogicalType> types;
	vector<string> names;
	virtual_column_map_t virtual_columns;
	//! Table column names - set when using COPY tbl FROM file.parquet
	vector<string> table_columns;
	shared_ptr<BaseFileReader> initial_reader;
	// The union readers are created (when parquet union_by_name option is on) during binding
	// Those readers can be re-used during ParquetParallelStateNext
	vector<unique_ptr<typename OP::UNION_DATA>> union_readers;

	void Initialize(shared_ptr<BaseFileReader> reader) {
		initial_reader = std::move(reader);
		OP::SetInitialReader(*bind_data, *initial_reader);
	}
	void Initialize(ClientContext &, unique_ptr<typename OP::UNION_DATA> &union_data) {
		Initialize(std::move(union_data->reader));
	}
};

template <class OP>
struct MultiFileLocalState : public LocalTableFunctionState {
	shared_ptr<BaseFileReader> reader;
	bool is_parallel;
	idx_t batch_index;
	idx_t file_index;
	unique_ptr<LocalTableFunctionState> local_state;
	//! The DataChunk containing all read columns (even columns that are immediately removed)
	DataChunk all_columns;
};

template <class OP>
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
	explicit MultiFileFileReaderData(unique_ptr<typename OP::UNION_DATA> union_data_p)
	    : file_mutex(make_uniq<mutex>()) {
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
	//! Flag to indicate the file is being opened
	MultiFileFileState file_state;
	//! Mutexes to wait for the file when it is being opened
	unique_ptr<mutex> file_mutex;
	//! Options for opening the file
	unique_ptr<typename OP::UNION_DATA> union_data;

	//! (only set when file_state is UNOPENED) the file to be opened
	string file_to_be_opened;
};

template <class OP>
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
	mutex lock;
	//! Signal to other threads that a file failed to open, letting every thread abort.
	bool error_opening_file = false;

	//! Index of file currently up for scanning
	atomic<idx_t> file_index;
	//! The current set of readers
	vector<unique_ptr<MultiFileFileReaderData<OP>>> readers;

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
		statistics = MultiFileScanStats;
		cardinality = MultiFileCardinality;
		table_scan_progress = MultiFileProgress;
		get_partition_data = MultiFileGetPartitionData;
		get_bind_info = MultiFileGetBindInfo;
		projection_pushdown = true;
		filter_pushdown = true;
		filter_prune = true;
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
	                                                      typename OP::OPTIONS options_p) {
		auto result = make_uniq<MultiFileBindData<OP>>();
		result->multi_file_reader = std::move(multi_file_reader_p);
		result->file_list = std::move(multi_file_list_p);
		// auto-detect hive partitioning
		result->file_options = std::move(file_options_p);
		result->bind_data = OP::InitializeBindData(*result, std::move(options_p));
		// now bind the readers
		// there are three ways of binding the readers
		// (1) MultiFileReader::Bind -> custom bind, used only for certain lakehouse extensions
		// (2) Schema bind ->
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

		typename OP::OPTIONS options(context);
		for (auto &kv : input.named_parameters) {
			if (kv.second.IsNull()) {
				throw BinderException("Cannot use NULL as function argument");
			}
			auto loption = StringUtil::Lower(kv.first);
			if (multi_file_reader->ParseOption(loption, kv.second, file_options, context)) {
				continue;
			}
			if (OP::ParseOption(context, loption, kv.second, file_options, options)) {
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
		typename OP::OPTIONS options(context);
		MultiFileReaderOptions file_options;

		for (auto &option : info.options) {
			auto loption = StringUtil::Lower(option.first);
			if (OP::ParseCopyOption(context, loption, option.second, options)) {
				continue;
			}
			throw NotImplementedException("Unsupported option for COPY FROM: %s", option.first);
		}

		// TODO: Allow overriding the MultiFileReader for COPY FROM?
		auto multi_file_reader = MultiFileReader::CreateDefault("ParquetCopy");
		vector<string> paths = {info.file_path};
		auto file_list = multi_file_reader->CreateFileList(context, paths);

		return MultiFileBindInternal(context, std::move(multi_file_reader), std::move(file_list), expected_types,
		                             expected_names, std::move(file_options), std::move(options));
	}

	static unique_ptr<MultiFileList> MultiFileFilterPushdown(ClientContext &context, const MultiFileBindData<OP> &data,
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
	static bool ResizeFiles(MultiFileGlobalState<OP> &parallel_state) {
		string scanned_file;
		if (!parallel_state.file_list.Scan(parallel_state.file_list_scan, scanned_file)) {
			return false;
		}

		// Push the file in the reader data, to be opened later
		parallel_state.readers.push_back(make_uniq<MultiFileFileReaderData<OP>>(scanned_file));

		return true;
	}

	static void InitializeReader(BaseFileReader &reader, const MultiFileBindData<OP> &bind_data,
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
	static bool TryOpenNextFile(ClientContext &context, const MultiFileBindData<OP> &bind_data,
	                            MultiFileLocalState<OP> &scan_data, MultiFileGlobalState<OP> &parallel_state,
	                            unique_lock<mutex> &parallel_lock) {
		const auto file_index_limit =
		    parallel_state.file_index + TaskScheduler::GetScheduler(context).NumberOfThreads();

		for (idx_t i = parallel_state.file_index; i < file_index_limit; i++) {
			// We check if we can resize files in this loop too otherwise we will only ever open 1 file ahead
			if (i >= parallel_state.readers.size() && !ResizeFiles(parallel_state)) {
				return false;
			}

			auto &current_reader_data = *parallel_state.readers[i];
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
						reader = OP::CreateReader(context, union_data);
					} else {
						reader = OP::CreateReader(context, current_reader_data.file_to_be_opened, *bind_data.bind_data);
					}
					InitializeReader(*reader, bind_data, parallel_state.column_indexes, parallel_state.filters, context,
					                 i, parallel_state.multi_file_reader_state);
				} catch (...) {
					parallel_lock.lock();
					parallel_state.error_opening_file = true;
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
	static void WaitForFile(idx_t file_index, MultiFileGlobalState<OP> &parallel_state,
	                        unique_lock<mutex> &parallel_lock) {
		while (true) {
			// Get pointer to file mutex before unlocking
			auto &file_mutex = *parallel_state.readers[file_index]->file_mutex;

			// To get the file lock, we first need to release the parallel_lock to prevent deadlocking. Note that this
			// requires getting the ref to the file mutex pointer with the lock stil held: readers get be resized
			parallel_lock.unlock();
			unique_lock<mutex> current_file_lock(file_mutex);
			parallel_lock.lock();

			// Here we have both locks which means we can stop waiting if:
			// - the thread opening the file is done and the file is available
			// - the thread opening the file has failed
			// - the file was somehow scanned till the end while we were waiting
			if (parallel_state.file_index >= parallel_state.readers.size() ||
			    parallel_state.readers[parallel_state.file_index]->file_state != MultiFileFileState::OPENING ||
			    parallel_state.error_opening_file) {
				return;
			}
		}
	}

	// This function looks for the next available row group. If not available, it will open files from bind_data.files
	// until there is a row group available for scanning or the files runs out
	static bool TryInitializeNextBatch(ClientContext &context, const MultiFileBindData<OP> &bind_data,
	                                   MultiFileLocalState<OP> &scan_data, MultiFileGlobalState<OP> &gstate) {
		unique_lock<mutex> parallel_lock(gstate.lock);

		while (true) {
			if (gstate.error_opening_file) {
				return false;
			}

			if (gstate.file_index >= gstate.readers.size() && !ResizeFiles(gstate)) {
				return false;
			}

			auto &current_reader_data = *gstate.readers[gstate.file_index];
			if (current_reader_data.file_state == MultiFileFileState::OPEN) {
				if (OP::TryInitializeScan(context, *current_reader_data.reader, *gstate.global_state,
				                          *scan_data.local_state)) {
					// The current reader has data left to be scanned
					scan_data.reader = current_reader_data.reader;
					scan_data.batch_index = gstate.batch_index++;
					scan_data.file_index = gstate.file_index;
					return true;
				} else {
					// Close current file
					current_reader_data.file_state = MultiFileFileState::CLOSED;
					current_reader_data.reader = nullptr;

					// Set state to the next file
					++gstate.file_index;
					OP::FinishFile(context, *gstate.global_state);
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
		auto &bind_data = input.bind_data->Cast<MultiFileBindData<OP>>();
		auto &gstate = gstate_p->Cast<MultiFileGlobalState<OP>>();

		auto result = make_uniq<MultiFileLocalState<OP>>();
		result->is_parallel = true;
		result->batch_index = 0;
		result->local_state = OP::InitializeLocalState();

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
		auto &bind_data = input.bind_data->CastNoConst<MultiFileBindData<OP>>();
		unique_ptr<MultiFileGlobalState<OP>> result;

		// before instantiating a scan trigger a dynamic filter pushdown if possible
		auto new_list = MultiFileFilterPushdown(context, bind_data, input.column_ids, input.filters);
		if (new_list) {
			result = make_uniq<MultiFileGlobalState<OP>>(std::move(new_list));
		} else {
			result = make_uniq<MultiFileGlobalState<OP>>(*bind_data.file_list);
		}
		auto &file_list = result->file_list;
		file_list.InitializeScan(result->file_list_scan);

		auto &global_columns = bind_data.reader_bind.schema.empty() ? bind_data.columns : bind_data.reader_bind.schema;
		result->multi_file_reader_state = bind_data.multi_file_reader->InitializeGlobalState(
		    context, bind_data.file_options, bind_data.reader_bind, file_list, global_columns, input.column_indexes);

		if (file_list.IsEmpty()) {
			result->readers = {};
		} else if (!bind_data.union_readers.empty()) {
			// TODO: confirm we are not changing behaviour by modifying the order here?
			for (auto &reader : bind_data.union_readers) {
				if (!reader) {
					break;
				}
				result->readers.push_back(make_uniq<MultiFileFileReaderData<OP>>(std::move(reader)));
			}
			if (result->readers.size() != file_list.GetTotalFileCount()) {
				// This case happens with recursive CTEs: the first execution the readers have already
				// been moved out of the bind data.
				// FIXME: clean up this process and make it more explicit
				result->readers = {};
			}
		} else if (bind_data.initial_reader) {
			// we can only use the initial reader if it was constructed from the first file
			if (bind_data.initial_reader->file_name == file_list.GetFirstFile()) {
				result->readers.push_back(make_uniq<MultiFileFileReaderData<OP>>(std::move(bind_data.initial_reader)));
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
		result->global_state = OP::InitializeGlobalState();
		if (bind_data.file_list->GetExpandResult() == FileExpandResult::MULTIPLE_FILES) {
			result->max_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
		} else {
			result->max_threads = OP::MaxThreads(*bind_data.bind_data);
		}
		bool require_extra_columns =
		    result->multi_file_reader_state && result->multi_file_reader_state->RequiresExtraColumns();
		if (input.CanRemoveFilterColumns() || require_extra_columns) {
			if (!input.projection_ids.empty()) {
				result->projection_ids = input.projection_ids;
			} else {
				result->projection_ids.resize(input.column_indexes.size());
				iota(begin(result->projection_ids), end(result->projection_ids), 0);
			}

			const auto table_types = bind_data.types;
			for (const auto &col_idx : input.column_indexes) {
				auto column_id = col_idx.GetPrimaryIndex();
				if (col_idx.IsVirtualColumn()) {
					auto entry = bind_data.virtual_columns.find(column_id);
					if (entry == bind_data.virtual_columns.end()) {
						throw InternalException("Parquet - virtual column definition not found");
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
		auto &bind_data = input.bind_data->CastNoConst<MultiFileBindData<OP>>();
		auto &data = input.local_state->Cast<MultiFileLocalState<OP>>();
		auto &gstate = input.global_state->Cast<MultiFileGlobalState<OP>>();
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
		auto &data = data_p.local_state->Cast<MultiFileLocalState<OP>>();
		auto &gstate = data_p.global_state->Cast<MultiFileGlobalState<OP>>();
		auto &bind_data = data_p.bind_data->CastNoConst<MultiFileBindData<OP>>();

		bool rowgroup_finished;
		do {
			if (gstate.CanRemoveColumns()) {
				data.all_columns.Reset();
				OP::Scan(context, *data.reader, *gstate.global_state, *data.local_state, data.all_columns);
				rowgroup_finished = data.all_columns.size() == 0;
				bind_data.multi_file_reader->FinalizeChunk(context, bind_data.reader_bind, data.reader->reader_data,
				                                           data.all_columns, gstate.multi_file_reader_state);
				output.ReferenceColumns(data.all_columns, gstate.projection_ids);
			} else {
				OP::Scan(context, *data.reader, *gstate.global_state, *data.local_state, output);
				rowgroup_finished = output.size() == 0;
				bind_data.multi_file_reader->FinalizeChunk(context, bind_data.reader_bind, data.reader->reader_data,
				                                           output, gstate.multi_file_reader_state);
			}

			if (output.size() > 0) {
				return;
			}
			if (rowgroup_finished && !TryInitializeNextBatch(context, bind_data, data, gstate)) {
				return;
			}
		} while (true);
	}

	static unique_ptr<BaseStatistics> MultiFileScanStats(ClientContext &context, const FunctionData *bind_data_p,
	                                                     column_t column_index) {
		auto &bind_data = bind_data_p->Cast<MultiFileBindData<OP>>();

		// NOTE: we do not want to parse the Parquet metadata for the sole purpose of getting column statistics
		if (bind_data.file_list->GetExpandResult() == FileExpandResult::MULTIPLE_FILES) {
			// multiple files, no luck!
			return nullptr;
		}
		if (!bind_data.initial_reader) {
			// no reader
			return nullptr;
		}
		// scanning single parquet file and we have the metadata read already
		if (IsVirtualColumn(column_index)) {
			return nullptr;
		}
		return OP::GetStatistics(context, *bind_data.initial_reader, bind_data.names[column_index]);
	}

	static double MultiFileProgress(ClientContext &context, const FunctionData *bind_data_p,
	                                const GlobalTableFunctionState *global_state) {
		auto &gstate = global_state->Cast<MultiFileGlobalState<OP>>();

		auto total_count = gstate.file_list.GetTotalFileCount();
		if (total_count == 0) {
			return 100.0;
		}
		// get the percentage WITHIN the current file from the currently active reader
		auto progress_in_file = MinValue<double>(100.0, OP::GetProgressInFile(context, *gstate.global_state));
		return (progress_in_file + 100.0 * static_cast<double>(gstate.file_index)) / static_cast<double>(total_count);
	}

	static unique_ptr<NodeStatistics> MultiFileCardinality(ClientContext &context, const FunctionData *bind_data) {
		auto &data = bind_data->Cast<MultiFileBindData<OP>>();
		auto file_list_cardinality_estimate = data.file_list->GetCardinality(context);
		if (file_list_cardinality_estimate) {
			return file_list_cardinality_estimate;
		}
		return OP::GetCardinality(*data.bind_data, data.file_list->GetTotalFileCount());
	}

	static void MultiFileComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
	                                           vector<unique_ptr<Expression>> &filters) {
		auto &data = bind_data_p->Cast<MultiFileBindData<OP>>();

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
		auto &bind_data = bind_data_p->Cast<MultiFileBindData<OP>>();

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
		auto &bind_data = input.bind_data->Cast<MultiFileBindData<OP>>();
		return bind_data.multi_file_reader->GetPartitionInfo(context, bind_data.reader_bind, input);
	}

	static virtual_column_map_t MultiFileGetVirtualColumns(ClientContext &context,
	                                                       optional_ptr<FunctionData> bind_data_p) {
		auto &bind_data = bind_data_p->Cast<MultiFileBindData<OP>>();
		virtual_column_map_t result;
		MultiFileReader::GetVirtualColumns(context, bind_data.reader_bind, result);
		// FIXME: forward virtual columns
		bind_data.virtual_columns = result;
		return result;
	}
};

} // namespace duckdb
