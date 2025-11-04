//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/multi_file_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include <numeric>

namespace duckdb {

struct MultiFileReaderInterface {
	virtual ~MultiFileReaderInterface();

	virtual void InitializeInterface(ClientContext &context, MultiFileReader &reader, MultiFileList &file_list);
	virtual unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext &context,
	                                                            optional_ptr<TableFunctionInfo> info) = 0;
	virtual bool ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
	                             BaseFileReaderOptions &options, vector<string> &expected_names,
	                             vector<LogicalType> &expected_types) = 0;
	virtual bool ParseOption(ClientContext &context, const string &key, const Value &val,
	                         MultiFileOptions &file_options, BaseFileReaderOptions &options) = 0;
	virtual void FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options,
	                              const vector<string> &expected_names, const vector<LogicalType> &expected_types);
	virtual unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData &multi_file_data,
	                                                         unique_ptr<BaseFileReaderOptions> options) = 0;
	virtual void BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
	                        MultiFileBindData &bind_data) = 0;
	virtual void FinalizeBindData(MultiFileBindData &multi_file_data);
	virtual void GetBindInfo(const TableFunctionData &bind_data, BindInfo &info);
	virtual optional_idx MaxThreads(const MultiFileBindData &bind_data_p, const MultiFileGlobalState &global_state,
	                                FileExpandResult expand_result);
	virtual unique_ptr<GlobalTableFunctionState>
	InitializeGlobalState(ClientContext &context, MultiFileBindData &bind_data, MultiFileGlobalState &global_state) = 0;
	virtual unique_ptr<LocalTableFunctionState> InitializeLocalState(ExecutionContext &,
	                                                                 GlobalTableFunctionState &) = 0;
	virtual shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                                BaseUnionData &union_data,
	                                                const MultiFileBindData &bind_data_p) = 0;
	virtual shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                                const OpenFileInfo &file, idx_t file_idx,
	                                                const MultiFileBindData &bind_data) = 0;
	virtual shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const OpenFileInfo &file,
	                                                BaseFileReaderOptions &options,
	                                                const MultiFileOptions &file_options);
	virtual void FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
	                           LocalTableFunctionState &local_state);
	virtual unique_ptr<NodeStatistics> GetCardinality(const MultiFileBindData &bind_data, idx_t file_count) = 0;
	virtual void GetVirtualColumns(ClientContext &context, MultiFileBindData &bind_data, virtual_column_map_t &result);
	virtual unique_ptr<MultiFileReaderInterface> Copy();
	virtual FileGlobInput GetGlobInput();
};

template <class OP>
class MultiFileFunction : public TableFunction {
public:
	explicit MultiFileFunction(string name_p)
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
		dynamic_to_string = MultiFileDynamicToString;
		MultiFileReader::AddParameters(*this);
	}

	static unique_ptr<FunctionData> MultiFileBindInternal(ClientContext &context,
	                                                      unique_ptr<MultiFileReader> multi_file_reader_p,
	                                                      shared_ptr<MultiFileList> multi_file_list_p,
	                                                      vector<LogicalType> &return_types, vector<string> &names,
	                                                      MultiFileOptions file_options_p,
	                                                      unique_ptr<BaseFileReaderOptions> options_p,
	                                                      unique_ptr<MultiFileReaderInterface> interface_p) {
		auto &interface = *interface_p;

		auto result = make_uniq<MultiFileBindData>();
		result->multi_file_reader = std::move(multi_file_reader_p);
		result->file_list = std::move(multi_file_list_p);
		// auto-detect hive partitioning
		result->file_options = std::move(file_options_p);
		result->bind_data = interface.InitializeBindData(*result, std::move(options_p));
		result->interface = std::move(interface_p);

		// now bind the readers
		// there are two ways of binding the readers
		// (1) MultiFileReader::Bind -> custom bind, used only for certain lakehouse extensions
		// (2) Interface::BindReader
		bool bound_on_first_file = true;
		if (result->multi_file_reader->Bind(result->file_options, *result->file_list, result->types, result->names,
		                                    result->reader_bind)) {
			result->multi_file_reader->BindOptions(result->file_options, *result->file_list, result->types,
			                                       result->names, result->reader_bind);
			bound_on_first_file = false;
		} else {
			result->file_options.AutoDetectHivePartitioning(*result->file_list, context);
			interface.BindReader(context, result->types, result->names, *result);
		}
		interface.FinalizeBindData(*result);

		if (return_types.empty()) {
			// no expected types - just copy the types
			return_types = result->types;
			names = result->names;
		} else {
			// We're deserializing from a previously successful bind call
			// verify that the amount of columns still matches
			if (return_types.size() != result->types.size()) {
				string file_string;
				if (!result->file_options.union_by_name && bound_on_first_file) {
					file_string = result->file_list->GetFirstFile().path;
				} else {
					for (auto &file : result->file_list->GetPaths()) {
						if (!file_string.empty()) {
							file_string += ",";
						}
						file_string += file.path;
					}
				}
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
		result->columns = MultiFileColumnDefinition::ColumnsFromNamesAndTypes(result->names, result->types);
		return std::move(result);
	}

	static unique_ptr<FunctionData> MultiFileBind(ClientContext &context, TableFunctionBindInput &input,
	                                              vector<LogicalType> &return_types, vector<string> &names) {
		auto interface = OP::CreateInterface(context);
		auto multi_file_reader = MultiFileReader::Create(input.table_function);

		auto glob_input = multi_file_reader->GetGlobInput(*interface);
		auto file_list = multi_file_reader->CreateFileList(context, input.inputs[0], glob_input);

		interface->InitializeInterface(context, *multi_file_reader, *file_list);

		MultiFileOptions file_options;

		auto options = interface->InitializeOptions(context, input.info);
		for (auto &kv : input.named_parameters) {
			auto loption = StringUtil::Lower(kv.first);
			if (multi_file_reader->ParseOption(loption, kv.second, file_options, context)) {
				continue;
			}
			if (interface->ParseOption(context, kv.first, kv.second, file_options, *options)) {
				continue;
			}
			throw NotImplementedException("Unimplemented option %s", kv.first);
		}
		return MultiFileBindInternal(context, std::move(multi_file_reader), std::move(file_list), return_types, names,
		                             std::move(file_options), std::move(options), std::move(interface));
	}

	static unique_ptr<FunctionData> MultiFileBindCopy(ClientContext &context, CopyFromFunctionBindInput &input,
	                                                  vector<string> &expected_names,
	                                                  vector<LogicalType> &expected_types) {
		auto interface = OP::CreateInterface(context);
		auto multi_file_reader = MultiFileReader::CreateDefault("COPY");
		vector<string> paths = {input.info.file_path};
		auto glob_input = multi_file_reader->GetGlobInput(*interface);
		auto file_list = multi_file_reader->CreateFileList(context, paths, glob_input);

		interface->InitializeInterface(context, *multi_file_reader, *file_list);

		auto options = interface->InitializeOptions(context, nullptr);
		MultiFileOptions file_options;
		file_options.auto_detect_hive_partitioning = false;

		for (auto &option : input.info.options) {
			auto loption = StringUtil::Lower(option.first);
			if (interface->ParseCopyOption(context, loption, option.second, *options, expected_names, expected_types)) {
				continue;
			}
			throw NotImplementedException("Unsupported option for COPY FROM: %s", option.first);
		}
		interface->FinalizeCopyBind(context, *options, expected_names, expected_types);

		return MultiFileBindInternal(context, std::move(multi_file_reader), std::move(file_list), expected_types,
		                             expected_names, std::move(file_options), std::move(options), std::move(interface));
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
	static bool TryGetNextFile(MultiFileGlobalState &global_state, unique_lock<mutex> &parallel_lock) {
		D_ASSERT(parallel_lock.owns_lock());
		OpenFileInfo scanned_file;
		if (!global_state.file_list.Scan(global_state.file_list_scan, scanned_file)) {
			return false;
		}

		// Push the file in the reader data, to be opened later
		global_state.readers.push_back(make_uniq<MultiFileReaderData>(scanned_file));

		return true;
	}

	static ReaderInitializeType InitializeReader(MultiFileReaderData &reader_data, const MultiFileBindData &bind_data,
	                                             const vector<ColumnIndex> &global_column_ids,
	                                             optional_ptr<TableFilterSet> table_filters, ClientContext &context,
	                                             optional_idx file_idx, MultiFileGlobalState &global_state) {
		auto &reader = *reader_data.reader;
		// Mark the file in the file list we are scanning here
		reader.file_list_idx = file_idx;

		// 'reader_bind.schema' could be set explicitly by:
		// 1. The MultiFileReader::Bind call
		// 2. The 'schema' parquet option
		auto &global_columns = bind_data.reader_bind.schema.empty() ? bind_data.columns : bind_data.reader_bind.schema;
		return bind_data.multi_file_reader->InitializeReader(reader_data, bind_data, global_columns, global_column_ids,
		                                                     table_filters, context, global_state);
	}

	//! Helper function that try to start opening a next file. Parallel lock should be locked when calling.
	static bool TryOpenNextFile(ClientContext &context, const MultiFileBindData &bind_data,
	                            MultiFileLocalState &scan_data, MultiFileGlobalState &global_state,
	                            unique_lock<mutex> &parallel_lock) {
		if (!parallel_lock.owns_lock()) {
			throw InternalException("parallel_lock is not held in TryOpenNextFile, this should not happen");
		}

		const auto file_lookahead_limit = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());

		idx_t file_index = global_state.file_index;
		idx_t i = 0;
		while (i < file_lookahead_limit) {
			// Try to get new files to open in this loop too otherwise we will only ever open 1 file ahead
			const bool has_file_to_read = file_index < global_state.readers.size();
			if (!has_file_to_read && !TryGetNextFile(global_state, parallel_lock)) {
				return false;
			}
			auto current_file_index = file_index++;

			auto &current_reader_data = *global_state.readers[current_file_index];
			if (current_reader_data.file_state == MultiFileFileState::UNOPENED) {
				current_reader_data.file_state = MultiFileFileState::OPENING;
				// Get pointer to file mutex before unlocking
				auto &current_file_lock = *current_reader_data.file_mutex;

				// Now we switch which lock we are holding, instead of locking the global state, we grab the lock on
				// the file we are opening. This file lock allows threads to wait for a file to be opened.
				parallel_lock.unlock();
				unique_lock<mutex> file_lock(current_file_lock);

				bool can_skip_file = false;
				try {
					if (current_reader_data.union_data) {
						auto &union_data = *current_reader_data.union_data;
						current_reader_data.reader = bind_data.multi_file_reader->CreateReader(
						    context, *global_state.global_state, union_data, bind_data);
					} else {
						current_reader_data.reader = bind_data.multi_file_reader->CreateReader(
						    context, *global_state.global_state, current_reader_data.file_to_be_opened,
						    current_file_index, bind_data);
					}
					auto init_result =
					    InitializeReader(current_reader_data, bind_data, global_state.column_indexes,
					                     global_state.filters, context, current_file_index, global_state);
					if (init_result == ReaderInitializeType::SKIP_READING_FILE) {
						//! File can be skipped entirely, close it and move on
						can_skip_file = true;
					} else {
						current_reader_data.reader->PrepareReader(context, *global_state.global_state);
					}
				} catch (...) {
					parallel_lock.lock();
					global_state.error_opening_file = true;
					throw;
				}

				// Now re-lock the state and add the reader
				parallel_lock.lock();
				if (can_skip_file) {
					current_reader_data.file_state = MultiFileFileState::SKIPPED;
					//! Intentionally do not increase 'i'
					continue;
				}
				current_reader_data.file_state = MultiFileFileState::OPEN;
				return true;
			}
			i++;
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
			if (!HasFilesToRead(global_state, parallel_lock) ||
			    global_state.readers[global_state.file_index]->file_state != MultiFileFileState::OPENING ||
			    global_state.error_opening_file) {
				return;
			}
		}
	}

	static void InitializeFileScanState(ClientContext &context, MultiFileReaderData &reader_data,
	                                    MultiFileLocalState &lstate, vector<idx_t> &projection_ids) {
		lstate.reader = reader_data.reader;
		lstate.reader_data = reader_data;
		auto &reader = *lstate.reader;
		//! Initialize the intermediate chunk to be used by the underlying reader before being finalized
		vector<LogicalType> intermediate_chunk_types;
		auto &local_column_ids = reader.column_ids;
		auto &local_columns = lstate.reader->GetColumns();
		for (idx_t i = 0; i < local_column_ids.size(); i++) {
			auto local_idx = MultiFileLocalIndex(i);
			auto local_id = local_column_ids[local_idx];
			auto cast_entry = reader.cast_map.find(local_id);
			auto expr_entry = reader.expression_map.find(local_id);
			if (cast_entry != reader.cast_map.end()) {
				intermediate_chunk_types.push_back(cast_entry->second);
			} else if (expr_entry != reader.expression_map.end()) {
				intermediate_chunk_types.push_back(expr_entry->second->return_type);
			} else {
				auto &col = local_columns[local_id];
				intermediate_chunk_types.push_back(col.type);
			}
		}
		lstate.scan_chunk.Destroy();
		lstate.scan_chunk.Initialize(BufferAllocator::Get(context), intermediate_chunk_types);

		auto &executor = lstate.executor;
		executor.ClearExpressions();
		if (!projection_ids.empty()) {
			for (auto &id : projection_ids) {
				executor.AddExpression(*reader_data.expressions[id]);
			}
		} else {
			for (auto &expr : reader_data.expressions) {
				executor.AddExpression(*expr);
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

			//! If we don't have a file to read, and the MultiFileList has no new file for us - end the scan
			if (!HasFilesToRead(gstate, parallel_lock) && !TryGetNextFile(gstate, parallel_lock)) {
				bind_data.interface->FinishReading(context, *gstate.global_state, *scan_data.local_state);
				return false;
			}

			auto &current_reader_data = *gstate.readers[gstate.file_index];
			if (current_reader_data.file_state == MultiFileFileState::OPEN) {
				if (current_reader_data.reader->TryInitializeScan(context, *gstate.global_state,
				                                                  *scan_data.local_state)) {
					if (!current_reader_data.reader) {
						throw InternalException("MultiFileReader was moved");
					}
					// The current reader has data left to be scanned
					scan_data.batch_index = gstate.batch_index++;
					auto old_file_index = scan_data.file_index;
					scan_data.file_index = gstate.file_index;
					if (old_file_index != scan_data.file_index) {
						InitializeFileScanState(context, current_reader_data, scan_data, gstate.projection_ids);
					}
					return true;
				} else {
					// Set state to the next file
					++gstate.file_index;

					// Close current file
					current_reader_data.file_state = MultiFileFileState::CLOSED;

					//! Finish processing the file
					current_reader_data.reader->FinishFile(context, *gstate.global_state);
					current_reader_data.closed_reader = current_reader_data.reader;
					current_reader_data.reader = nullptr;
					continue;
				}
			} else if (current_reader_data.file_state == MultiFileFileState::SKIPPED) {
				//! This file does not need to be opened or closed, the filters have determined that this file can be
				//! skipped entirely
				++gstate.file_index;
				continue;
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

		auto result = make_uniq<MultiFileLocalState>(context.client);
		result->is_parallel = true;
		result->batch_index = 0;
		result->local_state = bind_data.interface->InitializeLocalState(context, *gstate.global_state);

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
				result->readers.push_back(make_uniq<MultiFileReaderData>(reader));
			}
			if (result->readers.size() != file_list.GetTotalFileCount()) {
				result->readers = {};
			}
		} else if (bind_data.initial_reader) {
			// we can only use the initial reader if it was constructed from the first file
			if (bind_data.initial_reader->GetFileName() == file_list.GetFirstFile().path) {
				result->readers.push_back(make_uniq<MultiFileReaderData>(std::move(bind_data.initial_reader)));
			}
		}

		result->file_index = 0;
		result->column_indexes = input.column_indexes;
		result->filters = input.filters.get();
		result->op = input.op;
		result->global_state = bind_data.interface->InitializeGlobalState(context, bind_data, *result);
		result->max_threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());

		// Ensure all readers are initialized and FileListScan is sync with readers list
		for (auto &reader_data : result->readers) {
			OpenFileInfo file_name;
			idx_t file_idx = result->file_list_scan.current_file_idx;
			file_list.Scan(result->file_list_scan, file_name);
			if (reader_data->union_data) {
				if (file_name.path != reader_data->union_data->GetFileName()) {
					throw InternalException("Mismatch in filename order and union reader order in multi file scan");
				}
			} else {
				D_ASSERT(reader_data->reader);
				if (file_name.path != reader_data->reader->GetFileName()) {
					throw InternalException("Mismatch in filename order and reader order in multi file scan");
				}
				auto init_result = InitializeReader(*reader_data, bind_data, input.column_indexes, input.filters,
				                                    context, file_idx, *result);
				if (init_result == ReaderInitializeType::SKIP_READING_FILE) {
					//! File can be skipped entirely, close it and move on
					reader_data->file_state = MultiFileFileState::SKIPPED;
					result->file_index++;
				}
			}
		}

		auto expand_result = bind_data.file_list->GetExpandResult();
		auto max_threads = bind_data.interface->MaxThreads(bind_data, *result, expand_result);
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
		bind_data.multi_file_reader->GetPartitionData(context, bind_data.reader_bind, *data.reader_data,
		                                              gstate.multi_file_reader_state, input.partition_info,
		                                              partition_data);
		return partition_data;
	}

	static void MultiFileScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		if (!data_p.local_state) {
			data_p.async_result = SourceResultType::FINISHED;
			return;
		}
		auto &data = data_p.local_state->Cast<MultiFileLocalState>();
		auto &gstate = data_p.global_state->Cast<MultiFileGlobalState>();
		auto &bind_data = data_p.bind_data->CastNoConst<MultiFileBindData>();

		if (gstate.finished) {
			data_p.async_result = SourceResultType::FINISHED;
			return;
		}

		do {
			auto &scan_chunk = data.scan_chunk;
			scan_chunk.Reset();

			auto res = data.reader->Scan(context, *gstate.global_state, *data.local_state, scan_chunk);

			if (res.GetResultType() == AsyncResultType::BLOCKED) {
				if (scan_chunk.size() != 0) {
					throw InternalException("Unexpected behaviour from Scan, no rows should be returned");
				}
				switch (data_p.results_execution_mode) {
				case AsyncResultsExecutionMode::TASK_EXECUTOR:
					data_p.async_result = std::move(res);
					return;
				case AsyncResultsExecutionMode::SYNCHRONOUS:
					res.ExecuteTasksSynchronously();
					if (res.GetResultType() != AsyncResultType::HAVE_MORE_OUTPUT) {
						throw InternalException("Unexpected behaviour from ExecuteTasksSynchronously");
					}
					// scan_chunk.size() is 0, see check above, and result is HAVE_MORE_OUTPUT, we need to loop again
					continue;
				}
			}

			output.SetCardinality(scan_chunk.size());

			if (scan_chunk.size() > 0) {
				bind_data.multi_file_reader->FinalizeChunk(context, bind_data, *data.reader, *data.reader_data,
				                                           scan_chunk, output, data.executor,
				                                           gstate.multi_file_reader_state);
			}
			if (res.GetResultType() == AsyncResultType::HAVE_MORE_OUTPUT) {
				// Loop back to the same block
				if (scan_chunk.size() == 0 && data_p.results_execution_mode == AsyncResultsExecutionMode::SYNCHRONOUS) {
					continue;
				}
				data_p.async_result = SourceResultType::HAVE_MORE_OUTPUT;
				return;
			}

			if (res.GetResultType() != AsyncResultType::FINISHED) {
				throw InternalException("Unexpected result in MultiFileScan, must be FINISHED, is %s",
				                        EnumUtil::ToChars(res.GetResultType()));
			}

			if (!TryInitializeNextBatch(context, bind_data, data, gstate)) {
				if (scan_chunk.size() > 0 && data_p.results_execution_mode == AsyncResultsExecutionMode::SYNCHRONOUS) {
					gstate.finished = true;
					data_p.async_result = SourceResultType::HAVE_MORE_OUTPUT;
				} else {
					data_p.async_result = SourceResultType::FINISHED;
				}
			} else {
				if (scan_chunk.size() == 0 && data_p.results_execution_mode == AsyncResultsExecutionMode::SYNCHRONOUS) {
					continue;
				}
				data_p.async_result = SourceResultType::HAVE_MORE_OUTPUT;
			}
			return;
		} while (true);
	}

	static unique_ptr<BaseStatistics> MultiFileScanStats(ClientContext &context, const FunctionData *bind_data_p,
	                                                     column_t column_index) {
		auto &bind_data = bind_data_p->Cast<MultiFileBindData>();

		if (!bind_data.initial_reader) {
			// no reader
			return nullptr;
		}
		// scanning single file and we have the metadata read already
		if (IsVirtualColumn(column_index)) {
			return nullptr;
		}

		const auto &col_name = bind_data.names[column_index];

		// NOTE: we do not want to parse the file metadata for the sole purpose of getting column statistics
		if (bind_data.file_list->GetExpandResult() == FileExpandResult::MULTIPLE_FILES) {
			if (!bind_data.file_options.union_by_name) {
				// multiple files, but no union_by_name: no luck!
				return nullptr;
			}

			auto merged_stats = bind_data.initial_reader->GetStatistics(context, col_name);
			if (!merged_stats) {
				return nullptr;
			}

			for (idx_t i = 1; i < bind_data.union_readers.size(); i++) {
				auto &union_reader = *bind_data.union_readers[i];
				auto stats = union_reader.GetStatistics(context, col_name);
				if (!stats || merged_stats->GetType() != stats->GetType()) {
					return nullptr;
				}
				merged_stats->Merge(*stats);
			}
			return merged_stats;
		}
		return bind_data.initial_reader->GetStatistics(context, col_name);
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
			// Initialize progress_in_file with a default value to avoid uninitialized variable usage
			double progress_in_file = 0.0;
			if (reader_data.file_state == MultiFileFileState::OPEN) {
				// file is currently open - get the progress within the file
				progress_in_file = reader_data.reader->GetProgressInFile(context);
			} else if (reader_data.file_state == MultiFileFileState::CLOSED) {
				// file has been closed - check if the reader is still in use
				auto reader = reader_data.closed_reader.lock();
				if (!reader) {
					// reader has been destroyed - we are done with this file
					progress_in_file = 100.0;
				} else {
					// file is still being read
					progress_in_file = reader->GetProgressInFile(context);
				}
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
		return data.interface->GetCardinality(data, data.file_list->GetTotalFileCount());
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
			file_path.emplace_back(file.path);
		}

		// LCOV_EXCL_START
		bind_info.InsertOption("file_path", Value::LIST(LogicalType::VARCHAR, file_path));
		bind_data.interface->GetBindInfo(*bind_data.bind_data, bind_info);
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

		bind_data.interface->GetVirtualColumns(context, bind_data, result);

		bind_data.virtual_columns = result;
		return result;
	}

	static InsertionOrderPreservingMap<string> MultiFileDynamicToString(TableFunctionDynamicToStringInput &input) {
		auto &gstate = input.global_state->Cast<MultiFileGlobalState>();
		InsertionOrderPreservingMap<string> result;
		result.insert(make_pair("Total Files Read", std::to_string(gstate.file_index.load())));
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

private:
	static bool HasFilesToRead(MultiFileGlobalState &gstate, unique_lock<mutex> &parallel_lock) {
		D_ASSERT(parallel_lock.owns_lock());
		return (gstate.file_index.load() < gstate.readers.size());
	}
};

} // namespace duckdb
