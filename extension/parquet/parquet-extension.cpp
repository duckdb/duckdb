#define DUCKDB_EXTENSION_MAIN

#include "parquet-extension.hpp"

#include "duckdb.hpp"
#include "parquet_metadata.hpp"
#include "parquet_reader.hpp"
#include "parquet_writer.hpp"
#include "zstd_file_system.hpp"

#include <fstream>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/storage/table/row_group.hpp"
#endif

namespace duckdb {

struct ParquetReadBindData : public TableFunctionData {
	shared_ptr<ParquetReader> initial_reader;
	vector<string> files;
	atomic<idx_t> chunk_count;
	atomic<idx_t> cur_file;
	vector<string> names;
	vector<LogicalType> types;

	// The union readers are created (when parquet union_by_name option is on) during binding
	// Those readers can be re-used during ParquetParallelStateNext
	vector<shared_ptr<ParquetReader>> union_readers;

	// These come from the initial_reader, but need to be stored in case the initial_reader is removed by a filter
	idx_t initial_file_cardinality;
	idx_t initial_file_row_groups;
	ParquetOptions parquet_options;
	MultiFileReaderBindData reader_bind;

	void Initialize(shared_ptr<ParquetReader> reader) {
		initial_reader = std::move(reader);
		initial_file_cardinality = initial_reader->NumRows();
		initial_file_row_groups = initial_reader->NumRowGroups();
		parquet_options = initial_reader->parquet_options;
	}
};

struct ParquetReadLocalState : public LocalTableFunctionState {
	shared_ptr<ParquetReader> reader;
	ParquetReaderScanState scan_state;
	bool is_parallel;
	idx_t batch_index;
	idx_t file_index;
	//! The DataChunk containing all read columns (even filter columns that are immediately removed)
	DataChunk all_columns;
};

struct ParquetReadGlobalState : public GlobalTableFunctionState {
	mutex lock;

	//! The initial reader from the bind phase
	shared_ptr<ParquetReader> initial_reader;
	//! Currently opened readers
	vector<shared_ptr<ParquetReader>> readers;
	//! Flag to indicate a file is being opened
	vector<bool> file_opening;
	//! Mutexes to wait for a file that is currently being opened
	unique_ptr<mutex[]> file_mutexes;
	//! Signal to other threads that a file failed to open, letting every thread abort.
	bool error_opening_file = false;

	//! Index of file currently up for scanning
	idx_t file_index;
	//! Index of row group within file currently up for scanning
	idx_t row_group_index;
	//! Batch index of the next row group to be scanned
	idx_t batch_index;

	idx_t max_threads;
	vector<idx_t> projection_ids;
	vector<LogicalType> scanned_types;
	vector<column_t> column_ids;
	TableFilterSet *filters;

	idx_t MaxThreads() const override {
		return max_threads;
	}

	bool CanRemoveFilterColumns() const {
		return !projection_ids.empty();
	}
};

struct ParquetWriteBindData : public TableFunctionData {
	vector<LogicalType> sql_types;
	vector<string> column_names;
	duckdb_parquet::format::CompressionCodec::type codec = duckdb_parquet::format::CompressionCodec::SNAPPY;
	idx_t row_group_size = RowGroup::ROW_GROUP_SIZE;
};

struct ParquetWriteGlobalState : public GlobalFunctionData {
	unique_ptr<ParquetWriter> writer;
};

struct ParquetWriteLocalState : public LocalFunctionData {
	explicit ParquetWriteLocalState(ClientContext &context, const vector<LogicalType> &types)
	    : buffer(Allocator::Get(context), types) {
	}

	ColumnDataCollection buffer;
};

void ParquetOptions::Serialize(FieldWriter &writer) const {
	writer.WriteField<bool>(binary_as_string);
	writer.WriteField<bool>(file_row_number);
	writer.WriteSerializable(file_options);
}

void ParquetOptions::Deserialize(FieldReader &reader) {
	binary_as_string = reader.ReadRequired<bool>();
	file_row_number = reader.ReadRequired<bool>();
	file_options = reader.ReadRequiredSerializable<MultiFileReaderOptions, MultiFileReaderOptions>();
}

BindInfo ParquetGetBatchInfo(const FunctionData *bind_data) {
	auto bind_info = BindInfo(ScanType::PARQUET);
	auto parquet_bind = (ParquetReadBindData *)bind_data;
	vector<Value> file_path;
	for (auto &path : parquet_bind->files) {
		file_path.emplace_back(path);
	}
	bind_info.InsertOption("file_path", Value::LIST(LogicalType::VARCHAR, file_path));
	bind_info.InsertOption("binary_as_string", Value::BOOLEAN(parquet_bind->parquet_options.binary_as_string));
	bind_info.InsertOption("file_row_number", Value::BOOLEAN(parquet_bind->parquet_options.file_row_number));
	parquet_bind->parquet_options.file_options.AddBatchInfo(bind_info);
	return bind_info;
}

class ParquetScanFunction {
public:
	static TableFunctionSet GetFunctionSet() {
		TableFunction table_function("parquet_scan", {LogicalType::VARCHAR}, ParquetScanImplementation, ParquetScanBind,
		                             ParquetScanInitGlobal, ParquetScanInitLocal);
		table_function.statistics = ParquetScanStats;
		table_function.cardinality = ParquetCardinality;
		table_function.table_scan_progress = ParquetProgress;
		table_function.named_parameters["binary_as_string"] = LogicalType::BOOLEAN;
		table_function.named_parameters["file_row_number"] = LogicalType::BOOLEAN;
		table_function.named_parameters["compression"] = LogicalType::VARCHAR;
		MultiFileReader::AddParameters(table_function);
		table_function.get_batch_index = ParquetScanGetBatchIndex;
		table_function.serialize = ParquetScanSerialize;
		table_function.deserialize = ParquetScanDeserialize;
		table_function.get_batch_info = ParquetGetBatchInfo;

		table_function.projection_pushdown = true;
		table_function.filter_pushdown = true;
		table_function.filter_prune = true;
		table_function.pushdown_complex_filter = ParquetComplexFilterPushdown;
		return MultiFileReader::CreateFunctionSet(table_function);
	}

	static unique_ptr<FunctionData> ParquetReadBind(ClientContext &context, CopyInfo &info,
	                                                vector<string> &expected_names,
	                                                vector<LogicalType> &expected_types) {
		D_ASSERT(expected_names.size() == expected_types.size());
		ParquetOptions parquet_options(context);

		for (auto &option : info.options) {
			auto loption = StringUtil::Lower(option.first);
			if (loption == "compression" || loption == "codec") {
				// CODEC option has no effect on parquet read: we determine codec from the file
				continue;
			} else if (loption == "binary_as_string") {
				parquet_options.binary_as_string = true;
			} else if (loption == "file_row_number") {
				parquet_options.file_row_number = true;
			} else {
				throw NotImplementedException("Unsupported option for COPY FROM parquet: %s", option.first);
			}
		}

		auto files = MultiFileReader::GetFileList(context, Value(info.file_path), "Parquet");
		return ParquetScanBindInternal(context, std::move(files), expected_types, expected_names, parquet_options);
	}

	static unique_ptr<BaseStatistics> ParquetScanStats(ClientContext &context, const FunctionData *bind_data_p,
	                                                   column_t column_index) {
		auto &bind_data = (ParquetReadBindData &)*bind_data_p;

		if (IsRowIdColumnId(column_index)) {
			return nullptr;
		}

		// NOTE: we do not want to parse the Parquet metadata for the sole purpose of getting column statistics

		auto &config = DBConfig::GetConfig(context);
		if (bind_data.files.size() < 2) {
			if (bind_data.initial_reader) {
				// most common path, scanning single parquet file
				return bind_data.initial_reader->ReadStatistics(bind_data.names[column_index]);
			} else if (!config.options.object_cache_enable) {
				// our initial reader was reset
				return nullptr;
			}
		} else if (config.options.object_cache_enable) {
			// multiple files, object cache enabled: merge statistics
			unique_ptr<BaseStatistics> overall_stats;

			auto &cache = ObjectCache::GetObjectCache(context);
			// for more than one file, we could be lucky and metadata for *every* file is in the object cache (if
			// enabled at all)
			FileSystem &fs = FileSystem::GetFileSystem(context);

			for (idx_t file_idx = 0; file_idx < bind_data.files.size(); file_idx++) {
				auto &file_name = bind_data.files[file_idx];
				auto metadata = cache.Get<ParquetFileMetadataCache>(file_name);
				if (!metadata) {
					// missing metadata entry in cache, no usable stats
					return nullptr;
				}
				auto handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ, FileSystem::DEFAULT_LOCK,
				                          FileSystem::DEFAULT_COMPRESSION, FileSystem::GetFileOpener(context));
				// we need to check if the metadata cache entries are current
				if (fs.GetLastModifiedTime(*handle) >= metadata->read_time) {
					// missing or invalid metadata entry in cache, no usable stats overall
					return nullptr;
				}
				ParquetReader reader(context, bind_data.parquet_options, metadata);
				// get and merge stats for file
				auto file_stats = reader.ReadStatistics(bind_data.names[column_index]);
				if (!file_stats) {
					return nullptr;
				}
				if (overall_stats) {
					overall_stats->Merge(*file_stats);
				} else {
					overall_stats = std::move(file_stats);
				}
			}
			// success!
			return overall_stats;
		}

		// multiple files and no object cache, no luck!
		return nullptr;
	}

	static unique_ptr<FunctionData> ParquetScanBindInternal(ClientContext &context, vector<string> files,
	                                                        vector<LogicalType> &return_types, vector<string> &names,
	                                                        ParquetOptions parquet_options) {
		auto result = make_unique<ParquetReadBindData>();
		result->files = std::move(files);
		result->reader_bind =
		    MultiFileReader::BindReader<ParquetReader>(context, result->types, result->names, *result, parquet_options);
		if (return_types.empty()) {
			// no expected types - just copy the types
			return_types = result->types;
			names = result->names;
		} else {
			if (return_types.size() != result->types.size()) {
				throw std::runtime_error(StringUtil::Format(
				    "Failed to read file \"%s\" - column count mismatch: expected %d columns but found %d",
				    result->files[0], return_types.size(), result->types.size()));
			}
			// expected types - overwrite the types we want to read instead
			result->types = return_types;
		}
		return std::move(result);
	}

	static unique_ptr<FunctionData> ParquetScanBind(ClientContext &context, TableFunctionBindInput &input,
	                                                vector<LogicalType> &return_types, vector<string> &names) {
		auto files = MultiFileReader::GetFileList(context, input.inputs[0], "Parquet");
		ParquetOptions parquet_options(context);
		for (auto &kv : input.named_parameters) {
			auto loption = StringUtil::Lower(kv.first);
			if (MultiFileReader::ParseOption(kv.first, kv.second, parquet_options.file_options)) {
				continue;
			}
			if (loption == "binary_as_string") {
				parquet_options.binary_as_string = BooleanValue::Get(kv.second);
			} else if (loption == "file_row_number") {
				parquet_options.file_row_number = BooleanValue::Get(kv.second);
			}
		}
		return ParquetScanBindInternal(context, std::move(files), return_types, names, parquet_options);
	}

	static double ParquetProgress(ClientContext &context, const FunctionData *bind_data_p,
	                              const GlobalTableFunctionState *global_state) {
		auto &bind_data = (ParquetReadBindData &)*bind_data_p;
		if (bind_data.files.empty()) {
			return 100.0;
		}
		if (bind_data.initial_file_cardinality == 0) {
			return (100.0 * (bind_data.cur_file + 1)) / bind_data.files.size();
		}
		auto percentage = (bind_data.chunk_count * STANDARD_VECTOR_SIZE * 100.0 / bind_data.initial_file_cardinality) /
		                  bind_data.files.size();
		percentage += 100.0 * bind_data.cur_file / bind_data.files.size();
		return percentage;
	}

	static unique_ptr<LocalTableFunctionState>
	ParquetScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p) {
		auto &bind_data = (ParquetReadBindData &)*input.bind_data;
		auto &gstate = (ParquetReadGlobalState &)*gstate_p;

		auto result = make_unique<ParquetReadLocalState>();
		result->is_parallel = true;
		result->batch_index = 0;
		if (input.CanRemoveFilterColumns()) {
			result->all_columns.Initialize(context.client, gstate.scanned_types);
		}
		if (!ParquetParallelStateNext(context.client, bind_data, *result, gstate)) {
			return nullptr;
		}
		return std::move(result);
	}

	static unique_ptr<GlobalTableFunctionState> ParquetScanInitGlobal(ClientContext &context,
	                                                                  TableFunctionInitInput &input) {
		auto &bind_data = (ParquetReadBindData &)*input.bind_data;
		auto result = make_unique<ParquetReadGlobalState>();

		result->file_opening = std::vector<bool>(bind_data.files.size(), false);
		result->file_mutexes = std::unique_ptr<mutex[]>(new mutex[bind_data.files.size()]);
		if (bind_data.files.empty()) {
			result->initial_reader = nullptr;
		} else {
			result->readers = std::move(bind_data.union_readers);
			if (result->readers.size() != bind_data.files.size()) {
				result->readers = std::vector<shared_ptr<ParquetReader>>(bind_data.files.size(), nullptr);
			}
			if (bind_data.initial_reader) {
				result->initial_reader = std::move(bind_data.initial_reader);
				result->readers[0] = result->initial_reader;
			} else if (result->readers[0]) {
				result->initial_reader = result->readers[0];
			} else {
				result->initial_reader =
				    make_shared<ParquetReader>(context, bind_data.files[0], bind_data.parquet_options);
				result->readers[0] = result->initial_reader;
			}
		}
		for (auto &reader : result->readers) {
			if (!reader) {
				continue;
			}
			MultiFileReader::InitializeReader(*reader, bind_data.parquet_options.file_options, bind_data.reader_bind,
			                                  bind_data.types, bind_data.names, input.column_ids, input.filters);
		}

		result->column_ids = input.column_ids;
		result->filters = input.filters;
		result->row_group_index = 0;
		result->file_index = 0;
		result->batch_index = 0;
		result->max_threads = ParquetScanMaxThreads(context, input.bind_data);
		if (input.CanRemoveFilterColumns()) {
			result->projection_ids = input.projection_ids;
			const auto table_types = bind_data.types;
			for (const auto &col_idx : input.column_ids) {
				if (IsRowIdColumnId(col_idx)) {
					result->scanned_types.emplace_back(LogicalType::ROW_TYPE);
				} else {
					result->scanned_types.push_back(table_types[col_idx]);
				}
			}
		}
		return std::move(result);
	}

	static idx_t ParquetScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
	                                      LocalTableFunctionState *local_state,
	                                      GlobalTableFunctionState *global_state) {
		auto &data = (ParquetReadLocalState &)*local_state;
		return data.batch_index;
	}

	static void ParquetScanSerialize(FieldWriter &writer, const FunctionData *bind_data_p,
	                                 const TableFunction &function) {
		auto &bind_data = (ParquetReadBindData &)*bind_data_p;
		writer.WriteList<string>(bind_data.files);
		writer.WriteRegularSerializableList(bind_data.types);
		writer.WriteList<string>(bind_data.names);
		bind_data.parquet_options.Serialize(writer);
	}

	static unique_ptr<FunctionData> ParquetScanDeserialize(ClientContext &context, FieldReader &reader,
	                                                       TableFunction &function) {
		auto files = reader.ReadRequiredList<string>();
		auto types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
		auto names = reader.ReadRequiredList<string>();
		ParquetOptions options(context);
		options.Deserialize(reader);

		return ParquetScanBindInternal(context, files, types, names, options);
	}

	static void ParquetScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		if (!data_p.local_state) {
			return;
		}
		auto &data = (ParquetReadLocalState &)*data_p.local_state;
		auto &gstate = (ParquetReadGlobalState &)*data_p.global_state;
		auto &bind_data = (ParquetReadBindData &)*data_p.bind_data;

		do {
			if (gstate.CanRemoveFilterColumns()) {
				data.all_columns.Reset();
				data.reader->Scan(data.scan_state, data.all_columns);
				MultiFileReader::FinalizeChunk(bind_data.reader_bind, data.reader->reader_data, data.all_columns);
				output.ReferenceColumns(data.all_columns, gstate.projection_ids);
			} else {
				data.reader->Scan(data.scan_state, output);
				MultiFileReader::FinalizeChunk(bind_data.reader_bind, data.reader->reader_data, output);
			}

			bind_data.chunk_count++;
			if (output.size() > 0) {
				return;
			}
			if (!ParquetParallelStateNext(context, bind_data, data, gstate)) {
				return;
			}
		} while (true);
	}

	static unique_ptr<NodeStatistics> ParquetCardinality(ClientContext &context, const FunctionData *bind_data) {
		auto &data = (ParquetReadBindData &)*bind_data;
		return make_unique<NodeStatistics>(data.initial_file_cardinality * data.files.size());
	}

	static idx_t ParquetScanMaxThreads(ClientContext &context, const FunctionData *bind_data) {
		auto &data = (ParquetReadBindData &)*bind_data;
		return data.initial_file_row_groups * data.files.size();
	}

	// This function looks for the next available row group. If not available, it will open files from bind_data.files
	// until there is a row group available for scanning or the files runs out
	static bool ParquetParallelStateNext(ClientContext &context, const ParquetReadBindData &bind_data,
	                                     ParquetReadLocalState &scan_data, ParquetReadGlobalState &parallel_state) {
		unique_lock<mutex> parallel_lock(parallel_state.lock);

		while (true) {
			if (parallel_state.error_opening_file) {
				return false;
			}

			if (parallel_state.file_index >= parallel_state.readers.size()) {
				return false;
			}

			D_ASSERT(parallel_state.initial_reader);

			if (parallel_state.readers[parallel_state.file_index]) {
				if (parallel_state.row_group_index <
				    parallel_state.readers[parallel_state.file_index]->NumRowGroups()) {
					// The current reader has rowgroups left to be scanned
					scan_data.reader = parallel_state.readers[parallel_state.file_index];
					vector<idx_t> group_indexes {parallel_state.row_group_index};
					scan_data.reader->InitializeScan(scan_data.scan_state, group_indexes);
					scan_data.batch_index = parallel_state.batch_index++;
					scan_data.file_index = parallel_state.file_index;
					parallel_state.row_group_index++;
					return true;
				} else {
					// Set state to the next file
					parallel_state.file_index++;
					parallel_state.row_group_index = 0;

					parallel_state.readers[parallel_state.file_index - 1] = nullptr;

					if (parallel_state.file_index >= bind_data.files.size()) {
						return false;
					}
					continue;
				}
			}

			if (TryOpenNextFile(context, bind_data, scan_data, parallel_state, parallel_lock)) {
				continue;
			}

			// Check if the current file is being opened, in that case we need to wait for it.
			if (!parallel_state.readers[parallel_state.file_index] &&
			    parallel_state.file_opening[parallel_state.file_index]) {
				WaitForFile(parallel_state.file_index, parallel_state, parallel_lock);
			}
		}
	}

	static void ParquetComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
	                                         vector<unique_ptr<Expression>> &filters) {
		auto data = (ParquetReadBindData *)bind_data_p;
		auto reset_reader = MultiFileReader::ComplexFilterPushdown(context, data->files,
		                                                           data->parquet_options.file_options, get, filters);
		if (reset_reader) {
			MultiFileReader::PruneReaders(*data);
		}
	}

	//! Wait for a file to become available. Parallel lock should be locked when calling.
	static void WaitForFile(idx_t file_index, ParquetReadGlobalState &parallel_state,
	                        unique_lock<mutex> &parallel_lock) {
		while (true) {
			// To get the file lock, we first need to release the parallel_lock to prevent deadlocking
			parallel_lock.unlock();
			unique_lock<mutex> current_file_lock(parallel_state.file_mutexes[file_index]);
			parallel_lock.lock();

			// Here we have both locks which means we can stop waiting if:
			// - the thread opening the file is done and the file is available
			// - the thread opening the file has failed
			// - the file was somehow scanned till the end while we were waiting
			if (parallel_state.file_index >= parallel_state.readers.size() ||
			    parallel_state.readers[parallel_state.file_index] || parallel_state.error_opening_file) {
				return;
			}
		}
	}

	//! Helper function that try to start opening a next file. Parallel lock should be locked when calling.
	static bool TryOpenNextFile(ClientContext &context, const ParquetReadBindData &bind_data,
	                            ParquetReadLocalState &scan_data, ParquetReadGlobalState &parallel_state,
	                            unique_lock<mutex> &parallel_lock) {
		for (idx_t i = parallel_state.file_index; i < bind_data.files.size(); i++) {
			if (!parallel_state.readers[i] && parallel_state.file_opening[i] == false) {
				string file = bind_data.files[i];
				parallel_state.file_opening[i] = true;
				auto pq_options = parallel_state.initial_reader->parquet_options;

				// Now we switch which lock we are holding, instead of locking the global state, we grab the lock on
				// the file we are opening. This file lock allows threads to wait for a file to be opened.
				parallel_lock.unlock();

				unique_lock<mutex> file_lock(parallel_state.file_mutexes[i]);

				shared_ptr<ParquetReader> reader;
				try {
					reader = make_shared<ParquetReader>(context, file, pq_options);
					MultiFileReader::InitializeReader(*reader, bind_data.parquet_options.file_options,
					                                  bind_data.reader_bind, bind_data.types, bind_data.names,
					                                  parallel_state.column_ids, parallel_state.filters);
				} catch (...) {
					parallel_lock.lock();
					parallel_state.error_opening_file = true;
					throw;
				}

				// Now re-lock the state and add the reader
				parallel_lock.lock();
				parallel_state.readers[i] = reader;

				return true;
			}
		}

		return false;
	}
};

unique_ptr<FunctionData> ParquetWriteBind(ClientContext &context, CopyInfo &info, vector<string> &names,
                                          vector<LogicalType> &sql_types) {
	auto bind_data = make_unique<ParquetWriteBindData>();
	for (auto &option : info.options) {
		auto loption = StringUtil::Lower(option.first);
		if (loption == "row_group_size" || loption == "chunk_size") {
			bind_data->row_group_size = option.second[0].GetValue<uint64_t>();
		} else if (loption == "compression" || loption == "codec") {
			if (!option.second.empty()) {
				auto roption = StringUtil::Lower(option.second[0].ToString());
				if (roption == "uncompressed") {
					bind_data->codec = duckdb_parquet::format::CompressionCodec::UNCOMPRESSED;
					continue;
				} else if (roption == "snappy") {
					bind_data->codec = duckdb_parquet::format::CompressionCodec::SNAPPY;
					continue;
				} else if (roption == "gzip") {
					bind_data->codec = duckdb_parquet::format::CompressionCodec::GZIP;
					continue;
				} else if (roption == "zstd") {
					bind_data->codec = duckdb_parquet::format::CompressionCodec::ZSTD;
					continue;
				}
			}
			throw ParserException("Expected %s argument to be either [uncompressed, snappy, gzip or zstd]", loption);
		} else {
			throw NotImplementedException("Unrecognized option for PARQUET: %s", option.first.c_str());
		}
	}
	bind_data->sql_types = sql_types;
	bind_data->column_names = names;
	return std::move(bind_data);
}

unique_ptr<GlobalFunctionData> ParquetWriteInitializeGlobal(ClientContext &context, FunctionData &bind_data,
                                                            const string &file_path) {
	auto global_state = make_unique<ParquetWriteGlobalState>();
	auto &parquet_bind = (ParquetWriteBindData &)bind_data;

	auto &fs = FileSystem::GetFileSystem(context);
	global_state->writer =
	    make_unique<ParquetWriter>(fs, file_path, FileSystem::GetFileOpener(context), parquet_bind.sql_types,
	                               parquet_bind.column_names, parquet_bind.codec);
	return std::move(global_state);
}

void ParquetWriteSink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                      LocalFunctionData &lstate, DataChunk &input) {
	auto &bind_data = (ParquetWriteBindData &)bind_data_p;
	auto &global_state = (ParquetWriteGlobalState &)gstate;
	auto &local_state = (ParquetWriteLocalState &)lstate;

	// append data to the local (buffered) chunk collection
	local_state.buffer.Append(input);
	if (local_state.buffer.Count() > bind_data.row_group_size) {
		// if the chunk collection exceeds a certain size we flush it to the parquet file
		global_state.writer->Flush(local_state.buffer);
		// and reset the buffer
		local_state.buffer.Reset();
	}
}

void ParquetWriteCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                         LocalFunctionData &lstate) {
	auto &global_state = (ParquetWriteGlobalState &)gstate;
	auto &local_state = (ParquetWriteLocalState &)lstate;
	// flush any data left in the local state to the file
	global_state.writer->Flush(local_state.buffer);
}

void ParquetWriteFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &global_state = (ParquetWriteGlobalState &)gstate;
	// finalize: write any additional metadata to the file here
	global_state.writer->Finalize();
}

unique_ptr<LocalFunctionData> ParquetWriteInitializeLocal(ExecutionContext &context, FunctionData &bind_data_p) {
	auto &bind_data = (ParquetWriteBindData &)bind_data_p;
	return make_unique<ParquetWriteLocalState>(context.client, bind_data.sql_types);
}

//===--------------------------------------------------------------------===//
// Parallel
//===--------------------------------------------------------------------===//
bool ParquetWriteIsParallel(ClientContext &context, FunctionData &bind_data) {
	auto &config = DBConfig::GetConfig(context);
	if (config.options.preserve_insertion_order) {
		return false;
	}
	return true;
}

unique_ptr<TableRef> ParquetScanReplacement(ClientContext &context, const string &table_name,
                                            ReplacementScanData *data) {
	auto lower_name = StringUtil::Lower(table_name);
	if (!StringUtil::EndsWith(lower_name, ".parquet") && !StringUtil::Contains(lower_name, ".parquet?")) {
		return nullptr;
	}
	auto table_function = make_unique<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_unique<ConstantExpression>(Value(table_name)));
	table_function->function = make_unique<FunctionExpression>("parquet_scan", std::move(children));
	return std::move(table_function);
}

void ParquetExtension::Load(DuckDB &db) {
	auto &fs = db.GetFileSystem();
	fs.RegisterSubSystem(FileCompressionType::ZSTD, make_unique<ZStdFileSystem>());

	auto scan_fun = ParquetScanFunction::GetFunctionSet();
	CreateTableFunctionInfo cinfo(scan_fun);
	cinfo.name = "read_parquet";
	CreateTableFunctionInfo pq_scan = cinfo;
	pq_scan.name = "parquet_scan";

	ParquetMetaDataFunction meta_fun;
	CreateTableFunctionInfo meta_cinfo(MultiFileReader::CreateFunctionSet(meta_fun));

	ParquetSchemaFunction schema_fun;
	CreateTableFunctionInfo schema_cinfo(MultiFileReader::CreateFunctionSet(schema_fun));

	CopyFunction function("parquet");
	function.copy_to_bind = ParquetWriteBind;
	function.copy_to_initialize_global = ParquetWriteInitializeGlobal;
	function.copy_to_initialize_local = ParquetWriteInitializeLocal;
	function.copy_to_sink = ParquetWriteSink;
	function.copy_to_combine = ParquetWriteCombine;
	function.copy_to_finalize = ParquetWriteFinalize;
	function.parallel = ParquetWriteIsParallel;
	function.copy_from_bind = ParquetScanFunction::ParquetReadBind;
	function.copy_from_function = scan_fun.functions[0];

	function.extension = "parquet";
	CreateCopyFunctionInfo info(function);

	Connection con(db);
	con.BeginTransaction();
	auto &context = *con.context;
	auto &catalog = Catalog::GetSystemCatalog(context);

	if (catalog.GetEntry<TableFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "parquet_scan", true)) {
		throw InvalidInputException("Parquet extension is either already loaded or built-in");
	}

	catalog.CreateCopyFunction(context, &info);
	catalog.CreateTableFunction(context, &cinfo);
	catalog.CreateTableFunction(context, &pq_scan);
	catalog.CreateTableFunction(context, &meta_cinfo);
	catalog.CreateTableFunction(context, &schema_cinfo);
	con.Commit();

	auto &config = DBConfig::GetConfig(*db.instance);
	config.replacement_scans.emplace_back(ParquetScanReplacement);
	config.AddExtensionOption("binary_as_string", "In Parquet files, interpret binary data as a string.",
	                          LogicalType::BOOLEAN);
}

std::string ParquetExtension::Name() {
	return "parquet";
}

} // namespace duckdb

#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
extern "C" {

DUCKDB_EXTENSION_API void parquet_init(duckdb::DatabaseInstance &db) { // NOLINT
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::ParquetExtension>();
}

DUCKDB_EXTENSION_API const char *parquet_version() { // NOLINT
	return duckdb::DuckDB::LibraryVersion();
}
}
#endif

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
