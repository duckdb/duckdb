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
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/union_by_name.hpp"
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
#endif

namespace duckdb {

struct ParquetReadBindData : public TableFunctionData {
	shared_ptr<ParquetReader> initial_reader;
	vector<string> files;
	vector<column_t> column_ids;
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

	void SetInitialReader(shared_ptr<ParquetReader> reader) {
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
	vector<column_t> column_ids;
	TableFilterSet *table_filters;
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
	writer.WriteField<bool>(filename);
	writer.WriteField<bool>(file_row_number);
	writer.WriteField<bool>(hive_partitioning);
	writer.WriteField<bool>(union_by_name);
}

void ParquetOptions::Deserialize(FieldReader &reader) {
	binary_as_string = reader.ReadRequired<bool>();
	filename = reader.ReadRequired<bool>();
	file_row_number = reader.ReadRequired<bool>();
	hive_partitioning = reader.ReadRequired<bool>();
	union_by_name = reader.ReadRequired<bool>();
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
	bind_info.InsertOption("filename", Value::BOOLEAN(parquet_bind->parquet_options.filename));
	bind_info.InsertOption("file_row_number", Value::BOOLEAN(parquet_bind->parquet_options.file_row_number));
	bind_info.InsertOption("hive_partitioning", Value::BOOLEAN(parquet_bind->parquet_options.hive_partitioning));
	bind_info.InsertOption("union_by_name", Value::BOOLEAN(parquet_bind->parquet_options.union_by_name));
	return bind_info;
}

class ParquetScanFunction {
public:
	static TableFunctionSet GetFunctionSet() {
		TableFunctionSet set("parquet_scan");
		TableFunction table_function({LogicalType::VARCHAR}, ParquetScanImplementation, ParquetScanBind,
		                             ParquetScanInitGlobal, ParquetScanInitLocal);
		table_function.statistics = ParquetScanStats;
		table_function.cardinality = ParquetCardinality;
		table_function.table_scan_progress = ParquetProgress;
		table_function.named_parameters["binary_as_string"] = LogicalType::BOOLEAN;
		table_function.named_parameters["filename"] = LogicalType::BOOLEAN;
		table_function.named_parameters["file_row_number"] = LogicalType::BOOLEAN;
		table_function.named_parameters["hive_partitioning"] = LogicalType::BOOLEAN;
		table_function.named_parameters["union_by_name"] = LogicalType::BOOLEAN;
		table_function.named_parameters["compression"] = LogicalType::VARCHAR;
		table_function.get_batch_index = ParquetScanGetBatchIndex;
		table_function.serialize = ParquetScanSerialize;
		table_function.deserialize = ParquetScanDeserialize;
		table_function.get_batch_info = ParquetGetBatchInfo;

		table_function.projection_pushdown = true;
		table_function.filter_pushdown = true;
		table_function.filter_prune = true;
		table_function.pushdown_complex_filter = ParquetComplexFilterPushdown;
		set.AddFunction(table_function);
		table_function.arguments = {LogicalType::LIST(LogicalType::VARCHAR)};
		table_function.bind = ParquetScanBindList;
		table_function.named_parameters["binary_as_string"] = LogicalType::BOOLEAN;
		table_function.named_parameters["filename"] = LogicalType::BOOLEAN;
		table_function.named_parameters["file_row_number"] = LogicalType::BOOLEAN;
		table_function.named_parameters["hive_partitioning"] = LogicalType::BOOLEAN;
		table_function.named_parameters["union_by_name"] = LogicalType::BOOLEAN;
		set.AddFunction(table_function);
		return set;
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
			} else if (loption == "filename") {
				parquet_options.filename = true;
			} else if (loption == "file_row_number") {
				parquet_options.file_row_number = true;
			} else if (loption == "hive_partitioning") {
				parquet_options.hive_partitioning = true;
			} else if (loption == "union_by_name") {
				parquet_options.union_by_name = true;
			} else {
				throw NotImplementedException("Unsupported option for COPY FROM parquet: %s", option.first);
			}
		}

		FileSystem &fs = FileSystem::GetFileSystem(context);
		auto files = fs.GlobFiles(info.file_path, context);

		// The most likely path (Parquet read without union by name option)
		if (!parquet_options.union_by_name) {
			auto result = make_unique<ParquetReadBindData>();
			result->files = std::move(files);
			result->SetInitialReader(
			    make_shared<ParquetReader>(context, result->files[0], expected_types, parquet_options));
			result->names = result->initial_reader->names;
			result->types = result->initial_reader->return_types;
			return std::move(result);
		} else {
			return ParquetUnionNamesBind(context, files, expected_types, expected_names, parquet_options);
		}
	}

	static unique_ptr<BaseStatistics> ParquetScanStats(ClientContext &context, const FunctionData *bind_data_p,
	                                                   column_t column_index) {
		auto &bind_data = bind_data_p->CastNoConst<ParquetReadBindData>();

		if (IsRowIdColumnId(column_index)) {
			return nullptr;
		}

		// NOTE: we do not want to parse the Parquet metadata for the sole purpose of getting column statistics

		auto &config = DBConfig::GetConfig(context);
		if (bind_data.files.size() < 2) {
			if (bind_data.initial_reader) {
				// most common path, scanning single parquet file
				return ParquetReader::ReadStatistics(*bind_data.initial_reader,
				                                     bind_data.initial_reader->return_types[column_index], column_index,
				                                     bind_data.initial_reader->metadata->metadata.get());
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

			// If we don't have an initial_reader anymore, we may need to allocate a new one here.
			shared_ptr<ParquetReader> reader;

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

				// If we don't have an initial reader anymore we need to create a reader
				auto &current_reader = bind_data.initial_reader ? bind_data.initial_reader : reader;
				if (!current_reader) {
					std::vector<column_t> ids(bind_data.names.size());
					std::iota(std::begin(ids), std::end(ids), 0); // fill with 0,1,2,3.. etc

					current_reader =
					    make_shared<ParquetReader>(context, bind_data.files[0], bind_data.names, bind_data.types, ids,
					                               bind_data.parquet_options, bind_data.files[0]);
				}

				// get and merge stats for file
				auto file_stats =
				    ParquetReader::ReadStatistics(*current_reader, current_reader->return_types[column_index],
				                                  column_index, metadata->metadata.get());
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

		// The most likely path (Parquet Scan without union by name option)
		if (!parquet_options.union_by_name) {
			result->files = std::move(files);
			result->SetInitialReader(make_shared<ParquetReader>(context, result->files[0], parquet_options));
			return_types = result->types = result->initial_reader->return_types;
			names = result->names = result->initial_reader->names;
			return std::move(result);
		} else {
			return ParquetUnionNamesBind(context, files, return_types, names, parquet_options);
		}
	}

	static unique_ptr<FunctionData> ParquetUnionNamesBind(ClientContext &context, vector<string> files,
	                                                      vector<LogicalType> &return_types, vector<string> &names,
	                                                      ParquetOptions parquet_options) {
		auto result = make_unique<ParquetReadBindData>();
		result->files = std::move(files);

		case_insensitive_map_t<idx_t> union_names_map;
		vector<string> union_col_names;
		vector<LogicalType> union_col_types;
		auto dummy_readers = UnionByName<ParquetReader, ParquetOptions>::UnionCols(
		    context, result->files, union_col_types, union_col_names, union_names_map, parquet_options);

		dummy_readers = UnionByName<ParquetReader, ParquetOptions>::CreateUnionMap(
		    std::move(dummy_readers), union_col_types, union_col_names, union_names_map);

		std::move(dummy_readers.begin(), dummy_readers.end(), std::back_inserter(result->union_readers));
		names.assign(union_col_names.begin(), union_col_names.end());
		return_types.assign(union_col_types.begin(), union_col_types.end());
		result->SetInitialReader(result->union_readers[0]);
		D_ASSERT(names.size() == return_types.size());
		result->types = union_col_types;

		return std::move(result);
	}

	static vector<string> ParquetGlob(FileSystem &fs, const string &glob, ClientContext &context) {
		return fs.GlobFiles(glob, context);
	}

	static unique_ptr<FunctionData> ParquetScanBind(ClientContext &context, TableFunctionBindInput &input,
	                                                vector<LogicalType> &return_types, vector<string> &names) {
		auto &config = DBConfig::GetConfig(context);
		if (!config.options.enable_external_access) {
			throw PermissionException("Scanning Parquet files is disabled through configuration");
		}
		if (input.inputs[0].IsNull()) {
			throw ParserException("Parquet reader cannot take NULL list as parameter");
		}
		auto file_name = StringValue::Get(input.inputs[0]);
		ParquetOptions parquet_options(context);
		for (auto &kv : input.named_parameters) {
			auto loption = StringUtil::Lower(kv.first);
			if (loption == "binary_as_string") {
				parquet_options.binary_as_string = BooleanValue::Get(kv.second);
			} else if (loption == "filename") {
				parquet_options.filename = BooleanValue::Get(kv.second);
			} else if (loption == "file_row_number") {
				parquet_options.file_row_number = BooleanValue::Get(kv.second);
			} else if (loption == "hive_partitioning") {
				parquet_options.hive_partitioning = BooleanValue::Get(kv.second);
			} else if (loption == "union_by_name") {
				parquet_options.union_by_name = BooleanValue::Get(kv.second);
			}
		}
		FileSystem &fs = FileSystem::GetFileSystem(context);
		auto files = ParquetGlob(fs, file_name, context);
		return ParquetScanBindInternal(context, std::move(files), return_types, names, parquet_options);
	}

	static unique_ptr<FunctionData> ParquetScanBindList(ClientContext &context, TableFunctionBindInput &input,
	                                                    vector<LogicalType> &return_types, vector<string> &names) {
		auto &config = DBConfig::GetConfig(context);
		if (!config.options.enable_external_access) {
			throw PermissionException("Scanning Parquet files is disabled through configuration");
		}
		if (input.inputs[0].IsNull()) {
			throw ParserException("Parquet reader cannot take NULL list as parameter");
		}
		FileSystem &fs = FileSystem::GetFileSystem(context);
		vector<string> files;
		for (auto &val : ListValue::GetChildren(input.inputs[0])) {
			if (val.IsNull()) {
				throw ParserException("Parquet reader cannot take NULL input as parameter");
			}
			auto glob_files = ParquetGlob(fs, StringValue::Get(val), context);
			files.insert(files.end(), glob_files.begin(), glob_files.end());
		}
		if (files.empty()) {
			throw IOException("Parquet reader needs at least one file to read");
		}
		ParquetOptions parquet_options(context);
		for (auto &kv : input.named_parameters) {
			auto loption = StringUtil::Lower(kv.first);
			if (loption == "binary_as_string") {
				parquet_options.binary_as_string = BooleanValue::Get(kv.second);
			} else if (loption == "filename") {
				parquet_options.filename = BooleanValue::Get(kv.second);
			} else if (loption == "file_row_number") {
				parquet_options.file_row_number = BooleanValue::Get(kv.second);
			} else if (loption == "hive_partitioning") {
				parquet_options.hive_partitioning = BooleanValue::Get(kv.second);
			} else if (loption == "union_by_name") {
				parquet_options.union_by_name = true;
			}
		}
		return ParquetScanBindInternal(context, std::move(files), return_types, names, parquet_options);
	}

	static double ParquetProgress(ClientContext &context, const FunctionData *bind_data_p,
	                              const GlobalTableFunctionState *global_state) {
		auto &bind_data = bind_data_p->Cast<ParquetReadBindData>();
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
		auto &bind_data = input.bind_data->Cast<ParquetReadBindData>();
		auto &gstate = gstate_p->Cast<ParquetReadGlobalState>();

		auto result = make_unique<ParquetReadLocalState>();
		result->column_ids = input.column_ids;
		result->is_parallel = true;
		result->batch_index = 0;
		result->table_filters = input.filters;
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
		auto &bind_data = input.bind_data->Cast<ParquetReadBindData>();

		auto result = make_unique<ParquetReadGlobalState>();

		result->file_opening = std::vector<bool>(bind_data.files.size(), false);
		result->file_mutexes = std::unique_ptr<mutex[]>(new mutex[bind_data.files.size()]);
		if (!bind_data.parquet_options.union_by_name) {
			result->readers = std::vector<shared_ptr<ParquetReader>>(bind_data.files.size(), nullptr);
			if (bind_data.initial_reader) {
				result->initial_reader = bind_data.initial_reader;
				result->readers[0] = bind_data.initial_reader;
			} else {
				if (bind_data.files.empty()) {
					result->initial_reader = nullptr;
				} else {
					result->initial_reader =
					    make_shared<ParquetReader>(context, bind_data.files[0], bind_data.names, bind_data.types,
					                               input.column_ids, bind_data.parquet_options, bind_data.files[0]);
					result->readers[0] = result->initial_reader;
				}
			}
		} else {
			result->readers = std::move(bind_data.union_readers);
			result->initial_reader = result->readers[0];
		}

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
		auto &data = local_state->Cast<ParquetReadLocalState>();
		return data.batch_index;
	}

	static void ParquetScanSerialize(FieldWriter &writer, const FunctionData *bind_data_p,
	                                 const TableFunction &function) {
		auto &bind_data = bind_data_p->Cast<ParquetReadBindData>();
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
		auto &data = data_p.local_state->Cast<ParquetReadLocalState>();
		auto &gstate = data_p.global_state->Cast<ParquetReadGlobalState>();
		auto &bind_data = data_p.bind_data->CastNoConst<ParquetReadBindData>();

		do {
			if (gstate.CanRemoveFilterColumns()) {
				data.all_columns.Reset();
				data.reader->Scan(data.scan_state, data.all_columns);
				output.ReferenceColumns(data.all_columns, gstate.projection_ids);
			} else {
				data.reader->Scan(data.scan_state, output);
			}

			bind_data.chunk_count++;
			if (output.size() > 0) {
				if (bind_data.parquet_options.union_by_name) {
					UnionByName<ParquetReader, ParquetOptions>::SetNullUnionCols(output, data.reader->union_null_cols);
				}
				return;
			}
			if (!ParquetParallelStateNext(context, bind_data, data, gstate)) {
				return;
			}
		} while (true);
	}

	static unique_ptr<NodeStatistics> ParquetCardinality(ClientContext &context, const FunctionData *bind_data) {
		auto &data = bind_data->Cast<ParquetReadBindData>();
		return make_unique<NodeStatistics>(data.initial_file_cardinality * data.files.size());
	}

	static idx_t ParquetScanMaxThreads(ClientContext &context, const FunctionData *bind_data) {
		auto &data = bind_data->Cast<ParquetReadBindData>();
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
				const auto &current_reader = parallel_state.readers[parallel_state.file_index];
				if (current_reader->union_null_cols.empty()) {
					current_reader->union_null_cols.resize(current_reader->return_types.size());
					std::fill(current_reader->union_null_cols.begin(), current_reader->union_null_cols.end(), false);
				}

				if (parallel_state.row_group_index <
				    parallel_state.readers[parallel_state.file_index]->NumRowGroups()) {
					// The current reader has rowgroups left to be scanned
					scan_data.reader = parallel_state.readers[parallel_state.file_index];
					vector<idx_t> group_indexes {parallel_state.row_group_index};
					scan_data.reader->InitializeScan(scan_data.scan_state, scan_data.column_ids, group_indexes,
					                                 scan_data.table_filters);
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
		if (!data->files.empty()) {
			auto initial_filename = data->files[0];

			if (data->parquet_options.hive_partitioning || data->parquet_options.filename) {
				unordered_map<string, column_t> column_map;
				for (idx_t i = 0; i < get.column_ids.size(); i++) {
					column_map.insert({get.names[get.column_ids[i]], i});
				}

				HivePartitioning::ApplyFiltersToFileList(context, data->files, filters, column_map, get.table_index,
				                                         data->parquet_options.hive_partitioning,
				                                         data->parquet_options.filename);

				if (data->files.empty() || initial_filename != data->files[0]) {
					// Remove initial reader in case the first file gets filtered out
					data->initial_reader.reset();
				}
			}
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
					reader = make_shared<ParquetReader>(context, file, bind_data.names, bind_data.types,
					                                    scan_data.column_ids, pq_options, bind_data.files[0]);
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
	auto &parquet_bind = bind_data.Cast<ParquetWriteBindData>();

	auto &fs = FileSystem::GetFileSystem(context);
	global_state->writer =
	    make_unique<ParquetWriter>(fs, file_path, FileSystem::GetFileOpener(context), parquet_bind.sql_types,
	                               parquet_bind.column_names, parquet_bind.codec);
	return std::move(global_state);
}

void ParquetWriteSink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                      LocalFunctionData &lstate, DataChunk &input) {
	auto &bind_data = bind_data_p.Cast<ParquetWriteBindData>();
	auto &global_state = gstate.Cast<ParquetWriteGlobalState>();
	auto &local_state = lstate.Cast<ParquetWriteLocalState>();

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
	auto &global_state = gstate.Cast<ParquetWriteGlobalState>();
	auto &local_state = lstate.Cast<ParquetWriteLocalState>();
	// flush any data left in the local state to the file
	global_state.writer->Flush(local_state.buffer);
}

void ParquetWriteFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &global_state = gstate.Cast<ParquetWriteGlobalState>();
	// finalize: write any additional metadata to the file here
	global_state.writer->Finalize();
}

unique_ptr<LocalFunctionData> ParquetWriteInitializeLocal(ExecutionContext &context, FunctionData &bind_data_p) {
	auto &bind_data = bind_data_p.Cast<ParquetWriteBindData>();
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
	CreateTableFunctionInfo meta_cinfo(meta_fun);

	ParquetSchemaFunction schema_fun;
	CreateTableFunctionInfo schema_cinfo(schema_fun);

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
