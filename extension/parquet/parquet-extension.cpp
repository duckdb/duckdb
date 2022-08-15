#define DUCKDB_EXTENSION_MAIN

#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <numeric>

#include "parquet-extension.hpp"
#include "parquet_reader.hpp"
#include "parquet_writer.hpp"
#include "parquet_metadata.hpp"
#include "zstd_file_system.hpp"

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "duckdb/storage/statistics/base_statistics.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"

#include "duckdb/planner/operator/logical_get.hpp"
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
};

struct ParquetReadGlobalState : public GlobalTableFunctionState {
	mutex lock;
	shared_ptr<ParquetReader> current_reader;
	idx_t batch_index;
	idx_t file_index;
	idx_t row_group_index;
	idx_t max_threads;

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

struct ParquetWriteBindData : public TableFunctionData {
	vector<LogicalType> sql_types;
	string file_name;
	vector<string> column_names;
	duckdb_parquet::format::CompressionCodec::type codec = duckdb_parquet::format::CompressionCodec::SNAPPY;
	idx_t row_group_size = 100000;
};

struct ParquetWriteGlobalState : public GlobalFunctionData {
	unique_ptr<ParquetWriter> writer;
};

struct ParquetWriteLocalState : public LocalFunctionData {
	explicit ParquetWriteLocalState(ClientContext &context, const vector<LogicalType> &types) : buffer(context, types) {
	}

	ColumnDataCollection buffer;
};

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
		table_function.named_parameters["hive_partitioning"] = LogicalType::BOOLEAN;
		table_function.get_batch_index = ParquetScanGetBatchIndex;
		table_function.projection_pushdown = true;
		table_function.filter_pushdown = true;
		table_function.pushdown_complex_filter = ParquetComplexFilterPushdown;
		set.AddFunction(table_function);
		table_function.arguments = {LogicalType::LIST(LogicalType::VARCHAR)};
		table_function.bind = ParquetScanBindList;
		table_function.named_parameters["binary_as_string"] = LogicalType::BOOLEAN;
		table_function.named_parameters["filename"] = LogicalType::BOOLEAN;
		table_function.named_parameters["hive_partitioning"] = LogicalType::BOOLEAN;
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
			} else if (loption == "hive_partitioning") {
				parquet_options.hive_partitioning = true;
			} else {
				throw NotImplementedException("Unsupported option for COPY FROM parquet: %s", option.first);
			}
		}
		auto result = make_unique<ParquetReadBindData>();

		FileSystem &fs = FileSystem::GetFileSystem(context);
		result->files = fs.Glob(info.file_path, context);
		if (result->files.empty()) {
			throw IOException("No files found that match the pattern \"%s\"", info.file_path);
		}
		result->SetInitialReader(
		    make_shared<ParquetReader>(context, result->files[0], expected_types, parquet_options));
		result->names = result->initial_reader->names;
		result->types = result->initial_reader->return_types;
		return move(result);
	}

	static unique_ptr<BaseStatistics> ParquetScanStats(ClientContext &context, const FunctionData *bind_data_p,
	                                                   column_t column_index) {
		auto &bind_data = (ParquetReadBindData &)*bind_data_p;

		if (column_index == COLUMN_IDENTIFIER_ROW_ID) {
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
		result->files = move(files);

		result->SetInitialReader(make_shared<ParquetReader>(context, result->files[0], parquet_options));
		return_types = result->types = result->initial_reader->return_types;
		names = result->names = result->initial_reader->names;
		return move(result);
	}

	static vector<string> ParquetGlob(FileSystem &fs, const string &glob, ClientContext &context) {
		auto files = fs.Glob(glob, FileSystem::GetFileOpener(context));
		if (files.empty()) {
			throw IOException("No files found that match the pattern \"%s\"", glob);
		}
		return files;
	}

	static unique_ptr<FunctionData> ParquetScanBind(ClientContext &context, TableFunctionBindInput &input,
	                                                vector<LogicalType> &return_types, vector<string> &names) {
		auto &config = DBConfig::GetConfig(context);
		if (!config.options.enable_external_access) {
			throw PermissionException("Scanning Parquet files is disabled through configuration");
		}
		auto file_name = input.inputs[0].GetValue<string>();
		ParquetOptions parquet_options(context);
		for (auto &kv : input.named_parameters) {
			auto loption = StringUtil::Lower(kv.first);
			if (loption == "binary_as_string") {
				parquet_options.binary_as_string = BooleanValue::Get(kv.second);
			} else if (loption == "filename") {
				parquet_options.filename = BooleanValue::Get(kv.second);
			} else if (loption == "hive_partitioning") {
				parquet_options.hive_partitioning = BooleanValue::Get(kv.second);
			}
		}
		FileSystem &fs = FileSystem::GetFileSystem(context);
		auto files = ParquetGlob(fs, file_name, context);
		return ParquetScanBindInternal(context, move(files), return_types, names, parquet_options);
	}

	static unique_ptr<FunctionData> ParquetScanBindList(ClientContext &context, TableFunctionBindInput &input,
	                                                    vector<LogicalType> &return_types, vector<string> &names) {
		auto &config = DBConfig::GetConfig(context);
		if (!config.options.enable_external_access) {
			throw PermissionException("Scanning Parquet files is disabled through configuration");
		}
		FileSystem &fs = FileSystem::GetFileSystem(context);
		vector<string> files;
		for (auto &val : ListValue::GetChildren(input.inputs[0])) {
			auto glob_files = ParquetGlob(fs, val.ToString(), context);
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
			} else if (loption == "hive_partitioning") {
				parquet_options.hive_partitioning = BooleanValue::Get(kv.second);
			}
		}
		return ParquetScanBindInternal(context, move(files), return_types, names, parquet_options);
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
		result->column_ids = input.column_ids;
		result->is_parallel = true;
		result->batch_index = 0;
		result->table_filters = input.filters;
		if (!ParquetParallelStateNext(context.client, bind_data, *result, gstate)) {
			return nullptr;
		}
		return move(result);
	}

	static unique_ptr<GlobalTableFunctionState> ParquetScanInitGlobal(ClientContext &context,
	                                                                  TableFunctionInitInput &input) {
		auto &bind_data = (ParquetReadBindData &)*input.bind_data;

		auto result = make_unique<ParquetReadGlobalState>();

		if (bind_data.initial_reader) {
			result->current_reader = bind_data.initial_reader;
		} else {
			if (bind_data.files.empty()) {
				result->current_reader = nullptr;
			} else {
				result->current_reader =
				    make_shared<ParquetReader>(context, bind_data.files[0], bind_data.names, bind_data.types,
				                               input.column_ids, bind_data.parquet_options, bind_data.files[0]);
			}
		}

		result->row_group_index = 0;
		result->file_index = 0;
		result->batch_index = 0;
		result->max_threads = ParquetScanMaxThreads(context, input.bind_data);
		return move(result);
	}

	static idx_t ParquetScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
	                                      LocalTableFunctionState *local_state,
	                                      GlobalTableFunctionState *global_state) {
		auto &data = (ParquetReadLocalState &)*local_state;
		return data.batch_index;
	}

	static void ParquetScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		if (!data_p.local_state) {
			return;
		}
		auto &data = (ParquetReadLocalState &)*data_p.local_state;
		auto &gstate = (ParquetReadGlobalState &)*data_p.global_state;
		auto &bind_data = (ParquetReadBindData &)*data_p.bind_data;

		do {
			data.reader->Scan(data.scan_state, output);
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

	static bool ParquetParallelStateNext(ClientContext &context, const ParquetReadBindData &bind_data,
	                                     ParquetReadLocalState &scan_data, ParquetReadGlobalState &parallel_state) {
		lock_guard<mutex> parallel_lock(parallel_state.lock);

		if (parallel_state.current_reader == nullptr) {
			return false;
		}

		if (parallel_state.row_group_index < parallel_state.current_reader->NumRowGroups()) {
			// groups remain in the current parquet file: read the next group
			scan_data.reader = parallel_state.current_reader;
			vector<idx_t> group_indexes {parallel_state.row_group_index};
			scan_data.reader->InitializeScan(scan_data.scan_state, scan_data.column_ids, group_indexes,
			                                 scan_data.table_filters);
			scan_data.batch_index = parallel_state.batch_index++;
			scan_data.file_index = parallel_state.file_index;
			parallel_state.row_group_index++;
			return true;
		} else {
			// no groups remain in the current parquet file: check if there are more files to read
			while (parallel_state.file_index + 1 < bind_data.files.size()) {
				// read the next file
				string file = bind_data.files[++parallel_state.file_index];

				parallel_state.current_reader =
				    make_shared<ParquetReader>(context, file, bind_data.names, bind_data.types, scan_data.column_ids,
				                               parallel_state.current_reader->parquet_options, bind_data.files[0]);
				if (parallel_state.current_reader->NumRowGroups() == 0) {
					// empty parquet file, move to next file
					continue;
				}
				// set up the scan state to read the first group
				scan_data.reader = parallel_state.current_reader;
				vector<idx_t> group_indexes {0};
				scan_data.reader->InitializeScan(scan_data.scan_state, scan_data.column_ids, group_indexes,
				                                 scan_data.table_filters);
				scan_data.batch_index = parallel_state.batch_index++;
				scan_data.file_index = parallel_state.file_index;
				parallel_state.row_group_index = 1;
				return true;
			}
		}
		return false;
	}

	static void ParquetComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
	                                         vector<unique_ptr<Expression>> &filters) {
		auto data = (ParquetReadBindData *)bind_data_p;
		auto initial_filename = data->files[0];

		if (data->parquet_options.hive_partitioning || data->parquet_options.filename) {
			unordered_map<string, column_t> column_map;
			for (idx_t i = 0; i < get.column_ids.size(); i++) {
				column_map.insert({get.names[get.column_ids[i]], i});
			}

			HivePartitioning::ApplyFiltersToFileList(data->files, filters, column_map, get.table_index,
			                                         data->parquet_options.hive_partitioning,
			                                         data->parquet_options.filename);

			if (data->files.empty() || initial_filename != data->files[0]) {
				// Remove initial reader in case the first file gets filtered out
				data->initial_reader.reset();
			}
		}
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
	bind_data->file_name = info.file_path;
	return move(bind_data);
}

unique_ptr<GlobalFunctionData> ParquetWriteInitializeGlobal(ClientContext &context, FunctionData &bind_data,
                                                            const string &file_path) {
	auto global_state = make_unique<ParquetWriteGlobalState>();
	auto &parquet_bind = (ParquetWriteBindData &)bind_data;

	auto &fs = FileSystem::GetFileSystem(context);
	global_state->writer =
	    make_unique<ParquetWriter>(fs, file_path, FileSystem::GetFileOpener(context), parquet_bind.sql_types,
	                               parquet_bind.column_names, parquet_bind.codec);
	return move(global_state);
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

unique_ptr<TableFunctionRef> ParquetScanReplacement(ClientContext &context, const string &table_name,
                                                    ReplacementScanData *data) {
	if (!StringUtil::EndsWith(StringUtil::Lower(table_name), ".parquet")) {
		return nullptr;
	}
	auto table_function = make_unique<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_unique<ConstantExpression>(Value(table_name)));
	table_function->function = make_unique<FunctionExpression>("parquet_scan", move(children));
	return table_function;
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
	function.copy_from_bind = ParquetScanFunction::ParquetReadBind;
	function.copy_from_function = scan_fun.functions[0];

	function.extension = "parquet";
	CreateCopyFunctionInfo info(function);

	Connection con(db);
	con.BeginTransaction();
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context);
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

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
